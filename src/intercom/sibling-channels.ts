/**
 * File-based peer request lifecycle for sibling messaging.
 *
 * Scope is exactly one workflow run: `{ session, workflow }` maps to
 * `sibling-channels/s-<session>/w-<workflow>/`. Siblings in different
 * workflows (even under one parent session) never share a mailbox.
 * The roster itself travels in task text (see sibling-roster.ts).
 *
 * One lifecycle for everything addressed to a child, symmetric with the
 * supervisor's own inbox:
 * - `sendPeerRequest` writes `requests/<id>.json` (expectsReply true/false).
 * - `recordSupervisorMessage` files a supervisor steer or reply into the same
 *   mailbox, pre-settled, so the child has a durable copy of what the
 *   supervisor said even when the push was missed.
 * - `listInbox` returns every entry for a recipient, oldest-first with an
 *   explicit cursor, each tagged with its kind and state. Reads never consume.
 * - `writePeerReply` writes `replies/<requestId>.json`.
 * - `waitForPeerReply` blocks (polling) until the reply lands or the deadline
 *   passes. "Must respond" is enforced by timeout-plus-escalation: on expiry
 *   the waiter is told to escalate, and the caller files a supervisor note.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { TEMP_ROOT_DIR } from "../shared/types.ts";
import { writePrivateAtomicJson } from "../shared/atomic-json.ts";
import { isValidSiblingKey } from "./sibling-roster.ts";

const SIBLING_ROOT = path.join(TEMP_ROOT_DIR, "sibling-channels");

/** Mailbox root. Same value everywhere: test temp-root isolation applies at import time. */
export function siblingRootDir(): string {
	return SIBLING_ROOT;
}
const REQUESTS_DIR = "requests";
const REPLIES_DIR = "replies";
export const MAX_SIBLING_MESSAGE_BYTES = 16 * 1024;
const MAX_SIBLING_REQUESTS_PER_SCOPE = 200;
/** Resolved requests/replies older than this are eligible for reaping. */
export const SIBLING_MESSAGE_TTL_MS = 24 * 60 * 60 * 1000;
/** Empty scopes older than this are removed. */
export const SIBLING_EMPTY_SCOPE_TTL_MS = 60 * 60 * 1000;
/** Upper bound for a blocking peer wait. */
export const MAX_PEER_WAIT_MS = 10 * 60 * 1000;

export interface SiblingScope {
	/** Parent supervisor session id (or run id fallback). Namespaces concurrent parents. */
	session: string;
	/** Workflow run id. Isolates concurrent workflows in one session. */
	workflow: string;
}

export interface PeerRequest {
	version: 1;
	id: string;
	session: string;
	workflow: string;
	from: string;
	to: string;
	message: string;
	expectsReply: boolean;
	createdAt: number;
}

export interface PeerReply {
	version: 1;
	requestId: string;
	session: string;
	workflow: string;
	from: string;
	/** The original asker (the request's `from`). */
	to: string;
	message: string;
	createdAt: number;
}

export interface SiblingCursor {
	after: number;
	afterId?: string;
}

export type InboxKind = "ask" | "note" | "supervisor";
export type InboxState = "pending" | "answered" | "delivered";

/** One mailbox entry addressed to a child, with its lifecycle resolved. */
export interface InboxEntry {
	id: string;
	from: string;
	to: string;
	kind: InboxKind;
	state: InboxState;
	message: string;
	expectsReply: boolean;
	createdAt: number;
	/** Who answered an `ask` (a sibling key, or `system` for relay verdicts). */
	replyFrom?: string;
}

const STRICT_SEGMENT = /^[A-Za-z0-9._-]{1,128}$/;
const SCOPE_SEGMENT_MAX = 128;

/**
 * Scope-id sanitizer for HOST-GENERATED ids (supervisor session ids, workflow
 * run ids). Unlike child-supplied keys (strictly validated), these can be
 * tool-call-derived (`call_...%7Cfc_...`), so they are mapped into a safe
 * segment instead of rejected. Uniqueness is preserved by hashing whenever
 * truncation or heavy mapping applies.
 */
export function sanitizeScopeSegment(value: string): string {
	const trimmed = value.trim();
	if (!trimmed) throw new Error("Sibling scope id must not be empty.");
	let mapped = trimmed.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
	if (!mapped || mapped === "." || mapped === "..") mapped = "scope";
	if (mapped.length <= SCOPE_SEGMENT_MAX && mapped === trimmed) return mapped;
	// Truncation or remapping applied: suffix a hash of the full value so
	// distinct ids cannot collide (concurrent workflows stay isolated).
	let digest = 0;
	for (let i = 0; i < trimmed.length; i++) digest = (digest * 31 + trimmed.charCodeAt(i)) >>> 0;
	const suffix = digest.toString(36);
	const prefix = mapped.slice(0, Math.max(1, SCOPE_SEGMENT_MAX - suffix.length - 1)).replace(/-+$/g, "") || "scope";
	return `${prefix}-${suffix}`;
}

function scopeDir(scope: SiblingScope): string {
	const session = sanitizeScopeSegment(scope.session);
	const workflow = sanitizeScopeSegment(scope.workflow);
	const dir = path.join(SIBLING_ROOT, `s-${session}`, `w-${workflow}`);
	assertContained(dir);
	return dir;
}

function assertContained(resolved: string): void {
	const relative = path.relative(SIBLING_ROOT, resolved);
	if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
		throw new Error("Sibling mailbox path escapes its root.");
	}
}

function requestsDir(scope: SiblingScope): string {
	const dir = path.join(scopeDir(scope), REQUESTS_DIR);
	assertContained(dir);
	return dir;
}

function repliesDir(scope: SiblingScope): string {
	const dir = path.join(scopeDir(scope), REPLIES_DIR);
	assertContained(dir);
	return dir;
}

export function ensureSiblingScopeDir(scope: SiblingScope): string {
	const dir = scopeDir(scope);
	fs.mkdirSync(path.join(dir, REQUESTS_DIR), { recursive: true, mode: 0o700 });
	fs.mkdirSync(path.join(dir, REPLIES_DIR), { recursive: true, mode: 0o700 });
	return dir;
}

function messageIdPatternOk(id: string): boolean {
	return /^[A-Za-z0-9._-]{1,128}$/.test(id) && id !== "." && id !== "..";
}

function requestPath(scope: SiblingScope, id: string): string {
	if (!messageIdPatternOk(id)) throw new Error("Sibling request id is invalid.");
	const file = path.join(requestsDir(scope), `${id}.json`);
	assertContained(file);
	return file;
}

function replyPath(scope: SiblingScope, requestId: string): string {
	if (!messageIdPatternOk(requestId)) throw new Error("Sibling request id is invalid.");
	const file = path.join(repliesDir(scope), `${requestId}.json`);
	assertContained(file);
	return file;
}

/** The parent session's sender name in a child's inbox. */
export const SUPERVISOR_SENDER = "supervisor";
/** Names no sibling may take: the relay's own sender and the supervisor address. */
export const SIBLING_RESERVED_WORDS: ReadonlySet<string> = new Set(["system", SUPERVISOR_SENDER]);

/**
 * Validate a child-supplied sibling key (pattern plus reserved words). `role`
 * names the parameter in the error so callers share one check and one message.
 */
export function checkSiblingKey(value: string, role: string): string {
	const trimmed = value.trim();
	if (!isValidSiblingKey(trimmed)) {
		throw new Error(`'${role}' must be a sibling workflow key (1-128 chars: letters, numbers, '.', '_', '-').`);
	}
	if (SIBLING_RESERVED_WORDS.has(trimmed.toLowerCase())) {
		throw new Error(`'${trimmed}' is reserved and cannot be a sibling key.`);
	}
	return trimmed;
}

function checkMessageBody(message: string): string {
	const text = message.trim();
	if (!text) throw new Error("message is required for sibling messages.");
	if (Buffer.byteLength(text, "utf-8") > MAX_SIBLING_MESSAGE_BYTES) {
		throw new Error("Sibling message is too large (max 16 KiB).");
	}
	return text;
}

function readOwnRequest(scope: SiblingScope, id: string, from: string): PeerRequest | undefined {
	try {
		const parsed = JSON.parse(fs.readFileSync(requestPath(scope, id), "utf-8")) as Partial<PeerRequest>;
		return parsed.version === 1 && parsed.id === id && parsed.from === from ? (parsed as PeerRequest) : undefined;
	} catch {
		return undefined;
	}
}

/** Sender-scoped request id for an idempotency key; hashed when the pair would exceed the id length. */
function idempotentRequestId(from: string, key: string): string {
	const joined = `${from}.${key}`;
	if (joined.length <= 128) return joined;
	let digest = 0;
	for (let i = 0; i < joined.length; i++) digest = (digest * 31 + joined.charCodeAt(i)) >>> 0;
	const suffix = digest.toString(36);
	return `${joined.slice(0, 128 - suffix.length - 1)}.${suffix}`;
}

export function sendPeerRequest(input: {
	scope: SiblingScope;
	from: string;
	to: string;
	message: string;
	expectsReply: boolean;
	/** Optional client idempotency key: retries with the same key return the original request. */
	idempotencyKey?: string;
}): PeerRequest {
	const from = checkSiblingKey(input.from, "from");
	const to = checkSiblingKey(input.to, "to");
	if (from === to) throw new Error("Cannot send a sibling message to yourself.");
	const message = checkMessageBody(input.message);
	let id: string;
	if (input.idempotencyKey !== undefined) {
		const key = input.idempotencyKey.trim();
		if (!messageIdPatternOk(key)) throw new Error("Sibling idempotency key is invalid (1-128 chars: letters, numbers, '.', '_', '-').");
		// A sender may resume its own earlier ask by passing that ask's id (the
		// mutual-ask yield path); otherwise the key is namespaced by sender so
		// two siblings picking the same key never collide on one file.
		const own = readOwnRequest(input.scope, key, from);
		id = own ? key : idempotentRequestId(from, key);
		try {
			const prior = JSON.parse(fs.readFileSync(requestPath(input.scope, id), "utf-8")) as Partial<PeerRequest>;
			if (prior.version === 1 && prior.id === id) {
				if (prior.from === from && prior.to === to && prior.message === message && prior.expectsReply === input.expectsReply) {
					return prior as PeerRequest;
				}
				throw new Error(`Sibling idempotency key '${key}' was already used for a different ask; retries must resend the identical payload.`);
			}
		} catch (error) {
			if (error instanceof Error && /already used for a different ask/.test(error.message)) throw error;
			// No usable prior write; fall through and write.
		}
	} else {
		id = randomUUID();
	}
	ensureSiblingScopeDir(input.scope);
	assertMailboxCapacity(input.scope, id);
	const payload: PeerRequest = {
		version: 1,
		id,
		session: input.scope.session,
		workflow: input.scope.workflow,
		from,
		to,
		message,
		expectsReply: input.expectsReply,
		createdAt: Date.now(),
	};
	writePrivateAtomicJson(requestPath(input.scope, id), payload);
	return payload;
}

function assertMailboxCapacity(scope: SiblingScope, id: string): void {
	try {
		const inbox = requestsDir(scope);
		// Unparseable debris never counts toward the cap (GC reaps it by mtime).
		const existing = fs.readdirSync(inbox).filter((f) => f.endsWith(".json") && f !== `${id}.json`);
		let live = 0;
		for (const file of existing) {
			try {
				const parsed = JSON.parse(fs.readFileSync(path.join(inbox, file), "utf-8")) as Partial<PeerRequest>;
				if (parsed.version === 1 && typeof parsed.id === "string") live++;
			} catch {
				continue;
			}
		}
		if (live >= MAX_SIBLING_REQUESTS_PER_SCOPE) {
			throw new Error("Sibling mailbox is full for this workflow (200 requests). Summarize in your final output instead.");
		}
	} catch (error) {
		if (error instanceof Error && /mailbox is full/.test(error.message)) throw error;
		// Missing dir was just created; continue.
	}
}

/**
 * File a supervisor steer or reply into the recipient's inbox. The push (steer
 * or tool result) is the wake-up; this is the durable copy. Entries are
 * settled on write with a `system` receipt so they never look like pending
 * asks and are reaped with the rest of the mailbox.
 */
export function recordSupervisorMessage(input: {
	scope: SiblingScope;
	to: string;
	message: string;
	kind: "steer" | "reply";
	/** Delivery outcome of the push, for the receipt line. */
	delivery?: string;
}): PeerRequest {
	const to = checkSiblingKey(input.to, "to");
	const text = input.message.trim();
	if (!text) throw new Error("message is required for supervisor inbox entries.");
	const bounded = Buffer.byteLength(text, "utf-8") > MAX_SIBLING_MESSAGE_BYTES
		? `${Buffer.from(text, "utf-8").subarray(0, MAX_SIBLING_MESSAGE_BYTES - 32).toString("utf-8")}\n[truncated to 16 KiB]`
		: text;
	const id = `sup-${randomUUID()}`;
	ensureSiblingScopeDir(input.scope);
	assertMailboxCapacity(input.scope, id);
	const payload: PeerRequest = {
		version: 1,
		id,
		session: input.scope.session,
		workflow: input.scope.workflow,
		from: SUPERVISOR_SENDER,
		to,
		message: bounded,
		expectsReply: false,
		createdAt: Date.now(),
	};
	writePrivateAtomicJson(requestPath(input.scope, id), payload);
	try {
		writePeerReply({
			scope: input.scope,
			requestId: id,
			from: "system",
			systemReply: true,
			message: `Supervisor ${input.kind} recorded${input.delivery ? ` (${input.delivery})` : ""}.`,
		});
	} catch {
		// The entry is still readable; an unsettled receipt only lists it as pending.
	}
	return payload;
}

function readReplyFile(scope: SiblingScope, requestId: string): PeerReply | undefined {
	try {
		const parsed = JSON.parse(fs.readFileSync(replyPath(scope, requestId), "utf-8")) as Partial<PeerReply>;
		return parsed.version === 1 && parsed.requestId === requestId && typeof parsed.from === "string" ? (parsed as PeerReply) : undefined;
	} catch {
		return undefined;
	}
}

function inboxKind(request: PeerRequest): InboxKind {
	if (request.from === SUPERVISOR_SENDER) return "supervisor";
	return request.expectsReply ? "ask" : "note";
}

/**
 * Everything addressed to `selfKey`, oldest-first after an optional cursor.
 * `pendingOnly` narrows to unanswered peer asks and undelivered notes (the
 * items that still need action); the default view is the full history.
 */
export function listInbox(
	scope: SiblingScope,
	selfKey: string,
	options: { after?: number; afterId?: string; limit?: number; pendingOnly?: boolean } = {},
): InboxEntry[] {
	const to = checkSiblingKey(selfKey, "to");
	const limit = Math.min(Math.max(options.limit ?? 20, 1), 50);
	const after = options.after ?? 0;
	const afterId = options.afterId ?? "";
	let files: string[];
	try {
		files = fs.readdirSync(requestsDir(scope)).filter((f) => f.endsWith(".json"));
	} catch {
		return [];
	}
	const out: InboxEntry[] = [];
	for (const file of files) {
		const parsed = parseRequestFile(scope, path.join(requestsDir(scope), file));
		if (!parsed || parsed.to !== to) continue;
		if (parsed.createdAt < after) continue;
		if (parsed.createdAt === after && parsed.id <= afterId) continue;
		const kind = inboxKind(parsed);
		const reply = readReplyFile(scope, parsed.id);
		const state: InboxState = !reply ? "pending" : kind === "ask" ? "answered" : "delivered";
		if (options.pendingOnly && state !== "pending") continue;
		out.push({
			id: parsed.id,
			from: parsed.from,
			to: parsed.to,
			kind,
			state,
			message: parsed.message,
			expectsReply: parsed.expectsReply,
			createdAt: parsed.createdAt,
			...(reply && kind === "ask" ? { replyFrom: reply.from } : {}),
		});
	}
	out.sort((a, b) => a.createdAt - b.createdAt || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
	return out.slice(0, limit);
}

function parseRequestFile(scope: SiblingScope, file: string): PeerRequest | undefined {
	try {
		const parsed = JSON.parse(fs.readFileSync(file, "utf-8")) as Partial<PeerRequest>;
		if (parsed.version !== 1 || typeof parsed.id !== "string" || typeof parsed.from !== "string" ||
			typeof parsed.to !== "string" || typeof parsed.message !== "string" ||
			typeof parsed.createdAt !== "number" || typeof parsed.expectsReply !== "boolean" ||
			parsed.session !== scope.session || parsed.workflow !== scope.workflow) return undefined;
		return parsed as PeerRequest;
	} catch {
		return undefined;
	}
}

function hasReply(scope: SiblingScope, requestId: string): boolean {
	try {
		const parsed = JSON.parse(fs.readFileSync(replyPath(scope, requestId), "utf-8")) as Partial<PeerReply>;
		return parsed.version === 1 && parsed.requestId === requestId;
	} catch {
		return false;
	}
}

/** Pending peer traffic for `selfKey` (no reply yet), oldest-first, after an optional cursor. */
export function listPeerRequestsFor(
	scope: SiblingScope,
	selfKey: string,
	options: { after?: number; afterId?: string; limit?: number } = {},
): PeerRequest[] {
	return listInbox(scope, selfKey, { ...options, pendingOnly: true })
		.filter((entry) => entry.kind !== "supervisor")
		.map((entry) => ({
			version: 1 as const,
			id: entry.id,
			session: scope.session,
			workflow: scope.workflow,
			from: entry.from,
			to: entry.to,
			message: entry.message,
			expectsReply: entry.expectsReply,
			createdAt: entry.createdAt,
		}));
}

export function writePeerReply(input: {
	scope: SiblingScope;
	requestId: string;
	from: string;
	message: string;
	/** Relay-only: the honest `system` sender bypasses key and recipient checks. */
	systemReply?: boolean;
}): PeerReply {
	const systemSender = input.systemReply === true && input.from === "system";
	const from = systemSender ? "system" : checkSiblingKey(input.from, "from");
	const message = checkMessageBody(input.message);
	ensureSiblingScopeDir(input.scope);
	const requestFile = requestPath(input.scope, input.requestId.trim());
	let request: PeerRequest | undefined;
	try {
		request = parseRequestFile(input.scope, requestFile);
	} catch {
		request = undefined;
	}
	if (!request) throw new Error(`No pending sibling request '${input.requestId}'.`);
	if (!systemSender && from !== request.to) {
		throw new Error(`Only '${request.to}' may reply to this ask.`);
	}
	if (hasReply(input.scope, request.id)) throw new Error(`Sibling request '${input.requestId}' already has a reply.`);
	const reply: PeerReply = {
		version: 1,
		requestId: request.id,
		session: input.scope.session,
		workflow: input.scope.workflow,
		from,
		to: request.from,
		message,
		createdAt: Date.now(),
	};
	// Exclusive create: a lost race surfaces as "already has a reply", never a clobber.
	try {
		fs.writeFileSync(replyPath(input.scope, request.id), JSON.stringify(reply, null, 2), { encoding: "utf-8", mode: 0o600, flag: "wx" });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "EEXIST" && hasReply(input.scope, request.id)) {
			throw new Error(`Sibling request '${input.requestId}' already has a reply.`);
		}
		throw error;
	}
	return reply;
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal?.aborted) {
			reject(new Error("Sibling reply wait cancelled."));
			return;
		}
		let timer: ReturnType<typeof setTimeout> | undefined;
		const cleanup = () => {
			if (timer) clearTimeout(timer);
			signal?.removeEventListener("abort", onAbort);
		};
		const onAbort = () => {
			cleanup();
			reject(new Error("Sibling reply wait cancelled."));
		};
		timer = setTimeout(() => {
			cleanup();
			resolve();
		}, ms);
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

/** Block until the peer reply lands or the deadline passes. */
export async function waitForPeerReply(
	scope: SiblingScope,
	requestId: string,
	deadlineMs: number,
	signal?: AbortSignal,
	/** Checked each poll; a returned string aborts the wait with that message. */
	abortIf?: () => string | undefined,
): Promise<PeerReply> {
	const deadline = Date.now() + deadlineMs;
	for (;;) {
		if (signal?.aborted) throw new Error("Sibling reply wait cancelled.");
		const abortReason = abortIf?.();
		if (abortReason) throw new Error(abortReason);
		try {
			const parsed = JSON.parse(fs.readFileSync(replyPath(scope, requestId), "utf-8")) as Partial<PeerReply>;
			if (parsed.version === 1 && parsed.requestId === requestId && typeof parsed.message === "string") {
				return parsed as PeerReply;
			}
		} catch {
			// No reply yet.
		}
		if (Date.now() >= deadline) {
			throw new Error(`Timed out waiting for sibling reply to '${requestId}'. Escalate to the supervisor with the pending request id.`);
		}
		await delay(250, signal);
	}
}

/** Best-effort reaping: drops resolved pairs and orphan requests older than the TTL, then empty scopes. */
export function cleanupStaleSiblingScopes(nowMs = Date.now()): number {
	let removed = 0;
	let sessionEntries: fs.Dirent[];
	try {
		sessionEntries = fs.readdirSync(SIBLING_ROOT, { withFileTypes: true });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0;
		throw error;
	}
	for (const sessionEntry of sessionEntries) {
		if (!sessionEntry.isDirectory() || !sessionEntry.name.startsWith("s-")) continue;
		const sessionDir = path.join(SIBLING_ROOT, sessionEntry.name);
		let workflowEntries: fs.Dirent[];
		try {
			workflowEntries = fs.readdirSync(sessionDir, { withFileTypes: true });
		} catch {
			continue;
		}
		for (const workflowEntry of workflowEntries) {
			if (!workflowEntry.isDirectory() || !workflowEntry.name.startsWith("w-")) continue;
			const dir = path.join(sessionDir, workflowEntry.name);
			for (const sub of [REQUESTS_DIR, REPLIES_DIR, "forwarded"]) {
				const subDir = path.join(dir, sub);
				let files: string[];
				try {
					files = fs.readdirSync(subDir).filter((f) => f.endsWith(".json"));
				} catch {
					continue;
				}
				for (const file of files) {
					const filePath = path.join(subDir, file);
					try {
						const raw = fs.readFileSync(filePath, "utf-8");
						let parsed: Partial<PeerRequest & PeerReply>;
						try {
							parsed = JSON.parse(raw);
						} catch {
							// Unparseable debris: reap by mtime, never count it.
							if (nowMs - fs.statSync(filePath).mtimeMs > SIBLING_MESSAGE_TTL_MS) {
								fs.rmSync(filePath, { force: true });
								removed++;
							}
							continue;
						}
						const id = sub === REQUESTS_DIR ? parsed.id : parsed.requestId;
						const stale = typeof parsed.createdAt === "number" && nowMs - parsed.createdAt > SIBLING_MESSAGE_TTL_MS;
						const resolved = sub === REQUESTS_DIR && typeof id === "string" && hasReply(
							{ session: parsed.session ?? "", workflow: parsed.workflow ?? "" }, id,
						);
						// hasReply re-validates scope segments; a throw means foreign/corrupt — leave it.
						if (stale || resolved) {
							fs.rmSync(filePath, { force: true });
							removed++;
						}
					} catch {
						// Unparseable files are left alone; readers already skip them.
					}
				}
			}
			try {
				const remainingRequests = fs.readdirSync(path.join(dir, REQUESTS_DIR));
				const remainingReplies = fs.readdirSync(path.join(dir, REPLIES_DIR));
				let remainingForwarded: string[] = [];
				try {
					remainingForwarded = fs.readdirSync(path.join(dir, "forwarded"));
				} catch {
					// Never created; nothing to reap.
				}
				if (remainingRequests.length > 0 || remainingReplies.length > 0 || remainingForwarded.length > 0) continue;
				const forwardedDir = path.join(dir, "forwarded");
				const hasForwardedDir = fs.existsSync(forwardedDir);
				const newestMtime = Math.max(
					fs.statSync(dir).mtimeMs,
					fs.statSync(path.join(dir, REQUESTS_DIR)).mtimeMs,
					fs.statSync(path.join(dir, REPLIES_DIR)).mtimeMs,
					hasForwardedDir ? fs.statSync(forwardedDir).mtimeMs : 0,
				);
				if (nowMs - newestMtime < SIBLING_EMPTY_SCOPE_TTL_MS) continue;
				fs.rmdirSync(path.join(dir, REQUESTS_DIR));
				fs.rmdirSync(path.join(dir, REPLIES_DIR));
				if (hasForwardedDir) fs.rmdirSync(forwardedDir);
				fs.rmdirSync(dir);
				removed++;
			} catch {
				// Best-effort; a racing writer will be picked up on a later pass.
			}
		}
		try {
			if (fs.readdirSync(sessionDir).length === 0) fs.rmdirSync(sessionDir);
		} catch {
			// Best-effort.
		}
	}
	return removed;
}

/** Minimum spacing between best-effort GC passes in one process. */
export const SIBLING_CLEANUP_MIN_INTERVAL_MS = 5 * 60 * 1000;
let lastBestEffortCleanupAt = 0;

/**
 * Opportunistic cleanup for the parent relay's safety tick; never throws and
 * runs at most once per interval so a busy mailbox never pays a full-tree
 * scan on every tick.
 */
export function cleanupSiblingScopesBestEffort(nowMs = Date.now()): void {
	if (nowMs - lastBestEffortCleanupAt < SIBLING_CLEANUP_MIN_INTERVAL_MS) return;
	lastBestEffortCleanupAt = nowMs;
	try {
		cleanupStaleSiblingScopes(nowMs);
	} catch {
		// Mailbox delivery must never fail because best-effort temp cleanup failed.
	}
}
