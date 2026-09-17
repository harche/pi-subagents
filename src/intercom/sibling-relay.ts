/**
 * Parent-side automatic forwarding for peer asks.
 *
 * This is the push half of sibling messaging, built on the exact delivery
 * path supervisor steers use (`steerWorkflowChildByKey`: foreground session
 * routing plus the async file control inbox, with correlated receipts). The
 * parent never approves, edits, or answers peer traffic — it only transports:
 * a new peer ask is steered to its target sibling as user input, no model
 * turn is triggered, and the outcome is journaled beside the ask.
 *
 * Outcomes per ask (tracked by `forwarded/<id>.json` so restarts never
 * double-push):
 * - delivered/queued → forwarded; the waiter resolves when the peer replies.
 * - missed because the target is terminal → a `system` reply is filed so the
 *   waiter fails fast instead of blocking to timeout.
 * - missed for any other reason (starting up, no live route yet) → left
 *   unforwarded for the next poll.
 * Pull (`inbox`) keeps working throughout as catch-up and history.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { shouldUseNativeFsWatch } from "../shared/watch-strategy.ts";
import { writePrivateAtomicJson } from "../shared/atomic-json.ts";
import {
	cleanupSiblingScopesBestEffort,
	siblingRootDir,
	SIBLING_RESERVED_WORDS,
	writePeerReply,
	type PeerRequest,
} from "./sibling-channels.ts";
import { DIRS, type SubagentState } from "../shared/types.ts";
import { steerAsyncRun } from "../runs/foreground/async-steering-action.ts";
import { workflowSteerReceipt } from "../runs/foreground/subagent-executor.ts";
import { quoteUntrusted } from "./sibling-tools.ts";

const FORWARDED_DIR = "forwarded";
const REPLIES_DIR = "replies";
const POLL_MS = 1000;
const SAFETY_POLL_MS = 5000;
const MAX_FORWARDS_PER_POLL = 20;
/** Idle backoff ceiling when fs.watch drives delivery; the tick is only a safety net. */
const IDLE_MAX_WATCHED_MS = 30_000;
/** Idle backoff ceiling when polling is the only delivery path: a blocking ask must not sit for 30s. */
const IDLE_MAX_POLLED_MS = 5000;
/** How long an ask may stay unroutable (no step, no live run) before it is declared undeliverable. */
const UNKNOWN_TARGET_GRACE_MS = 30_000;

type SiblingRelayWatch = (filename: fs.PathLike, listener: fs.WatchListener<string>) => fs.FSWatcher;

export type SiblingRelayOutcome = "delivered" | "queued" | "missed" | "failed";

export interface SiblingRelaySteer {
	(workflowRunId: string, key: string, message: string): Promise<{ state: SiblingRelayOutcome; error?: string }>;
}

export interface ForwardReceipt {
	version: 1;
	requestId: string;
	workflowRunId: string;
	to: string;
	outcome: SiblingRelayOutcome;
	error?: string;
	createdAt: number;
}

interface SiblingRelayDeps {
	state: SubagentState;
	steer: SiblingRelaySteer;
	/** Direct child steer used by the status-scan fallback; defaults to the real path. */
	steerDirect?: (input: { runId: string; asyncDir: string; message: string }) => Promise<{ state: SiblingRelayOutcome; error?: string }>;

	pollMs?: number;
	platform?: NodeJS.Platform;
	watch?: SiblingRelayWatch;
	timers?: Pick<typeof globalThis, "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout">;
	/** Async runs root for the status-scan fallback; defaults to DIRS.async. */
	asyncDirRoot?: string;
	/** Age in ms after which a still-unforwarded ask warns once. */
	staleWarnAfterMs?: number;
	/** Age in ms after which an ask whose target is neither listed nor running is failed as undeliverable. */
	unknownTargetGraceMs?: number;
}

function siblingRoot(): string {
	// Sibling mailboxes live beside the supervisor channels under the shared temp root.
	return siblingRootDir();
}

function forwardedPath(scopeDir: string, id: string): string {
	return path.join(scopeDir, FORWARDED_DIR, `${id}.json`);
}

/** Request ids that already have a marker file in `sub` (one readdir; the relay writes these itself). */
function markedIds(scopeDir: string, sub: string): Set<string> {
	try {
		return new Set(fs.readdirSync(path.join(scopeDir, sub)).filter((f) => f.endsWith(".json")).map((f) => f.slice(0, -".json".length)));
	} catch {
		return new Set();
	}
}

function markForwarded(scopeDir: string, receipt: ForwardReceipt): void {
	try {
		fs.mkdirSync(path.join(scopeDir, FORWARDED_DIR), { recursive: true, mode: 0o700 });
		writePrivateAtomicJson(forwardedPath(scopeDir, receipt.requestId), receipt);
	} catch {
		// Best-effort; a missing marker only risks a duplicate push, never loss.
	}
}

function parsePeerRequest(file: string): (PeerRequest & { scopeDir: string }) | undefined {
	try {
		const parsed = JSON.parse(fs.readFileSync(file, "utf-8")) as Partial<PeerRequest>;
		if (parsed.version !== 1 || typeof parsed.id !== "string" || typeof parsed.from !== "string" ||
			typeof parsed.to !== "string" || typeof parsed.message !== "string" ||
			typeof parsed.createdAt !== "number" || typeof parsed.expectsReply !== "boolean" ||
			typeof parsed.session !== "string" || typeof parsed.workflow !== "string") return undefined;
		return { ...(parsed as PeerRequest), scopeDir: path.dirname(path.dirname(file)) };
	} catch {
		return undefined;
	}
}

function hasReplyFile(scopeDir: string, requestId: string): boolean {
	try {
		const parsed = JSON.parse(fs.readFileSync(path.join(scopeDir, "replies", `${requestId}.json`), "utf-8")) as { version?: unknown; requestId?: unknown };
		return parsed.version === 1 && parsed.requestId === requestId;
	} catch {
		return false;
	}
}

function terminalMiss(error: string | undefined): boolean {
	if (!error) return false;
	// Mirrors the terminal members of WORKFLOW_STEP_STATES (subagent-executor.ts);
	// paused and "no live route" are transient and keep retrying. "unknown" is
	// the keyed router's verdict that the workflow inventory never had the key.
	return /\bis (completed|complete|failed|stopped|partial|rejected|unknown)\b/i.test(error);
}

/** Keyed routing reported that the workflow inventory has no such step. */
function unknownTarget(error: string | undefined): boolean {
	return typeof error === "string" && /\bis unknown\b/i.test(error);
}

function steerMessage(request: PeerRequest): string {
	const quoted = quoteUntrusted(request.message);
	// A note is settled by the relay itself on delivery, so the target must not
	// try to reply: a replyTo for it would fail with "already has a reply".
	const header = request.expectsReply
		? `Peer ask from sibling '${request.from}'. Answer with: contact_agent({ to: "${request.from}", replyTo: "${request.id}", message: "..." })`
		: `Peer note from sibling '${request.from}' (no reply expected; do not answer it with replyTo).`;
	return [header, quoted].join("\n");
}

/**
 * Resolve a live child run for (workflowRunId, sibling key) by scanning child
 * status files. Covers flows with no workflow status record (e.g. headless
 * runs whose workflow id is a tool-call id), where the keyed steer path has
 * nothing to read.
 */
const TERMINAL_CHILD_STATES = new Set(["complete", "completed", "failed", "stopped", "partial", "rejected"]);

/**
 * Newest matching child status wins. A live run is returned as a steer
 * target; a terminal-only match is reported so the waiter can fail fast
 * instead of blocking to timeout.
 */
function resolveSiblingRunId(
	asyncDirRoot: string,
	workflowRunId: string,
	key: string,
): { live: { runId: string; asyncDir: string } } | { terminalState: string } | undefined {
	let entries: fs.Dirent[];
	try {
		entries = fs.readdirSync(asyncDirRoot, { withFileTypes: true });
	} catch {
		return undefined;
	}
	let terminal: { state: string; updatedAt: number } | undefined;
	for (const entry of entries) {
		if (!entry.isDirectory()) continue;
		const statusFile = path.join(asyncDirRoot, entry.name, "status.json");
		let status: { state?: unknown; parentWorkflowRunId?: unknown; workflowKey?: unknown; updatedAt?: unknown };
		try {
			status = JSON.parse(fs.readFileSync(statusFile, "utf-8"));
		} catch {
			continue;
		}
		if (status.parentWorkflowRunId !== workflowRunId || status.workflowKey !== key) continue;
		if (status.state === "running" || status.state === "queued") {
			return { live: { runId: entry.name, asyncDir: path.join(asyncDirRoot, entry.name) } };
		}
		if (typeof status.state === "string" && TERMINAL_CHILD_STATES.has(status.state)) {
			const updatedAt = typeof status.updatedAt === "number" ? status.updatedAt : 0;
			if (!terminal || updatedAt >= terminal.updatedAt) terminal = { state: status.state, updatedAt };
		}
	}
	return terminal ? { terminalState: terminal.state } : undefined;
}

export function createSiblingRelay(_pi: ExtensionAPI, deps: SiblingRelayDeps): {
	start: () => void;
	activateTransport: () => void;
	dispose: () => void;
	poll: () => Promise<{ forwarded: number; systemReplied: number }>;
} {
	const timers = deps.timers ?? globalThis;
	const watch = deps.watch ?? fs.watch;
	const platform = deps.platform ?? process.platform;
	const pollMs = deps.pollMs ?? POLL_MS;
	const useNativeWatcher = () => shouldUseNativeFsWatch("sibling-relay", platform) && platform !== "win32";
	let poller: ReturnType<typeof setInterval> | undefined;
	let started = false;
	const asyncDirRoot = deps.asyncDirRoot ?? DIRS.async;
	const staleWarnAfterMs = deps.staleWarnAfterMs ?? 30_000;
	const unknownTargetGraceMs = deps.unknownTargetGraceMs ?? UNKNOWN_TARGET_GRACE_MS;
	const steerDirect = deps.steerDirect ?? (async (input: { runId: string; asyncDir: string; message: string }) => {
		const direct = await steerAsyncRun({
			state: deps.state,
			runId: input.runId,
			message: input.message,
			mode: "auto",
			ackTimeoutMs: 3000,
			location: { asyncDir: input.asyncDir },
		});
		const mapped = workflowSteerReceipt(input.runId, direct);
		return { state: (mapped.state === "failed" ? "failed" : mapped.state) as SiblingRelayOutcome, ...(mapped.error ? { error: mapped.error } : {}) };
	});
	const mapKey = (scopeDir: string, id: string): string => `${scopeDir}\n${id}`;
	const firstSeen = new Map<string, number>();
	const warnedStale = new Set<string>();
	const inFlight = new Set<string>();
	const missBackoffUntil = new Map<string, number>();
	const MISS_BACKOFF_BASE_MS = 5000;
	const MISS_BACKOFF_MAX_MS = 30_000;
	let idleStreak = 0;
	const ownerSessionId = (): string | undefined =>
		deps.state.supervisorOwnerSessionId ?? deps.state.currentSessionId ?? undefined;
	const requestWatchers = new Map<string, fs.FSWatcher>();
	let rootWatcher: fs.FSWatcher | undefined;

	const startPolling = (): void => {
		if (poller) return;
		const tick = (): void => {
			if (!started) return;
			// GC runs here, throttled, so no child pays for it on a tool call.
			cleanupSiblingScopesBestEffort();
			// A new workflow under an already-watched session dir is invisible to the
			// non-recursive root watcher; the safety tick picks it up. It also drops
			// watchers whose directory GC removed, so a recreated one is re-watched.
			if (useNativeWatcher()) watchExistingRequestsDirs();
			void poll()
				.catch(() => {})
				.finally(() => {
					if (!started) return;
					const base = useNativeWatcher() ? SAFETY_POLL_MS : pollMs;
					const ceiling = useNativeWatcher() ? IDLE_MAX_WATCHED_MS : IDLE_MAX_POLLED_MS;
					const delayMs = Math.min(base * 2 ** Math.min(idleStreak, 5), ceiling);
					poller = timers.setTimeout(tick, delayMs);
					poller.unref?.();
				});
		};
		poller = timers.setTimeout(tick, useNativeWatcher() ? SAFETY_POLL_MS : pollMs);
		poller.unref?.();
	};
	const dropWatcher = (requestsDir: string): void => {
		const watcher = requestWatchers.get(requestsDir);
		if (!watcher) return;
		try { watcher.close(); } catch {}
		requestWatchers.delete(requestsDir);
	};
	const watchRequestsDir = (requestsDir: string): void => {
		if (requestWatchers.has(requestsDir)) return;
		try {
			const watcher = watch(requestsDir, () => {
				// A watched dir that was removed emits no error; it just goes inert.
				// Drop it so the next scan re-watches the recreated directory.
				if (!fs.existsSync(requestsDir)) dropWatcher(requestsDir);
				void poll().catch(() => {});
			});
			watcher.on("error", () => {
				try { watcher.close(); } catch {}
				requestWatchers.delete(requestsDir);
				startPolling();
			});
			watcher.unref?.();
			requestWatchers.set(requestsDir, watcher);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") startPolling();
		}
	};
	const watchExistingRequestsDirs = (): void => {
		for (const requestsDir of [...requestWatchers.keys()]) {
			if (!fs.existsSync(requestsDir)) dropWatcher(requestsDir);
		}
		let sessionEntries: fs.Dirent[];
		try {
			sessionEntries = fs.readdirSync(siblingRoot(), { withFileTypes: true });
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			startPolling();
			return;
		}
		for (const sessionEntry of sessionEntries) {
			if (!sessionEntry.isDirectory() || !sessionEntry.name.startsWith("s-")) continue;
			let workflowEntries: fs.Dirent[];
			try {
				workflowEntries = fs.readdirSync(path.join(siblingRoot(), sessionEntry.name), { withFileTypes: true });
			} catch {
				continue;
			}
			for (const workflowEntry of workflowEntries) {
				if (!workflowEntry.isDirectory() || !workflowEntry.name.startsWith("w-")) continue;
				watchRequestsDir(path.join(siblingRoot(), sessionEntry.name, workflowEntry.name, "requests"));
			}
		}
	};

	const poll = async (): Promise<{ forwarded: number; systemReplied: number }> => {
		let forwarded = 0;
		let systemReplied = 0;
		const owner = ownerSessionId();
		let sessionEntries: fs.Dirent[];
		try {
			sessionEntries = fs.readdirSync(siblingRoot(), { withFileTypes: true });
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return { forwarded, systemReplied };
			throw error;
		}
		const candidates: Array<PeerRequest & { scopeDir: string }> = [];
		for (const sessionEntry of sessionEntries) {
			if (!sessionEntry.isDirectory() || !sessionEntry.name.startsWith("s-")) continue;
			let workflowEntries: fs.Dirent[];
			try {
				workflowEntries = fs.readdirSync(path.join(siblingRoot(), sessionEntry.name), { withFileTypes: true });
			} catch {
				continue;
			}
			for (const workflowEntry of workflowEntries) {
				if (!workflowEntry.isDirectory() || !workflowEntry.name.startsWith("w-")) continue;
				const scopeDir = path.join(siblingRoot(), sessionEntry.name, workflowEntry.name);
				let files: string[];
				try {
					files = fs.readdirSync(path.join(scopeDir, "requests")).filter((f) => f.endsWith(".json"));
				} catch {
					continue;
				}
				// Two readdirs per scope instead of one open+parse per message: a
				// forwarded ask stays on disk up to the TTL and is skipped by name.
				const forwardedIds = markedIds(scopeDir, FORWARDED_DIR);
				const repliedIds = markedIds(scopeDir, REPLIES_DIR);
				for (const file of files) {
					const id = file.slice(0, -".json".length);
					if (forwardedIds.has(id)) continue;
					const request = parsePeerRequest(path.join(scopeDir, "requests", file));
					if (!request) continue;
					// Supervisor entries were pushed by the steer that recorded them; only peers are relayed.
					if (SIBLING_RESERVED_WORDS.has(request.from)) continue;
					// Foreign sessions belong to their own relay; never touch them.
					if (owner !== undefined && request.session !== owner) continue;
					if (repliedIds.has(request.id) && hasReplyFile(scopeDir, request.id)) {
						markForwarded(scopeDir, {
							version: 1, requestId: request.id, workflowRunId: request.workflow,
							to: request.to, outcome: "delivered", createdAt: Date.now(),
						});
						forgetTracked(scopeDir, request.id);
						continue;
					}
					candidates.push(request);
				}
			}
		}
		// Oldest first so a stuck head never starves the tail; bound the batch.
		candidates.sort((a, b) => a.createdAt - b.createdAt || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
		const pending = candidates.slice(0, MAX_FORWARDS_PER_POLL);
		if (pending.length === 0) {
			idleStreak++;
			for (const tracked of [...firstSeen.keys(), ...warnedStale]) {
				const sep = tracked.indexOf("\n");
				if (sep === -1) {
					firstSeen.delete(tracked);
					warnedStale.delete(tracked);
					missBackoffUntil.delete(tracked);
					missBackoffUntil.delete(`${tracked}:n`);
					continue;
				}
				const scopeDir = tracked.slice(0, sep);
				const id = tracked.slice(sep + 1);
				if (!fs.existsSync(path.join(scopeDir, "requests", `${id}.json`))) {
					firstSeen.delete(tracked);
					warnedStale.delete(tracked);
					missBackoffUntil.delete(tracked);
					missBackoffUntil.delete(`${tracked}:n`);
				}
			}
			return { forwarded, systemReplied };
		}
		idleStreak = 0;
		const now = Date.now();
		for (const request of pending) {
			const tracked = mapKey(request.scopeDir, request.id);
			if (inFlight.has(tracked)) continue;
			if ((missBackoffUntil.get(tracked) ?? 0) > now) continue;
			inFlight.add(tracked);
			try {
				const outcome = await forwardOne(request);
				if (outcome === "forwarded") forwarded++;
				else if (outcome === "system-replied") systemReplied++;
			} finally {
				inFlight.delete(tracked);
			}
		}
		return { forwarded, systemReplied };
	};

	const forgetTracked = (scopeDir: string, id: string): void => {
		const tracked = mapKey(scopeDir, id);
		firstSeen.delete(tracked);
		warnedStale.delete(tracked);
		missBackoffUntil.delete(tracked);
		missBackoffUntil.delete(`${tracked}:n`);
	};
	/** Fire-and-forget asks are complete once pushed: file a system receipt so they stop listing as pending and become reapable. */
	const settleNotification = (request: PeerRequest, outcome: SiblingRelayOutcome): void => {
		if (request.expectsReply) return;
		try {
			writePeerReply({
				scope: { session: request.session, workflow: request.workflow },
				requestId: request.id,
				from: "system",
				systemReply: true,
				message: `Delivered to sibling '${request.to}' (${outcome}).`,
			});
		} catch {
			// A racing peer reply or a foreign scope wins; the ask still lists until reaped.
		}
	};

	const noteStale = (scopeDir: string, request: PeerRequest, outcome: SiblingRelayOutcome, error: string | undefined): void => {
		const tracked = mapKey(scopeDir, request.id);
		const at = Date.now();
		if (!firstSeen.has(tracked)) firstSeen.set(tracked, at);
		if (!warnedStale.has(tracked) && at - (firstSeen.get(tracked) ?? at) >= staleWarnAfterMs) {
			warnedStale.add(tracked);
			console.warn(
				`[pi-subagents] sibling ask ${request.id} (${request.from} → ${request.to}) unforwarded after ${staleWarnAfterMs}ms; last outcome: ${outcome}${error ? ` (${error})` : ""}.`,
			);
		}
	};

	const forwardOne = async (request: PeerRequest & { scopeDir: string }): Promise<"forwarded" | "system-replied" | "retry"> => {
		let outcome: SiblingRelayOutcome = "failed";
		let error: string | undefined;
		// Tool-call-derived workflow ids (e.g. headless `call_…` runs) have no
		// workflow status record by construction: skip straight to the scan.
		const looksToolCallScoped = request.workflow.startsWith("call_");
		if (!looksToolCallScoped) {
			try {
				const receipt = await deps.steer(request.workflow, request.to, steerMessage(request));
				outcome = receipt.state;
				error = receipt.error;
			} catch (err) {
				error = err instanceof Error ? err.message : String(err);
			}
			if (outcome === "delivered" || outcome === "queued") {
				markForwarded(request.scopeDir, {
					version: 1, requestId: request.id, workflowRunId: request.workflow,
					to: request.to, outcome, createdAt: Date.now(),
				});
				settleNotification(request, outcome);
				forgetTracked(request.scopeDir, request.id);
				return "forwarded";
			}
		}
		if ((outcome === "missed" && (!terminalMiss(error) || unknownTarget(error))) || looksToolCallScoped) {
			// Keyed routing found nothing usable: resolve the live child run by
			// status scan and steer it directly on the same path. An "unknown"
			// verdict from the keyed router is not final on its own: a scripted
			// workflow registers steps as it launches them, so the scan (and a
			// grace period) decides.
			const keyedUnknown = unknownTarget(error);
			if (keyedUnknown) error = `Workflow child '${request.to}' had no live steering route.`;
			try {
				const target = resolveSiblingRunId(asyncDirRoot, request.workflow, request.to);
				if (target === undefined && (looksToolCallScoped || outcome === "missed")
					&& Date.now() - request.createdAt >= unknownTargetGraceMs) {
					// No step lists the key and no child run carries it: a typo or a
					// key from another workflow. Fail the waiter now rather than after
					// its full timeout (and stop re-steering the orphan until the TTL).
					outcome = "missed";
					error = `Workflow child '${request.to}' is unknown to this workflow${keyedUnknown ? " (not in the workflow inventory)" : ""}.`;
				} else if (target && "terminalState" in target) {
					// The only record of this sibling is a finished run: fail fast below.
					outcome = "missed";
					error = `Workflow child '${request.to}' is ${target.terminalState}.`;
				} else if (target) {
					const direct = await steerDirect({ runId: target.live.runId, asyncDir: target.live.asyncDir, message: steerMessage(request) });
					outcome = direct.state;
					error = direct.error;
					if (outcome === "delivered" || outcome === "queued") {
						markForwarded(request.scopeDir, {
							version: 1, requestId: request.id, workflowRunId: request.workflow,
							to: request.to, outcome, createdAt: Date.now(),
						});
						settleNotification(request, outcome);
						forgetTracked(request.scopeDir, request.id);
						return "forwarded";
					}
				}
			} catch (err) {
				error = err instanceof Error ? err.message : String(err);
			}
		}
		if (outcome === "missed" && terminalMiss(error)) {
			// Fail fast for the waiter with an honest system reply, then journal.
			try {
				writePeerReply({
					scope: { session: request.session, workflow: request.workflow },
					requestId: request.id,
					from: "system",
					systemReply: true,
					message: unknownTarget(error)
						? `Undeliverable: no sibling '${request.to}' exists in this workflow (${error}). Check the key against your roster or ask the supervisor.`
						: `Undeliverable: sibling '${request.to}' is no longer running (${error}). Restate the question to the supervisor instead.`,
				});
			} catch {
				// A racing peer reply wins; the waiter resolves either way.
			}
			markForwarded(request.scopeDir, {
				version: 1, requestId: request.id, workflowRunId: request.workflow,
				to: request.to, outcome: "missed", ...(error ? { error } : {}), createdAt: Date.now(),
			});
			forgetTracked(request.scopeDir, request.id);
			return "system-replied";
		}
		// Still unresolved: back off this ask, warn once when stale, retry later.
		const tracked = mapKey(request.scopeDir, request.id);
		const misses = (missBackoffUntil.get(tracked + ":n") ?? 0) + 1;
		missBackoffUntil.set(tracked + ":n", misses);
		missBackoffUntil.set(tracked, Date.now() + Math.min(MISS_BACKOFF_BASE_MS * misses, MISS_BACKOFF_MAX_MS));
		noteStale(request.scopeDir, request, outcome, error);
		return "retry";
	};


	return {
		start: () => {
			if (started) return;
			started = true;
			void poll().catch(() => {});
			if (!useNativeWatcher()) {
				startPolling();
				return;
			}
			try {
				fs.mkdirSync(siblingRoot(), { recursive: true });
				watchExistingRequestsDirs();
				rootWatcher = watch(siblingRoot(), () => {
					watchExistingRequestsDirs();
					void poll().catch(() => {});
				});
				rootWatcher.on("error", startPolling);
				rootWatcher.unref?.();
			} catch {
				startPolling();
			}
			startPolling();
		},
		activateTransport: () => {
			if (!started) return;
			watchExistingRequestsDirs();
			void poll().catch(() => {});
			startPolling();
		},
		dispose: () => {
			started = false;
			firstSeen.clear();
			warnedStale.clear();
			inFlight.clear();
			missBackoffUntil.clear();
			idleStreak = 0;
			try { rootWatcher?.close(); } catch {
				// Best effort during shutdown.
			}
			rootWatcher = undefined;
			for (const watcher of requestWatchers.values()) {
				try { watcher.close(); } catch {}
			}
			requestWatchers.clear();
			if (poller) {
				try {
					timers.clearInterval(poller as never);
				} catch {}
				try {
					timers.clearTimeout(poller as never);
				} catch {}
			}
			poller = undefined;
		},
		poll,
	};
}

export const __siblingRelayInternals = { terminalMiss, steerMessage, MAX_FORWARDS_PER_POLL, IDLE_MAX_POLLED_MS, IDLE_MAX_WATCHED_MS };
