/**
 * Unified peer-contact tools for siblings and the supervisor.
 *
 * One addressing model: `to` is a sibling workflow key or `"supervisor"`.
 * Blocking works both ways — a sibling ask with `awaitReply: true` waits for
 * the peer's answer (`contact_agent` with `replyTo`) exactly like a supervisor
 * decision waits for its reply. "Must respond" is enforced by timeout-plus-escalation: on expiry the
 * waiter throws and files a supervisor note carrying the pending request id,
 * so the supervisor can steer the silent peer.
 *
 * Identity is server-side: the workflow host threads `{ workflowRunId, selfKey }`
 * into the child runtime, and sends/replies bind to it. There is no `from`
 * parameter to spoof. Outside a workflow the peer branches fail closed.
 *
 * Peer content is untrusted: inbox output wraps every message in explicit
 * delimiters, and supervisor notes carry the same marking.
 */

import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { ChildRuntimeConfig, ChildSiblingIdentity, ChildSupervisorMetadata } from "../runs/shared/child-runtime-config.ts";
import {
	checkSiblingKey,
	listInbox,
	listPeerRequestsFor,
	recordSupervisorMessage,
	sendPeerRequest,
	waitForPeerReply,
	writePeerReply,
	MAX_PEER_WAIT_MS,
	MAX_SIBLING_MESSAGE_BYTES,
	type InboxEntry,
	type SiblingScope,
} from "./sibling-channels.ts";
import { sendSupervisorRequest } from "./native-supervisor-channel.ts";
import { SIBLING_TOOL_NAMES } from "./sibling-roster.ts";

export const CONTACT_AGENT_TOOL_NAME = "contact_agent";
export const INBOX_TOOL_NAME = "inbox";

export const UNTRUSTED_PEER_OPEN = "--- untrusted peer message begins ---";
export const UNTRUSTED_PEER_CLOSE = "--- untrusted peer message ends ---";

const REDACTED_DELIMITER = "[redacted peer delimiter]";

/** Wrap untrusted peer bytes so inner marker occurrences cannot break framing. */
export function quoteUntrusted(text: string): string {
	const safe = text.split(UNTRUSTED_PEER_OPEN).join(REDACTED_DELIMITER).split(UNTRUSTED_PEER_CLOSE).join(REDACTED_DELIMITER);
	return [UNTRUSTED_PEER_OPEN, safe, UNTRUSTED_PEER_CLOSE].join("\n");
}

const ContactAgentParamsSchema = Type.Object({
	// Omitted `to` addresses the supervisor: every pre-rename prompt keeps working verbatim.
	to: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
	message: Type.Optional(Type.String()),
	/** Sibling-only: answer the peer ask with this id instead of sending a new message. Unblocks a waiting asker. */
	replyTo: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
	/** Wait for the reply instead of returning after delivery. Default: true for the supervisor, false for siblings. */
	awaitReply: Type.Optional(Type.Boolean()),
	/** Blocking wait bound in ms (1s..10min). Default 5 minutes. */
	timeoutMs: Type.Optional(Type.Integer({ minimum: 1 })),
	/** Sibling-only idempotency key: retries reuse the original request. */
	idempotencyKey: Type.Optional(Type.String()),
	/** Supervisor-only: decision/interview/update reason. Default need_decision. */
	reason: Type.Optional(Type.String({ enum: ["need_decision", "interview_request", "progress_update"] })),
	/** Supervisor-only relay hint: the sibling workflow key this ask concerns. */
	about: Type.Optional(Type.String()),
	/** Supervisor-only structured interview payload. */
	interview: Type.Optional(Type.Unsafe({ type: "object", additionalProperties: true })),
}, { additionalProperties: false });

const InboxParamsSchema = Type.Object({
	/** Cursor from a previous call's details.nextCursor: only entries after it are returned. */
	after: Type.Optional(Type.Integer({ minimum: 0 })),
	afterId: Type.Optional(Type.String()),
	limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 50 })),
	/** Only peer asks still waiting on you (and notes not yet delivered). Default: full history. */
	pendingOnly: Type.Optional(Type.Boolean()),
}, { additionalProperties: false });

function hasTool(pi: ExtensionAPI, name: string): boolean {
	try {
		return pi.getAllTools?.().some((tool: { name?: unknown }) => tool.name === name) === true;
	} catch {
		return false;
	}
}

function siblingScope(
	metadata: ChildSupervisorMetadata | undefined,
	identity: ChildSiblingIdentity | undefined,
): SiblingScope {
	const session = metadata?.orchestratorSessionId?.trim() || metadata?.runId?.trim();
	if (!session) throw new Error("Sibling messaging is unavailable (no supervisor session).");
	if (!identity?.workflowRunId?.trim()) {
		throw new Error("Sibling messaging is available only to same-workflow siblings (no workflow identity).");
	}
	return { session, workflow: identity.workflowRunId.trim() };
}

function siblingSelfKey(identity: ChildSiblingIdentity | undefined): string {
	if (!identity?.selfKey?.trim()) {
		throw new Error("Sibling messaging is available only to same-workflow siblings (no workflow identity).");
	}
	return identity.selfKey.trim();
}

function resolveTimeoutMs(value: unknown): number {
	if (value === undefined) return 5 * 60 * 1000;
	if (typeof value !== "number" || !Number.isFinite(value) || value < 1) {
		throw new Error("timeoutMs must be a positive number of milliseconds.");
	}
	return Math.min(Math.floor(value), MAX_PEER_WAIT_MS);
}

function queueSupervisorProgressNote(
	metadata: ChildSupervisorMetadata,
	text: string,
): void {
	// Best-effort observability: peer traffic appears as a progress update on
	// the ordinary supervisor channel (same writer, same validation).
	void sendSupervisorRequest({ reason: "progress_update", message: text.slice(0, 4000) }, metadata).catch(() => {
		// Observability must never break peer delivery.
	});
}

interface ContactAgentInput {
	to?: string;
	message?: string;
	replyTo?: string;
	awaitReply?: boolean;
	timeoutMs?: number;
	idempotencyKey?: string;
	reason?: "need_decision" | "interview_request" | "progress_update";
	about?: string;
	interview?: unknown;
}

function formatInboxEntry(entry: InboxEntry): string {
	const at = new Date(entry.createdAt).toISOString();
	if (entry.kind === "supervisor") {
		// The supervisor is the authority; its text is not quoted as untrusted.
		return `- [supervisor] ${entry.id} @ ${at}:\n${entry.message}`;
	}
	const status = entry.kind === "ask"
		? entry.state === "answered"
			? `answered${entry.replyFrom ? ` by ${entry.replyFrom}` : ""}`
			: `expects reply — answer with contact_agent({ to: "${entry.from}", replyTo: "${entry.id}", message: "..." })`
		: entry.state === "delivered" ? "note" : "note, not yet pushed";
	return `- [${entry.kind}] ${entry.id} from ${entry.from} @ ${at} (${status}):\n${quoteUntrusted(entry.message)}`;
}

/** File the supervisor's answer beside the ask it resolved, so the inbox holds both halves. */
function recordSupervisorReply(
	metadata: ChildSupervisorMetadata,
	identity: ChildSiblingIdentity | undefined,
	result: { content?: Array<{ type?: string; text?: string }>; details?: Record<string, unknown> },
): void {
	try {
		if (!identity?.workflowRunId?.trim() || !identity.selfKey?.trim()) return;
		const text = result.content?.find((part) => part.type === "text")?.text ?? "";
		const reply = text.replace(/^\*\*Reply from supervisor:\*\*\n?/, "");
		if (!reply.trim()) return;
		recordSupervisorMessage({
			scope: siblingScope(metadata, identity),
			to: identity.selfKey.trim(),
			message: reply,
			kind: "reply",
			...(typeof result.details?.requestId === "string" ? { delivery: `to request ${result.details.requestId}` } : {}),
		});
	} catch {
		// History is best-effort; the reply already reached the child as the tool result.
	}
}

const REPLY_ONLY_REJECTED: ReadonlyArray<keyof ContactAgentInput> = ["awaitReply", "timeoutMs", "idempotencyKey", "reason", "about", "interview"];

/**
 * Answer a pending peer ask. The reply is keyed to the ask id, which is what
 * unblocks the asker; a fresh send would leave it waiting to timeout.
 */
function replyToPeerAsk(
	input: ContactAgentInput,
	metadata: ChildSupervisorMetadata,
	identity: ChildSiblingIdentity | undefined,
): { content: Array<{ type: "text"; text: string }>; details: Record<string, unknown> } {
	for (const field of REPLY_ONLY_REJECTED) {
		if (input[field] !== undefined) throw new Error(`${field} does not apply when answering a peer ask with replyTo.`);
	}
	const to = input.to?.trim();
	if (to !== undefined && to.toLowerCase() === "supervisor") {
		throw new Error("replyTo answers a sibling ask; set 'to' to the asking sibling's key (or omit it).");
	}
	const selfKey = siblingSelfKey(identity);
	const scope = siblingScope(metadata, identity);
	if (typeof input.message !== "string" || !input.message.trim()) throw new Error("message is required for sibling replies.");
	const reply = writePeerReply({ scope, requestId: input.replyTo!.trim(), from: selfKey, message: input.message });
	const addressee = to !== undefined && to !== "" && to !== reply.to ? `; note it was asked by '${reply.to}', not '${to}'` : "";
	return {
		content: [{ type: "text", text: `Replied to sibling '${reply.to}' (ask ${reply.requestId})${addressee}.` }],
		details: { replyTo: reply.requestId, from: reply.from, to: reply.to },
	};
}

export function registerSiblingTools(
	pi: ExtensionAPI,
	metadata: ChildSupervisorMetadata | undefined,
	runtimeConfig?: Pick<ChildRuntimeConfig, "sibling" | "siblingToolsExcluded">,
): void {
	if (!metadata) return;
	// Exclusions are per tool: dropping inbox must never remove supervisor contact.
	const excluded = new Set(runtimeConfig?.siblingToolsExcluded ?? []);
	const wanted = (name: (typeof SIBLING_TOOL_NAMES)[number]): boolean => !excluded.has(name) && !hasTool(pi, name);

	if (wanted(CONTACT_AGENT_TOOL_NAME)) {
		const contactTool: ToolDefinition<typeof ContactAgentParamsSchema, Record<string, unknown>> = {
			name: CONTACT_AGENT_TOOL_NAME,
			label: "Contact Agent",
			description: "Message the supervisor (default) or a sibling workflow key. Set awaitReply to block for the reply (default on for the supervisor, off for siblings). Pass replyTo with a peer ask id to answer that ask.",
			parameters: ContactAgentParamsSchema,
			async execute(id, params, signal) {
				const input = params as ContactAgentInput;
				// A reply is addressed by the ask id; `to` is optional there and only cross-checked.
				if (input.replyTo !== undefined) return replyToPeerAsk(input, metadata, runtimeConfig?.sibling);
				const to = (input.to ?? "supervisor").trim();
				if (!to) throw new Error("contact_agent 'to' must be a sibling workflow key or \"supervisor\".");
				if (to.toLowerCase() === "supervisor") {
					const reason = input.reason ?? "need_decision";
					const about = typeof input.about === "string" && input.about.trim() ? checkSiblingKey(input.about, "about") : undefined;
					if (reason !== "progress_update" && input.awaitReply === false) {
						throw new Error("contact_agent to supervisor with a blocking reason requires waiting; use reason progress_update for fire-and-forget.");
					}
					if (input.timeoutMs !== undefined) {
						throw new Error("timeoutMs applies to sibling waits only; supervisor deadlines are governed by PI_INTERCOM_ASK_TIMEOUT_MS.");
					}
					if (input.idempotencyKey !== undefined) {
						throw new Error("idempotencyKey applies to sibling sends only.");
					}
					const result = await sendSupervisorRequest({
						reason,
						...(input.message !== undefined ? { message: input.message } : {}),
						...(input.interview !== undefined ? { interview: input.interview } : {}),
						...(about !== undefined ? { to: about } : {}),
					}, metadata, signal, id);
					if (reason !== "progress_update") recordSupervisorReply(metadata, runtimeConfig?.sibling, result);
					return result;
				}
				const identity = runtimeConfig?.sibling;
				const selfKey = siblingSelfKey(identity);
				const scope = siblingScope(metadata, identity);
				if (typeof input.message !== "string" || !input.message.trim()) throw new Error("message is required for sibling messages.");
				if (Buffer.byteLength(input.message, "utf-8") > MAX_SIBLING_MESSAGE_BYTES) {
					throw new Error("Sibling message is too large (max 16 KiB).");
				}
				const sent = sendPeerRequest({
					scope,
					from: selfKey,
					to,
					message: input.message,
					expectsReply: input.awaitReply === true,
					...(typeof input.idempotencyKey === "string" && input.idempotencyKey.trim() ? { idempotencyKey: input.idempotencyKey } : {}),
				});
				queueSupervisorProgressNote(metadata, `PEER ${sent.from} → ${sent.to} (untrusted peer content, first 500 chars):\n${quoteUntrusted(sent.message.slice(0, 500))}`);
				if (input.awaitReply !== true) {
					return {
						content: [{ type: "text", text: `Direct message queued for sibling '${sent.to}'.` }],
						details: { delivered: true, id: sent.id, from: sent.from, to: sent.to },
					};
				}
				// Mutual blocking asks would deadlock until both timeouts expire: the
				// peer's ask arrives as steered input that this session only reads
				// after this tool returns. Yield to a counter-ask instead of blocking.
				// Tie-break: only the later asker yields, so exactly one side keeps waiting.
				const counterAsk = (): string | undefined => {
					const pending = listPeerRequestsFor(scope, selfKey, { limit: 50 });
					return pending.find((request) =>
						request.from === sent.to && request.expectsReply
						&& (request.createdAt < sent.createdAt || (request.createdAt === sent.createdAt && request.id < sent.id)),
					)?.id;
				};
				try {
					const reply = await waitForPeerReply(scope, sent.id, resolveTimeoutMs(input.timeoutMs), signal, () => {
						const askId = counterAsk();
						return askId === undefined
							? undefined
							: `Sibling '${sent.to}' is already waiting on you (ask ${askId}). Answer it first with contact_agent({ to: "${sent.to}", replyTo: "${askId}", message: "..." }), then resume your own ask by calling contact_agent again with the same to, message, awaitReply: true and idempotencyKey: "${sent.id}" (it stays queued; a plain retry would send a duplicate).`;
					});
					return {
						content: [{ type: "text", text: `**Reply from ${reply.from === "system" ? "system" : `sibling ${reply.from}`}:**\n${quoteUntrusted(reply.message)}` }],
						details: { requestId: sent.id, from: sent.from, to: sent.to, replyFrom: reply.from },
					};
				} catch (error) {
					queueSupervisorProgressNote(
						metadata,
						`UNANSWERED peer ask ${sent.from} → ${sent.to} (request ${sent.id}): ${error instanceof Error ? error.message : String(error)}`,
					);
					throw error;
				}
			},
		};
		pi.registerTool(contactTool);
	}

	if (wanted(INBOX_TOOL_NAME)) {
		const inboxTool: ToolDefinition<typeof InboxParamsSchema, Record<string, unknown>> = {
			name: INBOX_TOOL_NAME,
			label: "Inbox",
			description: "Everything addressed to you in this workflow: peer asks and notes (untrusted) and supervisor steers and replies. Oldest-first with a cursor; reads never consume. pendingOnly: true shows only asks still waiting on you.",
			parameters: InboxParamsSchema,
			execute(_id, params) {
				const input = params as { after?: number; afterId?: string; limit?: number; pendingOnly?: boolean };
				const identity = runtimeConfig?.sibling;
				const selfKey = siblingSelfKey(identity);
				const scope = siblingScope(metadata, identity);
				const entries = listInbox(scope, selfKey, {
					...(input.after !== undefined ? { after: input.after } : {}),
					...(input.afterId !== undefined ? { afterId: input.afterId } : {}),
					...(input.pendingOnly !== undefined ? { pendingOnly: input.pendingOnly } : {}),
					limit: input.limit ?? 20,
				});
				const lines = entries.length === 0
					? (input.pendingOnly ? "No pending peer asks." : "Inbox is empty.")
					: entries.map((entry) => formatInboxEntry(entry)).join("\n");
				const last = entries[entries.length - 1];
				return Promise.resolve({
					content: [{ type: "text", text: lines }],
					details: {
						selfKey,
						count: entries.length,
						entries: entries.map((entry) => ({ ...entry })),
						...(last ? { nextCursor: { after: last.createdAt, afterId: last.id } } : {}),
					},
				});
			},
		};
		pi.registerTool(inboxTool);
	}

}
