import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import {
	cleanupStaleSiblingScopes,
	listInbox,
	recordSupervisorMessage,
	listPeerRequestsFor,
	sanitizeScopeSegment,
	sendPeerRequest,
	waitForPeerReply,
	writePeerReply,
	siblingRootDir,
	SIBLING_EMPTY_SCOPE_TTL_MS,
	SIBLING_MESSAGE_TTL_MS,
	type SiblingScope,
} from "../../src/intercom/sibling-channels.ts";

function scope(tag: string, workflow = "wf-1"): SiblingScope {
	return { session: `sess-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, workflow };
}

describe("sanitizeScopeSegment", () => {
	it("rejects only empty values and maps the rest without collisions", () => {
		assert.throws(() => sanitizeScopeSegment(""), /must not be empty/);
		assert.throws(() => sanitizeScopeSegment("   "), /must not be empty/);
		assert.equal(sanitizeScopeSegment("abc-123_X.9"), "abc-123_X.9");
		// Tool-call-derived workflow ids (slashes, pipes, percent-escapes) map safely.
		const mapped = sanitizeScopeSegment("call_01a0b0a7%7Cfc_01a0b0a7");
		assert.match(mapped, /^[A-Za-z0-9._-]+$/);
		assert.ok(!mapped.includes("%") && !mapped.includes("|"));
		// Distinct ids never collide, even after truncation.
		const longA = "w".repeat(200) + "-a";
		const longB = "w".repeat(200) + "-b";
		assert.notEqual(sanitizeScopeSegment(longA), sanitizeScopeSegment(longB));
		assert.ok(sanitizeScopeSegment(longA).length <= 128);
		assert.notEqual(
			sanitizeScopeSegment("session:a:b"),
			sanitizeScopeSegment("session:a-b"),
		);
	});
});

describe("peer request isolation", () => {
	it("delivers asks to the addressed sibling only", () => {
		const s = scope("iso");
		sendPeerRequest({ scope: s, from: "api", to: "ui", message: "hi", expectsReply: false });
		assert.equal(listPeerRequestsFor(s, "ui").length, 1);
		assert.equal(listPeerRequestsFor(s, "api").length, 0);
	});

	it("keeps concurrent workflows in one session isolated", () => {
		const base = `sess-shared-${Date.now()}`;
		const a: SiblingScope = { session: base, workflow: "wf-a" };
		const b: SiblingScope = { session: base, workflow: "wf-b" };
		sendPeerRequest({ scope: a, from: "s1", to: "api", message: "for-a", expectsReply: false });
		sendPeerRequest({ scope: b, from: "s1", to: "api", message: "for-b", expectsReply: false });
		assert.deepEqual(listPeerRequestsFor(a, "api").map((m) => m.message), ["for-a"]);
		assert.deepEqual(listPeerRequestsFor(b, "api").map((m) => m.message), ["for-b"]);
	});

	it("rejects self-send, empty, oversized, and invalid keys", () => {
		const s = scope("neg");
		assert.throws(() => sendPeerRequest({ scope: s, from: "api", to: "api", message: "hi", expectsReply: false }), /yourself/);
		assert.throws(() => sendPeerRequest({ scope: s, from: "api", to: "ui", message: "  ", expectsReply: false }), /required/);
		assert.throws(() => sendPeerRequest({ scope: s, from: "bad key!", to: "ui", message: "hi", expectsReply: false }), /workflow key/);
		assert.throws(() => sendPeerRequest({ scope: s, from: "api", to: "", message: "hi", expectsReply: false }), /workflow key/);
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "api", to: "ui", message: "x".repeat(17 * 1024), expectsReply: false }),
			/too large/,
		);
		// 4-byte emoji: char-length under schema caps but byte-length over the channel cap.
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "api", to: "ui", message: "🚀".repeat(5000), expectsReply: false }),
			/too large/,
		);
	});
});

describe("peer request cursor and idempotency", () => {
	it("paginates oldest-first with an explicit cursor and deterministic ties", () => {
		const s = scope("cursor");
		const first = sendPeerRequest({ scope: s, from: "a", to: "b", message: "one", expectsReply: false });
		const second = sendPeerRequest({ scope: s, from: "a", to: "b", message: "two", expectsReply: false });
		const page1 = listPeerRequestsFor(s, "b", { limit: 1 });
		assert.equal(page1.length, 1);
		const page2 = listPeerRequestsFor(s, "b", { after: page1[0]!.createdAt, afterId: page1[0]!.id, limit: 1 });
		assert.equal(page2.length, 1);
		assert.notEqual(page1[0]!.id, page2[0]!.id);
		assert.deepEqual(
			new Set([page1[0]!.message, page2[0]!.message]),
			new Set([first.message, second.message]),
		);
		assert.equal(listPeerRequestsFor(s, "b", { after: page2[0]!.createdAt, afterId: page2[0]!.id }).length, 0);
	});

	it("returns the original request for a retried idempotency key", () => {
		const s = scope("idem");
		const first = sendPeerRequest({ scope: s, from: "a", to: "b", message: "once", expectsReply: true, idempotencyKey: "op-1" });
		const retry = sendPeerRequest({ scope: s, from: "a", to: "b", message: "once", expectsReply: true, idempotencyKey: "op-1" });
		assert.equal(retry.id, first.id);
		assert.equal(listPeerRequestsFor(s, "b").length, 1);
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "a", to: "b", message: "x", expectsReply: false, idempotencyKey: "../evil" }),
			/invalid/,
		);
	});

	it("enforces the per-scope request cap", () => {
		const s = scope("cap");
		for (let i = 0; i < 200; i++) {
			sendPeerRequest({ scope: s, from: "a", to: `k${i % 50}`, message: `m${i}`, expectsReply: false, idempotencyKey: `cap-${i}` });
		}
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "a", to: "b", message: "overflow", expectsReply: false }),
			/mailbox is full/,
		);
	});
});

describe("peer reply lifecycle", () => {
	it("resolves pending asks and unblocks waiters", async () => {
		const s = scope("reply");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "decide?", expectsReply: true });
		assert.equal(listPeerRequestsFor(s, "b").length, 1);
		const waiter = waitForPeerReply(s, ask.id, 5000);
		const reply = writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "yes" });
		assert.equal(reply.requestId, ask.id);
		assert.equal((await waiter).message, "yes");
		// Resolved asks leave the pending list.
		assert.equal(listPeerRequestsFor(s, "b").length, 0);
	});

	it("rejects unknown and double replies", () => {
		const s = scope("reply-neg");
		assert.throws(() => writePeerReply({ scope: s, requestId: "missing", from: "b", message: "x" }), /No pending sibling request/);
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "q", expectsReply: true });
		writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "a1" });
		assert.throws(() => writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "a2" }), /already has a reply/);
	});

	it("times out with an escalation-ready error", async () => {
		const s = scope("reply-timeout");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "hello?", expectsReply: true });
		await assert.rejects(waitForPeerReply(s, ask.id, 50), /Timed out waiting for sibling reply.*Escalate/);
	});
});

describe("reserved identities", () => {
	it("rejects system and supervisor as sibling keys", () => {
		const s = scope("reserved");
		for (const name of ["system", "System", "SUPERVISOR", "supervisor"]) {
			assert.throws(() => sendPeerRequest({ scope: s, from: name, to: "b", message: "x", expectsReply: false }), /reserved/);
			assert.throws(() => sendPeerRequest({ scope: s, from: "a", to: name, message: "x", expectsReply: false }), /reserved/);
		}
	});
});

describe("inbox history", () => {
	it("lists supervisor entries as delivered history and keeps them out of pending views", () => {
		const s = scope("inbox");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "q?", expectsReply: true });
		sendPeerRequest({ scope: s, from: "c", to: "b", message: "fyi", expectsReply: false });
		const steer = recordSupervisorMessage({ scope: s, to: "b", message: "Do X.", kind: "steer", delivery: "delivered" });
		assert.equal(steer.from, "supervisor");
		assert.ok(steer.id.startsWith("sup-"));
		const all = listInbox(s, "b");
		// Same-millisecond writes tie on createdAt and order by id, so compare as sets.
		assert.deepEqual(all.map((e) => `${e.kind}:${e.state}`).sort(), ["ask:pending", "note:pending", "supervisor:delivered"]);
		const noteId = all.find((e) => e.kind === "note")!.id;
		assert.deepEqual(listPeerRequestsFor(s, "b").map((r) => r.id).sort(), [ask.id, noteId].sort());
		assert.deepEqual(listInbox(s, "b", { pendingOnly: true }).map((e) => e.kind).sort(), ["ask", "note"]);
		writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "answer" });
		const answered = listInbox(s, "b").find((e) => e.id === ask.id)!;
		assert.equal(answered.state, "answered");
		assert.equal(answered.replyFrom, "b");
		assert.equal(listPeerRequestsFor(s, "b").length, 1);
		// The supervisor entry cannot be "answered": it is already settled.
		assert.throws(() => writePeerReply({ scope: s, requestId: steer.id, from: "b", message: "?" }), /already has a reply/);
		// Nobody else sees b's inbox.
		assert.equal(listInbox(s, "a").length, 0);
	});

	it("bounds oversized supervisor messages and rejects reserved recipients", () => {
		const s = scope("inbox-bounds");
		const big = recordSupervisorMessage({ scope: s, to: "b", message: "x".repeat(20_000), kind: "reply" });
		assert.ok(Buffer.byteLength(big.message, "utf-8") <= 16 * 1024);
		assert.match(big.message, /\[truncated to 16 KiB\]$/);
		assert.throws(() => recordSupervisorMessage({ scope: s, to: "supervisor", message: "x", kind: "steer" }), /reserved/);
	});
});

describe("idempotency namespacing", () => {
	it("keeps the same idempotency key independent across senders", () => {
		const s = scope("idem-ns");
		const fromA = sendPeerRequest({ scope: s, from: "a", to: "b", message: "from a", expectsReply: false, idempotencyKey: "ask-1" });
		const fromC = sendPeerRequest({ scope: s, from: "c", to: "b", message: "from c", expectsReply: false, idempotencyKey: "ask-1" });
		assert.notEqual(fromA.id, fromC.id);
		assert.equal(listPeerRequestsFor(s, "b").length, 2);
		// Each sender's retry still returns its own original.
		assert.equal(sendPeerRequest({ scope: s, from: "c", to: "b", message: "from c", expectsReply: false, idempotencyKey: "ask-1" }).id, fromC.id);
	});

	it("lets a sender resume its own earlier ask by passing that ask's id", () => {
		const s = scope("idem-resume");
		const original = sendPeerRequest({ scope: s, from: "a", to: "b", message: "resume me", expectsReply: true });
		const resumed = sendPeerRequest({ scope: s, from: "a", to: "b", message: "resume me", expectsReply: true, idempotencyKey: original.id });
		assert.equal(resumed.id, original.id);
		assert.equal(listPeerRequestsFor(s, "b").length, 1);
		// Another sender passing that id gets its own request, never a's.
		const other = sendPeerRequest({ scope: s, from: "c", to: "b", message: "mine", expectsReply: false, idempotencyKey: original.id });
		assert.notEqual(other.id, original.id);
	});

	it("bounds namespaced ids to the id length", () => {
		const s = scope("idem-long");
		const from = "a".repeat(100);
		const sent = sendPeerRequest({ scope: s, from, to: "b", message: "long", expectsReply: false, idempotencyKey: "k".repeat(100) });
		assert.ok(sent.id.length <= 128);
		assert.equal(sendPeerRequest({ scope: s, from, to: "b", message: "long", expectsReply: false, idempotencyKey: "k".repeat(100) }).id, sent.id);
	});
});

describe("idempotency strictness", () => {
	it("throws when a key is reused for a different ask", () => {
		const s = scope("idem-strict");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "one", expectsReply: false, idempotencyKey: "k-1" });
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "a", to: "c", message: "one", expectsReply: false, idempotencyKey: "k-1" }),
			/different ask/,
		);
		assert.throws(
			() => sendPeerRequest({ scope: s, from: "a", to: "b", message: "two", expectsReply: false, idempotencyKey: "k-1" }),
			/different ask/,
		);
	});
});

describe("reply atomicity", () => {
	it("loses a reply race as already-answered instead of clobbering", () => {
		const s = scope("race");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "q", expectsReply: true });
		// Simulate a won race: a reply file already exists.
		const dir = siblingRootDir();
		const sessionDirs = fs.readdirSync(dir).filter((d) => d.startsWith("s-"));
		let replyFile: string | null = null;
		outer: for (const sd of sessionDirs) {
			for (const wd of fs.readdirSync(path.join(dir, sd))) {
				const candidate = path.join(dir, sd, wd, "replies", `${ask.id}.json`);
				if (fs.existsSync(path.join(dir, sd, wd, "requests", `${ask.id}.json`))) {
					replyFile = candidate;
					break outer;
				}
			}
		}
		assert.ok(replyFile);
		fs.mkdirSync(path.dirname(replyFile), { recursive: true });
		fs.writeFileSync(replyFile, JSON.stringify({ version: 1, requestId: ask.id, session: s.session, workflow: s.workflow, from: "b", message: "winner", createdAt: Date.now() }));
		assert.throws(() => writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "loser" }), /already has a reply/);
	});
});

describe("mailbox hygiene", () => {
	it("excludes unparseable debris from the cap and reaps it by mtime", () => {
		const s = scope("debris");
		const inbox = path.join(siblingRootDir(), `s-${s.session}`, `w-${s.workflow}`, "requests");
		fs.mkdirSync(inbox, { recursive: true });
		for (let i = 0; i < 200; i++) fs.writeFileSync(path.join(inbox, `debris-${i}.json`), "not json{{{");
		// 200 debris files, zero live asks: a live send must still succeed (debris excluded from cap).
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "live", expectsReply: false });
		// Readers skip debris.
		assert.equal(listPeerRequestsFor(s, "b").length, 1);
		// GC reaps all 200 debris files by mtime.
		const removed = cleanupStaleSiblingScopes(Date.now() + SIBLING_MESSAGE_TTL_MS + 1000);
		assert.ok(removed >= 200);
		let remaining: string[] = [];
		try {
			remaining = fs.readdirSync(inbox).filter((f) => f.startsWith("debris-"));
		} catch {
			// Fully reaped scope: also zero debris.
		}
		assert.equal(remaining.length, 0);
	});
});

describe("cleanupStaleSiblingScopes", () => {
	it("reaps expired pairs and empty scopes, keeps live ones", () => {
		const s = scope("gc");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "old", expectsReply: true });
		writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "done" });
		const future = Date.now() + SIBLING_MESSAGE_TTL_MS + 1000;
		const removedPairs = cleanupStaleSiblingScopes(future);
		assert.ok(removedPairs >= 2);
		const farFuture = future + SIBLING_EMPTY_SCOPE_TTL_MS + 1000;
		cleanupStaleSiblingScopes(farFuture);
		const live = scope("gc-live");
		sendPeerRequest({ scope: live, from: "a", to: "b", message: "live", expectsReply: false });
		cleanupStaleSiblingScopes(Date.now());
		assert.equal(listPeerRequestsFor(live, "b").length, 1);
	});
});

describe("cleanupStaleSiblingScopes with relay receipts", () => {
	it("reaps an empty scope even after the relay created a forwarded dir", () => {
		const s = scope("forwarded");
		const req = sendPeerRequest({ scope: s, from: "a", to: "b", message: "hi", expectsReply: true });
		writePeerReply({ scope: s, requestId: req.id, from: "b", message: "yo" });
		const sessions = fs.readdirSync(siblingRootDir()).filter((d) => d.includes(s.session));
		assert.equal(sessions.length, 1);
		const scopeDir = path.join(siblingRootDir(), sessions[0]!, "w-wf-1");
		fs.mkdirSync(path.join(scopeDir, "forwarded"), { recursive: true });
		fs.writeFileSync(path.join(scopeDir, "forwarded", `${req.id}.json`), JSON.stringify({ version: 1, requestId: req.id, createdAt: req.createdAt }));
		const far = Date.now() + SIBLING_MESSAGE_TTL_MS + SIBLING_EMPTY_SCOPE_TTL_MS + 1000;
		cleanupStaleSiblingScopes(far);
		cleanupStaleSiblingScopes(far);
		assert.equal(fs.existsSync(scopeDir), false);
	});
});

describe("waitForPeerReply abortIf", () => {
	it("aborts the wait with the hook's message", async () => {
		const s = scope("abortif");
		const req = sendPeerRequest({ scope: s, from: "a", to: "b", message: "hi", expectsReply: true });
		let polls = 0;
		await assert.rejects(
			waitForPeerReply(s, req.id, 5000, undefined, () => (++polls >= 2 ? "yield now" : undefined)),
			/yield now/,
		);
	});
});
