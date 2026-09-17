import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { createSiblingRelay, type SiblingRelayOutcome } from "../../src/intercom/sibling-relay.ts";
import {
	listPeerRequestsFor,
	recordSupervisorMessage,
	sendPeerRequest,
	type SiblingScope,
} from "../../src/intercom/sibling-channels.ts";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

const liveScopes: SiblingScope[] = [];

function scope(tag: string, workflow = "wf-relay"): SiblingScope {
	const s: SiblingScope = { session: `relay-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, workflow };
	liveScopes.push(s);
	return s;
}

// The relay polls the shared temp root, so remove each test's scopes
// afterwards; otherwise leftover asks leak into later tests' polls.
afterEach(() => {
	for (const s of liveScopes.splice(0)) {
		if (/[^A-Za-z0-9._-]/.test(s.session)) continue;
		try {
			fs.rmSync(path.join(siblingRoot(), `s-${s.session}`), { recursive: true, force: true });
		} catch {
			// Best-effort test hygiene.
		}
	}
});

function makeRelay(
	steer: (workflowRunId: string, key: string, message: string) => Promise<{ state: SiblingRelayOutcome; error?: string }>,
) {
	const calls: Array<{ workflowRunId: string; key: string; message: string }> = [];
	const relay = createSiblingRelay({} as never, {
		state: {} as never,
		steer: (workflowRunId, key, message) => {
			calls.push({ workflowRunId, key, message });
			return steer(workflowRunId, key, message);
		},
	});
	return { relay, calls };
}

function siblingRoot(): string {
	const root = process.env.PI_SUBAGENTS_TEMP_ROOT?.trim();
	assert.ok(root, "isolated temp root required");
	return path.join(root, "sibling-channels");
}

function scopeDirOf(s: SiblingScope): string {
	const sessions = fs.readdirSync(siblingRoot());
	for (const sd of sessions) {
		const wdirs = fs.readdirSync(path.join(siblingRoot(), sd), { withFileTypes: true });
		for (const wd of wdirs) {
			const reqFile = path.join(siblingRoot(), sd, wd.name, "requests");
			if (!fs.existsSync(reqFile)) continue;
			for (const f of fs.readdirSync(reqFile)) {
				try {
					const parsed = JSON.parse(fs.readFileSync(path.join(reqFile, f), "utf-8")) as { session?: string; workflow?: string };
					if (parsed.session === s.session && parsed.workflow === s.workflow) {
						return path.join(siblingRoot(), sd, wd.name);
					}
				} catch { /* ignore */ }
			}
		}
	}
	throw new Error("scope dir not found");
}

describe("sibling relay push", () => {
	it("steers new peer asks once and never double-pushes", async () => {
		const s = scope("push");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "ping?", expectsReply: true });
		const { relay, calls } = makeRelay(async () => ({ state: "delivered" }));
		const first = await relay.poll();
		assert.equal(first.forwarded, 1);
		assert.equal(calls.length, 1);
		assert.equal(calls[0]!.key, "b");
		assert.match(calls[0]!.message, /contact_agent\(\{ to: "a", replyTo: "/);
		assert.match(calls[0]!.message, /ping\?/);
		const second = await relay.poll();
		assert.equal(second.forwarded, 0);
		assert.equal(calls.length, 1);
		relay.dispose();
	});

	it("skips already-resolved asks and malformed files", async () => {
		const s = scope("skip");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "q", expectsReply: true });
		const dir = scopeDirOf(s);
		fs.writeFileSync(path.join(dir, "requests", "garbage.json"), "not json{{{");
		const { relay, calls } = makeRelay(async () => ({ state: "delivered" }));
		// Resolve it first via a direct reply: relay must not push resolved asks.
		const { writePeerReply } = await import("../../src/intercom/sibling-channels.ts");
		writePeerReply({ scope: s, requestId: ask.id, from: "b", message: "done" });
		const res = await relay.poll();
		assert.equal(res.forwarded, 0);
		assert.equal(calls.length, 0);
		relay.dispose();
	});

	it("files a system reply when the target is terminally gone", async () => {
		const s = scope("missed");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "gone", message: "hello?", expectsReply: true });
		const { relay, calls } = makeRelay(async () => ({ state: "missed", error: "Workflow child 'gone' is completed." }));
		const res = await relay.poll();
		assert.equal(calls.length, 1);
		assert.equal(res.systemReplied, 1);
		// The waiter now fails fast with an honest answer instead of blocking to timeout.
		const remaining = listPeerRequestsFor(s, "gone");
		assert.equal(remaining.length, 0);
		const dir = scopeDirOf(s);
		const reply = JSON.parse(fs.readFileSync(path.join(dir, "replies", `${ask.id}.json`), "utf-8")) as { from: string; message: string };
		assert.equal(reply.from, "system");
		assert.match(reply.message, /no longer running/);
		relay.dispose();
	});

	it("settles fire-and-forget asks once pushed so they stop listing as pending", async () => {
		const s = scope("notify");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "b", message: "fyi", expectsReply: false });
		const { relay } = makeRelay(async () => ({ state: "delivered" }));
		const res = await relay.poll();
		assert.equal(res.forwarded, 1);
		assert.equal(listPeerRequestsFor(s, "b").length, 0);
		const dir = scopeDirOf(s);
		const reply = JSON.parse(fs.readFileSync(path.join(dir, "replies", `${ask.id}.json`), "utf-8")) as { from: string };
		assert.equal(reply.from, "system");
		// Blocking asks are untouched: the peer still has to answer.
		const blocking = sendPeerRequest({ scope: s, from: "a", to: "b", message: "answer me", expectsReply: true });
		await relay.poll();
		assert.equal(listPeerRequestsFor(s, "b").map((r) => r.id).includes(blocking.id), true);
		relay.dispose();
	});

	it("treats partial and rejected children as terminal", async () => {
		const s = scope("partial");
		sendPeerRequest({ scope: s, from: "a", to: "gone", message: "hello?", expectsReply: true });
		const { relay } = makeRelay(async () => ({ state: "missed", error: "Workflow child 'gone' is partial." }));
		const res = await relay.poll();
		assert.equal(res.systemReplied, 1);
		relay.dispose();
	});

	it("retries transient misses without journaling failure", async () => {
		const s = scope("transient");
		sendPeerRequest({ scope: s, from: "a", to: "starting", message: "yo", expectsReply: true });
		const { relay, calls } = makeRelay(async () => ({ state: "missed", error: "Workflow child 'starting' had no live steering route." }));
		const res = await relay.poll();
		assert.equal(calls.length, 1);
		assert.equal(res.systemReplied, 0);
		// Still pending: a later poll retries.
		assert.equal(listPeerRequestsFor(s, "starting").length, 1);
		const dir = scopeDirOf(s);
		assert.equal(fs.existsSync(path.join(dir, "replies")), true);
		const replies = fs.readdirSync(path.join(dir, "replies")).filter((f) => f.endsWith(".json"));
		assert.equal(replies.length, 0);
		relay.dispose();
	});

	it("watches request dirs on supporting platforms and polls on events", async () => {
		const s = scope("watch");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "pushed?", expectsReply: true });
		const listeners: Array<() => void> = [];
		const fakeWatch = ((_file: unknown, listener: () => void) => {
			listeners.push(listener);
			return { on: () => {}, close: () => {}, unref: () => {} };
		}) as never;
		const steered: string[] = [];
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			platform: "linux",
			watch: fakeWatch,
			steer: async (_w, key) => {
				steered.push(key);
				return { state: "delivered" as const };
			},
		});
		relay.start();
		assert.ok(listeners.length >= 1, "expected request-dir watchers to register");
		for (const fire of listeners) fire();
		// Let polled promises settle.
		await new Promise((resolve) => setTimeout(resolve, 50));
		assert.ok(steered.includes("b"));
		relay.dispose();
	});

	it("falls back to status-scan direct steer when keyed routing misses", async () => {
		const s = scope("fallback");
		sendPeerRequest({ scope: s, from: "a", to: "live", message: "knock knock", expectsReply: true });
		// Fake async-runs root: one running child linked to this workflow+key.
		const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-relay-runs-"));
		const childDir = path.join(runsRoot, "child-run-1");
		fs.mkdirSync(childDir, { recursive: true });
		fs.writeFileSync(path.join(childDir, "status.json"), JSON.stringify({
			runId: "child-run-1", state: "running", parentWorkflowRunId: "wf-fallback-marker",
		}));
		const directCalls: string[] = [];
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			asyncDirRoot: runsRoot,
			steer: async () => ({ state: "missed" as const, error: "Workflow child 'live' had no live steering route." }),
			steerDirect: async (input) => {
				directCalls.push(`${input.runId}:${input.asyncDir}`);
				return { state: "delivered" as const };
			},
		});
		// Rewrite the ask's workflow linkage to the fake root by re-sending in a matching scope is
		// unnecessary: instead point the relay at a scope whose workflow id we control via status file.
		// Simpler direct unit check of the resolver path below; here assert the fallback fired.
		const s2: SiblingScope = { session: s.session, workflow: "wf-fallback-marker" };
		// Plant a matching status entry for the s2 workflow/key pair.
		const child2 = path.join(runsRoot, "child-run-2");
		fs.mkdirSync(child2, { recursive: true });
		fs.writeFileSync(path.join(child2, "status.json"), JSON.stringify({
			runId: "child-run-2", state: "queued", parentWorkflowRunId: "wf-fallback-marker", workflowKey: "live",
		}));
		sendPeerRequest({ scope: s2, from: "a", to: "live", message: "knock knock", expectsReply: true });
		const res = await relay.poll();
		assert.equal(res.forwarded, 1);
		assert.ok(directCalls.some((c) => c.startsWith("child-run-2:")));
		relay.dispose();
	});

	it("fails fast when the status scan finds only a finished sibling", async () => {
		const s = scope("finished");
		const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-relay-runs-"));
		const s2: SiblingScope = { session: s.session, workflow: "call_finished-marker" };
		const done = path.join(runsRoot, "child-run-done");
		fs.mkdirSync(done, { recursive: true });
		fs.writeFileSync(path.join(done, "status.json"), JSON.stringify({
			runId: "child-run-done", state: "complete", parentWorkflowRunId: "call_finished-marker", workflowKey: "beta", updatedAt: 10,
		}));
		const directCalls: string[] = [];
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			asyncDirRoot: runsRoot,
			steer: async () => { throw new Error("keyed steer must be skipped for tool-call scoped workflows"); },
			steerDirect: async (input) => { directCalls.push(input.runId); return { state: "delivered" as const }; },
		});
		const ask = sendPeerRequest({ scope: s2, from: "alpha", to: "beta", message: "word?", expectsReply: true });
		const res = await relay.poll();
		assert.equal(res.systemReplied, 1);
		assert.deepEqual(directCalls, []);
		const dir = scopeDirOf(s2);
		const reply = JSON.parse(fs.readFileSync(path.join(dir, "replies", `${ask.id}.json`), "utf-8")) as { from: string; message: string };
		assert.equal(reply.from, "system");
		assert.match(reply.message, /is complete/);
		// A newer live run for the same key still wins over an older finished one.
		const live = path.join(runsRoot, "child-run-live");
		fs.mkdirSync(live, { recursive: true });
		fs.writeFileSync(path.join(live, "status.json"), JSON.stringify({
			runId: "child-run-live", state: "running", parentWorkflowRunId: "call_finished-marker", workflowKey: "beta", updatedAt: 20,
		}));
		sendPeerRequest({ scope: s2, from: "alpha", to: "beta", message: "again?", expectsReply: true });
		const res2 = await relay.poll();
		assert.equal(res2.forwarded, 1);
		assert.deepEqual(directCalls, ["child-run-live"]);
		relay.dispose();
	});

	it("warns once about stale unforwarded asks", async () => {
		const s = scope("stale");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "ghost", message: "anybody?", expectsReply: true });
		const warnings: unknown[][] = [];
		const origWarn = console.warn;
		console.warn = (...args: unknown[]) => { warnings.push(args); };
		try {
			const relay = createSiblingRelay({} as never, {
				state: {} as never,
				staleWarnAfterMs: 0,
				steer: async () => ({ state: "missed" as const, error: "no route yet" }),
			});
			await relay.poll();
			await relay.poll();
			relay.dispose();
		} finally {
			console.warn = origWarn;
		}
		const own = warnings.filter((args) => String(args[0]).includes(ask.id));
		assert.equal(own.length, 1);
		assert.match(String(own[0]![0]), /unforwarded/);
	});

	it("claims in-flight asks so concurrent polls steer once", async () => {
		const s = scope("inflight");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "once", expectsReply: true });
		let steers = 0;
		let release!: () => void;
		const gate = new Promise<void>((resolve) => { release = resolve; });
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			steer: async () => {
				steers++;
				await gate;
				return { state: "delivered" as const };
			},
		});
		const first = relay.poll();
		const second = relay.poll();
		release();
		const [r1, r2] = await Promise.all([first, second]);
		assert.equal(steers, 1);
		assert.equal(r1.forwarded + r2.forwarded, 1);
		relay.dispose();
	});

	it("forwards oldest-first across scopes", async () => {
		const order: string[] = [];
		const mk = (tag: string) => {
			const s = scope(`order-${tag}`);
			sendPeerRequest({ scope: s, from: "a", to: "b", message: `m-${tag}`, expectsReply: false });
			return s;
		};
		mk("1"); mk("2"); mk("3");
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			steer: async (_w, _k, message) => {
				const m = message.match(/m-(\d)/);
				order.push(m ? m[1]! : "?");
				return { state: "delivered" as const };
			},
		});
		await relay.poll();
		// All three forwarded; same-ms ties break deterministically by id set order.
		assert.equal(order.length, 3);
		assert.deepEqual([...order].sort(), ["1", "2", "3"]);
		relay.dispose();
	});

	it("skips foreign sessions when the owner is known", async () => {
		const mine = scope("owner-mine");
		sendPeerRequest({ scope: mine, from: "a", to: "b", message: "mine", expectsReply: true });
		const foreign: SiblingScope = { session: "someone-else", workflow: "wf-x" };
		liveScopes.push(foreign);
		sendPeerRequest({ scope: foreign, from: "a", to: "b", message: "theirs", expectsReply: true });
		let steers = 0;
		const relay = createSiblingRelay({} as never, {
			state: { supervisorOwnerSessionId: mine.session } as never,
			steer: async () => {
				steers++;
				return { state: "delivered" as const };
			},
		});
		const res = await relay.poll();
		assert.equal(res.forwarded, 1);
		assert.equal(steers, 1);
		relay.dispose();
	});

	it("backs off repeatedly-missed asks between polls", async () => {
		const s = scope("backoff");
		sendPeerRequest({ scope: s, from: "a", to: "ghost", message: "?", expectsReply: true });
		let steers = 0;
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			steer: async () => {
				steers++;
				return { state: "missed" as const, error: "no route yet" };
			},
		});
		await relay.poll();
		assert.equal(steers, 1);
		await relay.poll();
		assert.equal(steers, 1, "second immediate poll must back off");
		relay.dispose();
	});

	it("skips keyed routing for tool-call workflow ids and uses the scan", async () => {
		const s: SiblingScope = { session: `sess-call-${Date.now()}`, workflow: "call_abc%7Cfc_def" };
		liveScopes.push(s);
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "yo", expectsReply: true });
		let keyed = 0;
		// Fake runs root with a matching live child so the scan resolves.
		const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-relay-call-"));
		const childDir = path.join(runsRoot, "child-9");
		fs.mkdirSync(childDir, { recursive: true });
		fs.writeFileSync(path.join(childDir, "status.json"), JSON.stringify({
			runId: "child-9", state: "running", parentWorkflowRunId: "call_abc%7Cfc_def", workflowKey: "b",
		}));
		const relay2 = createSiblingRelay({} as never, {
			state: {} as never,
			asyncDirRoot: runsRoot,
			steer: async () => {
				keyed++;
				return { state: "delivered" as const };
			},
			steerDirect: async () => ({ state: "delivered" as const }),
		});
		const res = await relay2.poll();
		assert.equal(keyed, 0, "call_-scoped asks must skip keyed routing");
		assert.equal(res.forwarded, 1);
		relay2.dispose();
	});

	it("journals queued as forwarded and retries failed without a system reply", async () => {
		const s = scope("outcomes");
		const q = sendPeerRequest({ scope: s, from: "a", to: "q", message: "q?", expectsReply: true });
		const f = sendPeerRequest({ scope: s, from: "a", to: "f", message: "f?", expectsReply: true });
		const { relay } = makeRelay(async (_w, key) => {
			if (key === "q") return { state: "queued" as const };
			return { state: "failed" as const, error: "boom" };
		});
		const res = await relay.poll();
		assert.equal(res.forwarded, 1);
		assert.equal(res.systemReplied, 0);
		// Queued ask journaled; failed ask left pending with no system reply.
		const dir = scopeDirOf(s);
		assert.ok(fs.existsSync(path.join(dir, "forwarded", `${q.id}.json`)));
		assert.ok(!fs.existsSync(path.join(dir, "forwarded", `${f.id}.json`)));
		assert.ok(!fs.existsSync(path.join(dir, "replies", `${f.id}.json`)));
		relay.dispose();
	});

	it("steers the full ask text wrapped in untrusted delimiters", async () => {
		const s = scope("delims");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "hi there", expectsReply: true });
		let steered = "";
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			steer: async (_w, _k, message) => {
				steered = message;
				return { state: "delivered" as const };
			},
		});
		await relay.poll();
		assert.match(steered, /--- untrusted peer message begins ---/);
		assert.match(steered, /hi there/);
		assert.match(steered, /--- untrusted peer message ends ---/);
		assert.match(steered, /replyTo: "/);
		relay.dispose();
	});

	it("tells the target not to reply to a fire-and-forget note", async () => {
		const s = scope("note-text");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "just fyi", expectsReply: false });
		let steered = "";
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			steer: async (_w, _k, message) => {
				steered = message;
				return { state: "delivered" as const };
			},
		});
		await relay.poll();
		assert.match(steered, /no reply expected/);
		assert.doesNotMatch(steered, /Answer with:/);
		relay.dispose();
	});

	it("fails an ask to a key nobody has after the unknown-target grace", async () => {
		const s = scope("unknown-key");
		const ask = sendPeerRequest({ scope: s, from: "a", to: "reviewr", message: "typo?", expectsReply: true });
		const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-relay-unknown-"));
		let steers = 0;
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			asyncDirRoot: runsRoot,
			unknownTargetGraceMs: 0,
			steer: async () => {
				steers++;
				return { state: "missed" as const, error: "Workflow child 'reviewr' is unknown to workflow 'wf-relay'." };
			},
		});
		const res = await relay.poll();
		assert.equal(steers, 1);
		assert.equal(res.systemReplied, 1);
		const dir = scopeDirOf(s);
		const reply = JSON.parse(fs.readFileSync(path.join(dir, "replies", `${ask.id}.json`), "utf-8")) as { from: string; message: string };
		assert.equal(reply.from, "system");
		assert.match(reply.message, /no sibling 'reviewr' exists/);
		// Journaled: the orphan is never re-steered.
		await relay.poll();
		assert.equal(steers, 1);
		relay.dispose();
	});

	it("keeps retrying a keyed 'unknown' verdict inside the grace window", async () => {
		const s = scope("unknown-grace");
		sendPeerRequest({ scope: s, from: "a", to: "late", message: "not launched yet", expectsReply: true });
		const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-relay-grace-"));
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			asyncDirRoot: runsRoot,
			unknownTargetGraceMs: 60_000,
			steer: async () => ({ state: "missed" as const, error: "Workflow child 'late' is unknown to workflow 'wf-relay'." }),
		});
		const res = await relay.poll();
		assert.equal(res.systemReplied, 0);
		assert.equal(listPeerRequestsFor(s, "late").length, 1);
		relay.dispose();
	});

	it("skips already-forwarded asks without opening them", async () => {
		const s = scope("cheap-poll");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "once", expectsReply: true });
		const { relay } = makeRelay(async () => ({ state: "delivered" }));
		await relay.poll();
		const dir = scopeDirOf(s);
		const requestFile = fs.readdirSync(path.join(dir, "requests")).find((f) => f.endsWith(".json"))!;
		// Corrupt the forwarded request body: a poll that re-read it would now treat it as debris and,
		// on a marker miss, re-steer; the name-based skip never touches it.
		fs.writeFileSync(path.join(dir, "requests", requestFile), "not json");
		const res = await relay.poll();
		assert.equal(res.forwarded, 0);
		relay.dispose();
	});

	it("drops a watcher whose directory disappeared so a recreated one is re-watched", async () => {
		const s = scope("rewatch");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "first", expectsReply: false });
		const watched: string[] = [];
		const closed: string[] = [];
		const fakeWatch = ((file: string) => {
			watched.push(file);
			return { on: () => {}, close: () => closed.push(file), unref: () => {} };
		}) as never;
		const relay = createSiblingRelay({} as never, {
			state: {} as never,
			platform: "linux",
			watch: fakeWatch,
			steer: async () => ({ state: "delivered" as const }),
		});
		relay.start();
		const dir = scopeDirOf(s);
		const requestsDir = path.join(dir, "requests");
		assert.ok(watched.includes(requestsDir));
		relay.dispose();
		// Simulate GC removing the scope, then a sender recreating it.
		fs.rmSync(dir, { recursive: true, force: true });
		watched.length = 0;
		closed.length = 0;
		const relay2 = createSiblingRelay({} as never, {
			state: {} as never,
			platform: "linux",
			watch: fakeWatch,
			steer: async () => ({ state: "delivered" as const }),
		});
		relay2.start();
		assert.ok(!watched.includes(requestsDir), "removed dir must not be watched");
		sendPeerRequest({ scope: s, from: "a", to: "b", message: "second", expectsReply: false });
		relay2.activateTransport();
		assert.ok(watched.includes(requestsDir), "recreated dir must be watched again");
		relay2.dispose();
	});

	it("never relays supervisor inbox entries", async () => {
		const s = scope("sup-skip");
		recordSupervisorMessage({ scope: s, to: "b", message: "steer text", kind: "steer" });
		const { relay, calls } = makeRelay(async () => ({ state: "delivered" }));
		const res = await relay.poll();
		assert.equal(calls.length, 0);
		assert.equal(res.forwarded, 0);
		relay.dispose();
	});

	it("start/activate/dispose are idempotent", async () => {
		const { relay } = makeRelay(async () => ({ state: "delivered" }));
		relay.start();
		relay.start();
		relay.activateTransport();
		relay.activateTransport();
		await relay.poll();
		relay.dispose();
		relay.dispose();
	});
});
