import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { isBlockingSupervisorTool } from "../../src/runs/background/subagent-runner.ts";

describe("isBlockingSupervisorTool", () => {
	it("flags supervisor-directed blocking asks under both tool names", () => {
		assert.equal(isBlockingSupervisorTool("contact_agent", { message: "q" }), true);
		assert.equal(isBlockingSupervisorTool("contact_agent", { to: "supervisor", reason: "need_decision", message: "q" }), true);
		assert.equal(isBlockingSupervisorTool("contact_agent", { to: " Supervisor ", reason: "interview_request" }), true);
		assert.equal(isBlockingSupervisorTool("contact_supervisor", { reason: "need_decision", message: "q" }), true);
		assert.equal(isBlockingSupervisorTool("contact_agent", { to: "supervisor", reason: "progress_update", message: "u" }), false);
	});

	it("never treats a sibling-directed ask as supervisor attention, even when it blocks", () => {
		// The child waits on a peer's replyTo answer, not on a supervisor request:
		// flipping to needs_attention would misdirect the supervisor and cut waits short.
		assert.equal(isBlockingSupervisorTool("contact_agent", { to: "reviewer", awaitReply: true, message: "q" }), false);
		assert.equal(isBlockingSupervisorTool("contact_agent", { to: "reviewer", message: "fyi" }), false);
		// Answering a peer ask with `to` omitted must not be mistaken for a supervisor ask.
		assert.equal(isBlockingSupervisorTool("contact_agent", { replyTo: "ask-1", message: "answer" }), false);
	});

	it("keeps the intercom ask predicate", () => {
		assert.equal(isBlockingSupervisorTool("intercom", { action: "ask" }), true);
		assert.equal(isBlockingSupervisorTool("intercom", { action: "send" }), false);
		assert.equal(isBlockingSupervisorTool("read", { path: "x" }), false);
	});
});
