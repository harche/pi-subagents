import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { recordSupervisorSteer } from "../../src/intercom/supervisor-inbox.ts";
import { listInbox, siblingRootDir } from "../../src/intercom/sibling-channels.ts";
import type { SubagentState } from "../../src/shared/types.ts";

function stateWith(session: string | null): SubagentState {
	return { currentSessionId: session, supervisorOwnerSessionId: null } as unknown as SubagentState;
}

describe("recordSupervisorSteer", () => {
	it("files a delivered steer into the keyed child's inbox", () => {
		const session = `sup-inbox-${Date.now()}`;
		const ok = recordSupervisorSteer({
			state: stateWith(session),
			target: { workflowRunId: "wf-1", key: "writer" },
			message: "Also update the docs.",
			outcome: { state: "delivered", deliveryStatus: "delivered" },
		});
		assert.equal(ok, true);
		const entries = listInbox({ session, workflow: "wf-1" }, "writer");
		assert.equal(entries.length, 1);
		assert.equal(entries[0]!.kind, "supervisor");
		assert.equal(entries[0]!.message, "Also update the docs.");
		fs.rmSync(path.join(siblingRootDir(), `s-${session}`), { recursive: true, force: true });
	});

	it("resolves the workflow linkage from the child's status file", () => {
		const session = `sup-inbox-link-${Date.now()}`;
		const asyncDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sup-inbox-"));
		fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: "child-1", state: "running", parentWorkflowRunId: "wf-2", workflowKey: "reviewer" }));
		assert.equal(recordSupervisorSteer({ state: stateWith(session), target: { asyncDir }, message: "Be brief.", outcome: { state: "pending", deliveryStatus: "queued" } }), true);
		assert.equal(listInbox({ session, workflow: "wf-2" }, "reviewer")[0]?.message, "Be brief.");
		fs.rmSync(path.join(siblingRootDir(), `s-${session}`), { recursive: true, force: true });
	});

	it("records nothing for failed steers, non-workflow children, or without a session", () => {
		const session = `sup-inbox-skip-${Date.now()}`;
		assert.equal(recordSupervisorSteer({ state: stateWith(session), target: { workflowRunId: "wf-3", key: "k" }, message: "x", outcome: { state: "failed" } }), false);
		assert.equal(recordSupervisorSteer({ state: stateWith(session), target: { workflowRunId: "wf-3", key: "k" }, message: "x", outcome: { state: "delivered", isError: true } }), false);
		assert.equal(recordSupervisorSteer({ state: stateWith(session), target: { asyncDir: "/nonexistent" }, message: "x", outcome: { state: "delivered" } }), false);
		assert.equal(recordSupervisorSteer({ state: stateWith(null), target: { workflowRunId: "wf-3", key: "k" }, message: "x", outcome: { state: "delivered" } }), false);
		assert.equal(listInbox({ session, workflow: "wf-3" }, "k").length, 0);
	});
});
