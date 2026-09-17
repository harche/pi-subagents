/**
 * Parent-side recording of supervisor steers into a child's inbox.
 *
 * A steer is pushed as user input; that push is the wake-up. This files the
 * durable copy so the child can consult what the supervisor said even when
 * the push landed between turns or after its last tool call.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { SubagentState } from "../shared/types.ts";
import { recordSupervisorMessage } from "./sibling-channels.ts";

export interface SupervisorSteerTarget {
	/** Workflow run id and key, when the caller already knows them. */
	workflowRunId?: string;
	key?: string;
	/** Child run directory; its status carries the workflow linkage when the caller does not. */
	asyncDir?: string | null;
}

function readLinkage(asyncDir: string | null | undefined): { workflowRunId?: string; key?: string } {
	if (!asyncDir) return {};
	try {
		const status = JSON.parse(fs.readFileSync(path.join(asyncDir, "status.json"), "utf-8")) as { parentWorkflowRunId?: unknown; workflowKey?: unknown };
		return {
			...(typeof status.parentWorkflowRunId === "string" ? { workflowRunId: status.parentWorkflowRunId } : {}),
			...(typeof status.workflowKey === "string" ? { key: status.workflowKey } : {}),
		};
	} catch {
		return {};
	}
}

/**
 * Best-effort: never throws, never records when the steer did not reach the
 * child or the target has no sibling scope (a direct, non-workflow child).
 */
export function recordSupervisorSteer(input: {
	state: SubagentState;
	target: SupervisorSteerTarget;
	message: string;
	outcome: { state?: string; isError?: boolean; deliveryStatus?: string };
}): boolean {
	try {
		if (input.outcome.isError === true) return false;
		if (input.outcome.state === "failed" || input.outcome.state === "missed" || input.outcome.state === "partial") return false;
		const linkage = readLinkage(input.target.asyncDir);
		const workflowRunId = input.target.workflowRunId ?? linkage.workflowRunId;
		const key = input.target.key ?? linkage.key;
		const session = input.state.supervisorOwnerSessionId ?? input.state.currentSessionId ?? undefined;
		if (!workflowRunId || !key || !session) return false;
		recordSupervisorMessage({
			scope: { session, workflow: workflowRunId },
			to: key,
			message: input.message,
			kind: "steer",
			delivery: input.outcome.deliveryStatus ?? input.outcome.state,
		});
		return true;
	} catch {
		return false;
	}
}
