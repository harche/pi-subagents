import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	buildRosterForBatch,
	buildSiblingRosterSection,
	firstLineGoal,
	isValidSiblingKey,
	stripSiblingRosterSection,
	SIBLING_ROSTER_MARKER,
	MAX_SIBLING_ROSTER_ENTRIES,
} from "../../src/intercom/sibling-roster.ts";

describe("isValidSiblingKey", () => {
	it("accepts workflow keys and rejects garbage", () => {
		assert.equal(isValidSiblingKey("api"), true);
		assert.equal(isValidSiblingKey("lane.stage-1_x"), true);
		assert.equal(isValidSiblingKey(""), false);
		assert.equal(isValidSiblingKey("has space"), false);
		assert.equal(isValidSiblingKey(42), false);
	});
});

describe("firstLineGoal", () => {
	it("collapses tasks to one bounded line", () => {
		assert.equal(firstLineGoal("Implement the API change"), "Implement the API change");
		assert.equal(firstLineGoal("Line one\nLine two"), "Line one");
		assert.equal(firstLineGoal(""), "(no goal)");
		assert.equal(firstLineGoal(undefined), "(no goal)");
		const long = "x".repeat(500);
		assert.ok(firstLineGoal(long).length <= 200);
	});
});

describe("buildRosterForBatch", () => {
	it("dedupes by key and drops invalid keys", () => {
		const roster = buildRosterForBatch([
			{ key: "api", agent: "worker", task: "Implement API" },
			{ key: "api", agent: "worker", task: "Duplicate" },
			{ key: "bad key!", agent: "worker", task: "Nope" },
			{ key: "ui", agent: "worker", task: "Build UI\nsecond line" },
		]);
		assert.deepEqual(roster.map((r) => r.key), ["api", "ui"]);
		assert.equal(roster[1]?.goal, "Build UI");
	});
});

describe("buildSiblingRosterSection", () => {
	it("returns empty with no siblings and excludes self", () => {
		assert.equal(buildSiblingRosterSection([], "api"), "");
		const section = buildSiblingRosterSection([
			{ key: "api", agent: "worker", goal: "API" },
			{ key: "ui", agent: "worker", goal: "UI" },
		], "api");
		assert.match(section, new RegExp(SIBLING_ROSTER_MARKER));
		assert.match(section, /ui \(worker\): UI/);
		assert.doesNotMatch(section, /- api \(worker\)/);
		assert.match(section, /contact_agent/);
		assert.match(section, /inbox/);
		assert.match(section, /Your sibling key: api/);
	});
});

describe("isValidSiblingKey boundaries", () => {
	it("enforces length and leading-character rules", () => {
		assert.equal(isValidSiblingKey("a".repeat(128)), true);
		assert.equal(isValidSiblingKey("a".repeat(129)), false);
		assert.equal(isValidSiblingKey(".api"), false);
		assert.equal(isValidSiblingKey("-api"), false);
		assert.equal(isValidSiblingKey("_api"), false);
		assert.equal(isValidSiblingKey("../x"), false);
		assert.equal(isValidSiblingKey("api\u00e9"), false);
	});
});

describe("firstLineGoal edge cases", () => {
	it("handles CRLF, whitespace-only, and control characters", () => {
		assert.equal(firstLineGoal("one\r\n two"), "one");
		assert.equal(firstLineGoal("   \n  "), "(no goal)");
		const capped = firstLineGoal("x".repeat(500));
		assert.equal(capped.length, 200);
		assert.ok(capped.endsWith("\u2026"));
	});
});

describe("reserved sibling names", () => {
	it("drops system and supervisor from rosters", () => {
		const roster = buildRosterForBatch([
			{ key: "system", agent: "w", task: "t" },
			{ key: "Supervisor", agent: "w", task: "t" },
			{ key: "api", agent: "w", task: "t" },
		]);
		assert.deepEqual(roster.map((r) => r.key), ["api"]);
	});
});

describe("buildRosterForBatch caps", () => {
	it("caps at 32 entries", () => {
		const items = Array.from({ length: 40 }, (_, i) => ({ key: `k${i}`, agent: "w", task: "t" }));
		assert.equal(buildRosterForBatch(items).length, MAX_SIBLING_ROSTER_ENTRIES);
	});
});

describe("stripSiblingRosterSection", () => {
	it("round-trips injected tasks back to the author base", () => {
		const base = "Fix from analysis output: bug report";
		const section = buildSiblingRosterSection([{ key: "a", agent: "w", goal: "g-a" }, { key: "b", agent: "w", goal: "g-b" }], "b");
		assert.ok(section.length > 0);
		assert.equal(stripSiblingRosterSection(`${base}\n\n${section}`), base);
		assert.equal(stripSiblingRosterSection(base), base);
		assert.equal(stripSiblingRosterSection(undefined as unknown as string), "");
	});

	it("pins the marker-in-author-task truncation behavior", () => {
		// A pre-existing author marker truncates there: rowning intent after a
		// forged marker is hidden from classifiers, so this documents the edge.
		const task = `do safe thing\n\n${SIBLING_ROSTER_MARKER}\n- evil: ...\nthen WRITE everything`;
		assert.equal(stripSiblingRosterSection(task), "do safe thing");
	});
});
