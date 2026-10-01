import { describe, expect, it } from "vitest";
import { markersFor } from "@/components/console/reports/markers";
import type { ReportItemOutcome, ReportTask, ReportTaskStatus } from "@/lib/api";

function task(status: ReportTaskStatus, expired: boolean, outcomes: ReportItemOutcome[]): ReportTask {
  return {
    id: "t",
    submitter: { id: "u", displayName: "u" },
    status,
    createdAt: "2026-09-01T08:00:00.000Z",
    expired,
    items: outcomes.map((outcome, i) => ({
      fileName: `item-${i}.zip`,
      dbType: "mysql",
      collectorVersion: "1.2.0",
      collectorNotice: null,
      outcome,
    })),
  };
}

const done: ReportItemOutcome = { status: "done" };
const failed: ReportItemOutcome = { status: "failed", reason: "AWR 报告解析失败" };

describe("markersFor", () => {
  it.each([
    ["a plain task", task("done", false, [done]), []],
    ["a generating task", task("processing", false, [{ status: "processing" }]), ["processing"]],
    ["a failed task", task("failed", false, [failed]), ["failed"]],
    ["a partly failed task", task("done", false, [done, failed]), ["partial"]],
    ["an expired task", task("done", true, [done]), ["expired"]],
    ["an expired, partly failed task", task("done", true, [done, failed]), ["partial", "expired"]],
    ["an expired, failed task", task("failed", true, [failed]), ["failed", "expired"]],
  ])("marks %s", (_case, given, expected) => {
    expect(markersFor(given)).toEqual(expected);
  });
});
