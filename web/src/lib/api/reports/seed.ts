import type { ReportItem, ReportTaskStatus } from "@/lib/api/reports/contract";
import { seedFixture, seedTime } from "@/lib/api/seed-fixture";

/** A stored item; its `collectorNotice` is joined from the releases on every read. */
export type MockReportItem = Omit<ReportItem, "collectorNotice">;

export interface MockReportTask {
  id: string;
  submitterId: string;
  /** One per report item, in submission order. */
  items: MockReportItem[];
  status: ReportTaskStatus;
  createdAt: string;
  /**
   * When a processing task finishes by itself, watched or not. Absent on a
   * task that never finishes by itself, such as seed task 003 (生成中).
   */
  finishesAt?: string;
}

/**
 * The fixture's report tasks. Task 003 stays generating (生成中), 004 is
 * partly failed (部分失败), 005 is failed (失败), 008 and 006 are expired
 * (已过期), and 010 holds an item from the revoked release 1.0.0, so My
 * reports (我的报告) shows every marker and the revoked warning.
 */
export function seedReportTasks(now: number): MockReportTask[] {
  return seedFixture.reportTasks.map((task) => ({ ...task, createdAt: seedTime(task.createdAt, now) })) as MockReportTask[];
}
