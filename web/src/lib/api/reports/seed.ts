import type { ReportItem, ReportTaskStatus } from "@/lib/api/reports/contract";
import { DAY_MS, MINUTE_MS } from "@/lib/time";
import type { DbType } from "@/lib/types";

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


function done(fileName: string, dbType: DbType, collectorVersion: string | null = "1.2.0"): MockReportItem {
  return { fileName, dbType, collectorVersion, outcome: { status: "done" } };
}

function failed(fileName: string, dbType: DbType, reason: string): MockReportItem {
  return { fileName, dbType, collectorVersion: "1.2.0", outcome: { status: "failed", reason } };
}

/**
 * Dates are relative to `now`, so each 我的报告 marker (生成中, 部分失败, 失败,
 * 已过期) keeps showing however long after the seed is read. Task 010 holds an
 * item from the revoked release 1.0.0, so 我的报告 shows the revoked warning.
 */
export function seedReportTasks(now: number): MockReportTask[] {
  const ago = (ms: number) => new Date(now - ms).toISOString();
  return [
    {
      id: "task-seed-003",
      submitterId: "u-user-001",
      items: [{ fileName: "gauss-billing-01.zip", dbType: "gaussdb", collectorVersion: "1.2.0", outcome: { status: "processing" } }],
      status: "processing",
      createdAt: ago(3 * MINUTE_MS),
    },
    {
      id: "task-seed-007",
      submitterId: "u-admin-001",
      items: [done("gauss-core-01.zip", "gaussdb")],
      status: "done",
      createdAt: ago(1 * DAY_MS),
    },
    {
      id: "task-seed-001",
      submitterId: "u-user-001",
      items: [done("mysql-prod-01.zip", "mysql"), done("mysql-prod-02.zip", "mysql")],
      status: "done",
      createdAt: ago(2 * DAY_MS),
    },
    {
      id: "task-seed-004",
      submitterId: "u-user-001",
      items: [
        done("oracle-erp-01.zip", "oracle"),
        failed("oracle-erp-02.zip", "oracle", "AWR 报告解析失败：文件不是有效的 AWR HTML"),
        done("mysql-crm-01.zip", "mysql"),
      ],
      status: "done",
      createdAt: ago(6 * DAY_MS),
    },
    {
      id: "task-seed-010",
      submitterId: "u-user-001",
      items: [done("oracle-fin-01.zip", "oracle", "1.0.0"), done("mysql-fin-01.zip", "mysql")],
      status: "done",
      createdAt: ago(9 * DAY_MS),
    },
    {
      id: "task-seed-002",
      submitterId: "u-user-001",
      items: [done("oracle-core-rac.zip", "oracle", "1.1.0")],
      status: "done",
      createdAt: ago(12 * DAY_MS),
    },
    {
      id: "task-seed-005",
      submitterId: "u-user-001",
      items: [failed("mysql-legacy-01.zip", "mysql", "result.json 校验失败：缺少 meta 字段")],
      status: "failed",
      createdAt: ago(20 * DAY_MS),
    },
    {
      // A disabled user's tasks are kept and stay visible to admins.
      id: "task-seed-009",
      submitterId: "u-disabled-001",
      items: [done("oracle-ops-01.zip", "oracle")],
      status: "done",
      createdAt: ago(25 * DAY_MS),
    },
    {
      id: "task-seed-008",
      submitterId: "u-admin-001",
      items: [done("mysql-dw-01.zip", "mysql", "1.1.0")],
      status: "done",
      createdAt: ago(38 * DAY_MS),
    },
    {
      id: "task-seed-006",
      submitterId: "u-user-001",
      items: [done("oracle-hr-01.zip", "oracle", "1.1.0"), done("oracle-hr-02.zip", "oracle", "1.1.0")],
      status: "done",
      createdAt: ago(45 * DAY_MS),
    },
  ];
}
