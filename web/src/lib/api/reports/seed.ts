import type { ReportItem, ReportTaskStatus } from "@/lib/api/reports/contract";
import type { DbType } from "@/lib/types";

export interface MockReportTask {
  id: string;
  submitterId: string;
  /** One per report item, in submission order. */
  items: ReportItem[];
  status: ReportTaskStatus;
  createdAt: string;
}

const MINUTE_MS = 60 * 1000;
const DAY_MS = 24 * 60 * MINUTE_MS;

function done(fileName: string, dbType: DbType, collectorVersion: string | null = "1.2.0"): ReportItem {
  return { fileName, dbType, collectorVersion, outcome: { status: "done" } };
}

function failed(fileName: string, dbType: DbType, reason: string): ReportItem {
  return { fileName, dbType, collectorVersion: "1.2.0", outcome: { status: "failed", reason } };
}

/**
 * Dates are relative to `now`, so each 我的报告 marker (生成中, 部分失败, 失败,
 * 已过期) keeps showing however long after the seed is read.
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
