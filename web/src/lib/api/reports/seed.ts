import type { ReportItem, ReportTaskStatus } from "@/lib/api/reports/contract";

export interface MockReportTask {
  id: string;
  submitterId: string;
  /** One per report item, in submission order. */
  items: ReportItem[];
  status: ReportTaskStatus;
  createdAt: string;
}

/** Mirrors the seed rows of `stores/history-store.ts`, so their re-download works. */
export function seedReportTasks(): MockReportTask[] {
  return [
    {
      id: "task-seed-001",
      submitterId: "u-user-001",
      items: [
        { fileName: "mysql-prod-01.zip", dbType: "mysql", collectorVersion: "1.2.0" },
        { fileName: "mysql-prod-02.zip", dbType: "mysql", collectorVersion: "1.2.0" },
      ],
      status: "done",
      createdAt: "2026-09-20T14:32:00Z",
    },
    {
      id: "task-seed-002",
      submitterId: "u-user-001",
      items: [{ fileName: "oracle-core-rac.zip", dbType: "oracle", collectorVersion: "1.1.0" }],
      status: "done",
      createdAt: "2026-09-18T09:15:00Z",
    },
  ];
}
