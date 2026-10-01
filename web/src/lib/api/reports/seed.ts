import type { DbType } from "@/lib/types";

export interface MockReportTask {
  id: string;
  submitterId: string;
  dbType: DbType;
  /** One per report item, in submission order. */
  fileNames: string[];
  status: "processing" | "done" | "failed";
  createdAt: string;
}

/** Mirrors the seed rows of `stores/history-store.ts`, so their re-download works. */
export function seedReportTasks(): MockReportTask[] {
  return [
    {
      id: "task-seed-001",
      submitterId: "u-user-001",
      dbType: "mysql",
      fileNames: ["mysql-prod-01.zip", "mysql-prod-02.zip"],
      status: "done",
      createdAt: "2026-09-20T14:32:00Z",
    },
    {
      id: "task-seed-002",
      submitterId: "u-user-001",
      dbType: "oracle",
      fileNames: ["oracle-core-rac.zip"],
      status: "done",
      createdAt: "2026-09-18T09:15:00Z",
    },
  ];
}
