/**
 * The typed API contract every console screen talks to, one interface per
 * domain. Two implementations sit behind it: `createMockApi` (browser local
 * storage) and `createHttpApi` (db-web). `NEXT_PUBLIC_API_MODE` picks one at
 * build time in `./index.ts`. The `*.test.ts` suites beside each domain pin
 * the contract's behaviour through this interface only.
 */
import type { AuthApi } from "@/lib/api/auth/contract";
import type { DownloadsApi } from "@/lib/api/downloads/contract";
import type { ReleasesApi } from "@/lib/api/releases/contract";
import type { ReportsApi } from "@/lib/api/reports/contract";
import type { UsersApi } from "@/lib/api/users/contract";

export interface DbCheckApi {
  auth: AuthApi;
  users: UsersApi;
  releases: ReleasesApi;
  downloads: DownloadsApi;
  reports: ReportsApi;
}

export type { Session } from "@/lib/api/auth/contract";
export type {
  ReportEvent,
  ReportItem,
  ReportItemInput,
  ReportTask,
  ReportTaskInput,
  ReportTaskStatus,
  SubmittedReportTask,
} from "@/lib/api/reports/contract";
export { ApiError, type ApiErrorCode } from "@/lib/api/errors";
