import type { DbType, WsMessage } from "@/lib/types";

/**
 * One report item as submitted: a collector ZIP, what the browser read from
 * it (see `lib/report-input`), and its optional AWR or WDR HTML files.
 */
export interface ReportItemInput {
  zip: File;
  dbType: DbType;
  /** `meta.collector_version` of the ZIP's result JSON; `null` when unknown. */
  collectorVersion: string | null;
  diagnostics: File[];
}

export interface ReportTaskInput {
  items: ReportItemInput[];
}

export interface SubmittedReportTask {
  taskId: string;
  total: number;
}

export type ReportTaskStatus = "processing" | "done" | "failed";

/** Each report item's own outcome; a failed item always carries its reason. */
export type ReportItemOutcome = { status: "processing" } | { status: "done" } | { status: "failed"; reason: string };

/** A report item as recorded on its task. */
export interface ReportItem {
  fileName: string;
  dbType: DbType;
  collectorVersion: string | null;
  outcome: ReportItemOutcome;
}

/** A recorded report task. */
export interface ReportTask {
  id: string;
  submitter: { id: string; displayName: string };
  status: ReportTaskStatus;
  createdAt: string;
  /**
   * The uploaded ZIPs and generated reports are deleted 30 days after
   * submission (ADR 0003); the task record itself stays.
   */
  expired: boolean;
  /** In submission order. */
  items: ReportItem[];
}

/** Progress events of a running report task, in the backend WebSocket shape. */
export type ReportEvent = WsMessage;

export interface ReportsApi {
  /** Submits a report task with the session's user as submitter. */
  generate(token: string, input: ReportTaskInput): Promise<SubmittedReportTask>;
  /** The caller's own tasks, admins included, newest first (我的报告). */
  listOwn(token: string): Promise<ReportTask[]>;
  /** One task; `not_found` unless the caller is its submitter or an admin. */
  getTask(token: string, taskId: string): Promise<ReportTask>;
  /**
   * Streams the task's events until it ends with a `done` or `error` event;
   * failures to stream also arrive as an `error` event. Returns a stop function.
   */
  watch(token: string, taskId: string, onEvent: (event: ReportEvent) => void): () => void;
  /** Downloads a finished task's report collection (a ZIP). */
  download(token: string, taskId: string): Promise<Blob>;
}
