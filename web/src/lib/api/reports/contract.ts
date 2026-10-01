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

/** A report item as recorded on its task. */
export interface ReportItem {
  fileName: string;
  dbType: DbType;
  collectorVersion: string | null;
}

/** A recorded report task. */
export interface ReportTask {
  id: string;
  submitter: { id: string; displayName: string };
  status: ReportTaskStatus;
  createdAt: string;
  /** In submission order. */
  items: ReportItem[];
}

/** Progress events of a running report task, in the backend WebSocket shape. */
export type ReportEvent = WsMessage;

export interface ReportsApi {
  /** Submits a report task with the session's user as submitter. */
  generate(token: string, input: ReportTaskInput): Promise<SubmittedReportTask>;
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
