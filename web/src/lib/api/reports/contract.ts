import type { DbType, WsMessage } from "@/lib/types";

/** One report item: a collector ZIP and its optional AWR or WDR HTML files. */
export interface ReportItemInput {
  zip: File;
  diagnostics: File[];
}

export interface ReportTaskInput {
  dbType: DbType;
  items: ReportItemInput[];
}

export interface SubmittedReportTask {
  taskId: string;
  total: number;
}

/** Progress events of a running report task, in the backend WebSocket shape. */
export type ReportEvent = WsMessage;

export interface ReportsApi {
  /** Submits a report task with the session's user as submitter. */
  generate(token: string, input: ReportTaskInput): Promise<SubmittedReportTask>;
  /**
   * Streams the task's events until it ends with a `done` or `error` event;
   * failures to stream also arrive as an `error` event. Returns a stop function.
   */
  watch(token: string, taskId: string, onEvent: (event: ReportEvent) => void): () => void;
  /** Downloads a finished task's report collection (a ZIP). */
  download(token: string, taskId: string): Promise<Blob>;
}
