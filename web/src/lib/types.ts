/* ─── DB types ─── */
/** The database types a report can be generated for. */
export const DB_TYPES = ["mysql", "oracle", "gaussdb"] as const;
export type DbType = (typeof DB_TYPES)[number];

/* ─── Log entries ─── */
export type LogLevel = "info" | "success" | "error" | "warn";

/* ─── API contracts ─── */
export interface GenerateResponse {
  task_id: string;
  status: "processing";
  total: number;
  ws_url: string;
}

export interface WsLogMessage {
  type: "log";
  seq: number;
  timestamp: string;
  level: LogLevel;
  message: string;
}

export interface WsProgressMessage {
  type: "progress";
  seq: number;
  completed: number;
  total: number;
  current_file: string;
}

export interface WsDoneMessage {
  type: "done";
  seq: number;
  download_url: string;
}

export interface WsErrorMessage {
  type: "error";
  seq: number;
  message: string;
}

export type WsMessage =
  | WsLogMessage
  | WsProgressMessage
  | WsDoneMessage
  | WsErrorMessage;
