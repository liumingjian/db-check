/* ─── DB types ─── */
export type DbType = "mysql" | "oracle" | "gaussdb";

export interface DbTypeOption {
  type: DbType;
  label: string;
  versions: string;
  description: string;
  hasAwrWdr: boolean;
  awrWdrLabel: string;
}

export const DB_TYPE_OPTIONS: DbTypeOption[] = [
  {
    type: "mysql",
    label: "MySQL",
    versions: "5.6 / 5.7 / 8.0",
    description: "社区版与企业版 MySQL 数据库",
    hasAwrWdr: false,
    awrWdrLabel: "",
  },
  {
    type: "oracle",
    label: "Oracle",
    versions: "11g / 19c",
    description: "Oracle 数据库实例",
    hasAwrWdr: true,
    awrWdrLabel: "AWR 报告",
  },
  {
    type: "gaussdb",
    label: "GaussDB",
    versions: "505.2.1",
    description: "华为 GaussDB 数据库",
    // Web 版暂不支持 WDR 上传
    hasAwrWdr: false,
    awrWdrLabel: "",
  },
];

/* ─── File entries ─── */
export interface ZipFileEntry {
  id: string;
  file: File;
  name: string;
  size: number;
}

export interface AwrFileEntry {
  file: File;
  name: string;
}

/* ─── Log entries ─── */
export type LogLevel = "info" | "success" | "error" | "warn";

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  message: string;
}

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

/* ─── Progress state ─── */
export interface ProgressState {
  completed: number;
  total: number;
  currentFile: string;
}
