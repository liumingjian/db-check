/* ─── DB types ─── */
export type DbType = "mysql" | "oracle" | "gaussdb" | "postgresql" | "dameng";

export interface DiagnosticMeta {
  label: string;
  accept: string;
  allowMultiple: boolean;
  required: boolean;
  hint: string;
}

export interface DbTypeMeta {
  type: DbType;
  label: string;
  versions: string;
  description: string;
  diagnostic?: DiagnosticMeta;
}

export const DB_TYPE_OPTIONS: DbTypeMeta[] = [
  {
    type: "mysql",
    label: "MySQL",
    versions: "5.6 / 5.7 / 8.0",
    description: "社区版与企业版 MySQL 数据库",
  },
  {
    type: "oracle",
    label: "Oracle",
    versions: "11g / 19c",
    description: "Oracle 数据库实例",
    diagnostic: {
      label: "AWR 报告",
      accept: ".html,.htm",
      allowMultiple: false,
      required: false,
      hint: "可选关联 AWR HTML 性能报告",
    },
  },
  {
    type: "gaussdb",
    label: "GaussDB",
    versions: "505.2.1",
    description: "华为 GaussDB 数据库",
    diagnostic: {
      label: "WDR 报告",
      accept: ".html,.htm",
      allowMultiple: true,
      required: false,
      hint: "可选关联一个或多个 WDR HTML 报告",
    },
  },
  {
    type: "postgresql",
    label: "PostgreSQL",
    versions: "12 ~ 16",
    description: "开源 PostgreSQL 数据库",
    diagnostic: {
      label: "pg_profile 报告",
      accept: ".html,.htm",
      allowMultiple: false,
      required: false,
      hint: "可选关联 pg_profile HTML 性能报告",
    },
  },
  {
    type: "dameng",
    label: "达梦 DM",
    versions: "DM7 / DM8",
    description: "国产达梦数据库",
    diagnostic: {
      label: "AWR 报告",
      accept: ".html,.htm",
      allowMultiple: false,
      required: false,
      hint: "可选关联达梦 AWR 性能报告",
    },
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
