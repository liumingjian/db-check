import { apiUrl, getApiBase } from "@/lib/api";
import type {
  DbType,
  GenerateResponse,
  ReportTaskSnapshot,
  ZipFileEntry,
} from "@/lib/types";

const BACKEND_PROBE_TASK_ID = "frontend-probe";

export const REPORT_API_ERROR_CODES = {
  capacityExhausted: "capacity_exhausted",
  idempotencyKeyConflict: "idempotency_key_conflict",
  storageUnavailable: "storage_unavailable",
} as const;

export type ReportAPIErrorCode =
  (typeof REPORT_API_ERROR_CODES)[keyof typeof REPORT_API_ERROR_CODES];

export class ReportAPIError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "ReportAPIError";
    this.status = status;
    this.code = code;
  }
}

function authHeaders(token: string): HeadersInit {
  return { Authorization: `Bearer ${token}` };
}

function currentOrigin(): string {
  if (typeof window === "undefined") return "unknown";
  return window.location.origin;
}

function networkErrorMessage(action: string, cause: unknown): string {
  return [
    `${action}: 浏览器无法连接 db-web API。`,
    `当前 API 地址: ${getApiBase() || "未配置"}`,
    `当前页面 Origin: ${currentOrigin()}`,
    "请确认 db-web 已启动、API Base 指向后端、ALLOWED_ORIGINS 包含当前页面 Origin。",
    `原始错误: ${String(cause)}`,
  ].join(" ");
}

type ErrorDetails = {
  message: string;
  code?: string;
};

export function isReportAPIErrorCode<Code extends ReportAPIErrorCode>(
  error: unknown,
  code: Code,
): error is ReportAPIError & { code: Code } {
  return error instanceof ReportAPIError && error.code === code;
}

async function readErrorDetails(resp: Response): Promise<ErrorDetails> {
  const text = await resp.text().catch(() => "");
  const message = text.trim();
  if (!message) {
    return { message };
  }

  try {
    const payload: unknown = JSON.parse(message);
    if (typeof payload !== "object" || payload === null) {
      return { message };
    }
    const record = payload as Record<string, unknown>;
    return {
      message: typeof record.error === "string" ? record.error : message,
      code: typeof record.code === "string" ? record.code : undefined,
    };
  } catch {
    return { message };
  }
}

function httpErrorMessage(action: string, resp: Response, text: string): string {
  const detail = text ? ` ${text}` : "";
  return `${action}: HTTP ${resp.status}${detail}`;
}

async function responseError(action: string, resp: Response): Promise<ReportAPIError> {
  const details = await readErrorDetails(resp);
  return new ReportAPIError(
    httpErrorMessage(action, resp, details.message),
    resp.status,
    details.code,
  );
}

export async function probeReportBackend(token: string): Promise<void> {
  let resp: Response;
  try {
    resp = await fetch(apiUrl(`/api/reports/status/${BACKEND_PROBE_TASK_ID}`), {
      headers: authHeaders(token),
    });
  } catch (e) {
    throw new Error(networkErrorMessage("API 探测失败", e));
  }

  if (resp.ok || resp.status === 404) return;

  const details = await readErrorDetails(resp);
  if (resp.status === 401) {
    throw new Error("API 探测失败: Token 无效，需与后端 DBCHECK_API_TOKEN 一致。");
  }
  if (resp.status === 403) {
    throw new Error(
      `API 探测失败: 当前页面 Origin 未被后端 ALLOWED_ORIGINS 放行。当前页面 Origin: ${currentOrigin()}`,
    );
  }
  throw new ReportAPIError(
    httpErrorMessage("API 探测失败", resp, details.message),
    resp.status,
    details.code,
  );
}

export async function generateReportTask(
  token: string,
  dbType: DbType,
  zipFiles: ZipFileEntry[],
  awrFiles: Record<string, File[]>,
  submissionKey?: string,
): Promise<GenerateResponse> {
  await probeReportBackend(token);

  const form = new FormData();
  zipFiles.forEach((z) => {
    form.append("zips", z.file, z.name);
  });
  zipFiles.forEach((z, idx) => {
    const files = awrFiles[z.id] ?? [];
    const field = dbType === "gaussdb" ? "wdr" : "awr";
    const selected = dbType === "gaussdb" ? files : files.slice(0, 1);
    selected.forEach((file) => {
      form.append(`${field}_${idx + 1}`, file, file.name);
    });
  });

  let resp: Response;
  try {
    const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
    if (submissionKey?.trim()) {
      headers["Idempotency-Key"] = submissionKey.trim();
    }
    resp = await fetch(apiUrl("/api/reports/generate"), {
      method: "POST",
      headers,
      body: form,
    });
  } catch (e) {
    throw new Error(networkErrorMessage("生成接口请求失败", e));
  }
  if (!resp.ok) {
    throw await responseError("生成接口失败", resp);
  }
  return (await resp.json()) as GenerateResponse;
}

export async function getReportTaskStatus(
  token: string,
  taskId: string,
): Promise<ReportTaskSnapshot> {
  let resp: Response;
  try {
    resp = await fetch(apiUrl(`/api/reports/status/${encodeURIComponent(taskId)}`), {
      headers: authHeaders(token),
    });
  } catch (e) {
    throw new Error(networkErrorMessage("查询任务状态失败", e));
  }
  if (!resp.ok) {
    throw await responseError("查询任务状态失败", resp);
  }
  return (await resp.json()) as ReportTaskSnapshot;
}

export async function downloadReportBlob(token: string, downloadUrl: string): Promise<Blob> {
  let resp: Response;
  try {
    resp = await fetch(apiUrl(downloadUrl), {
      headers: authHeaders(token),
    });
  } catch (e) {
    throw new Error(networkErrorMessage("下载请求失败", e));
  }
  if (!resp.ok) {
    throw await responseError("下载失败", resp);
  }
  return resp.blob();
}
