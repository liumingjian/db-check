import { apiUrl, getApiBase } from "@/lib/api";
import type { GenerateResponse, ZipFileEntry } from "@/lib/types";

const BACKEND_PROBE_TASK_ID = "frontend-probe";

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

async function readErrorText(resp: Response): Promise<string> {
  const text = await resp.text().catch(() => "");
  return text.trim();
}

function httpErrorMessage(action: string, resp: Response, text: string): string {
  const detail = text ? ` ${text}` : "";
  return `${action}: HTTP ${resp.status}${detail}`;
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

  const text = await readErrorText(resp);
  if (resp.status === 401) {
    throw new Error("API 探测失败: Token 无效，需与后端 DBCHECK_API_TOKEN 一致。");
  }
  if (resp.status === 403) {
    throw new Error(
      `API 探测失败: 当前页面 Origin 未被后端 ALLOWED_ORIGINS 放行。当前页面 Origin: ${currentOrigin()}`,
    );
  }
  throw new Error(httpErrorMessage("API 探测失败", resp, text));
}

export async function generateReportTask(
  token: string,
  zipFiles: ZipFileEntry[],
  awrFiles: Record<string, File>,
): Promise<GenerateResponse> {
  await probeReportBackend(token);

  const form = new FormData();
  zipFiles.forEach((z) => {
    form.append("zips", z.file, z.name);
  });
  zipFiles.forEach((z, idx) => {
    const awr = awrFiles[z.id];
    if (awr) {
      form.append(`awr_${idx + 1}`, awr, awr.name);
    }
  });

  let resp: Response;
  try {
    resp = await fetch(apiUrl("/api/reports/generate"), {
      method: "POST",
      headers: authHeaders(token),
      body: form,
    });
  } catch (e) {
    throw new Error(networkErrorMessage("生成接口请求失败", e));
  }
  if (!resp.ok) {
    const text = await readErrorText(resp);
    throw new Error(httpErrorMessage("生成接口失败", resp, text));
  }
  return (await resp.json()) as GenerateResponse;
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
    const text = await readErrorText(resp);
    throw new Error(httpErrorMessage("下载失败", resp, text));
  }
  return resp.blob();
}
