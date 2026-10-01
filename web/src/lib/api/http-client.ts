import { ApiError, type ApiErrorCode } from "@/lib/api/errors";

const ENV_API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "").trim();
const ENV_API_PORT = (process.env.NEXT_PUBLIC_API_PORT ?? "8080").trim() || "8080";
const API_BASE_STORAGE_KEY = "dbcheck_api_base";
const API_BASE_MANUAL_STORAGE_KEY = "dbcheck_api_base_manual";

function hasScheme(s: string): boolean {
  return /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(s);
}

function normalizeOrigin(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  const withScheme = hasScheme(trimmed) ? trimmed : `http://${trimmed}`;
  const u = new URL(withScheme);
  return u.origin;
}

function inferDefaultApiBaseFromWindow(): string {
  if (typeof window === "undefined") return "";
  const here = new URL(window.location.origin);

  // Dev convenience: when running Next.js dev server on :3000, assume backend is on :8080.
  // Keep the same hostname so remote Linux deployments work when the server IP changes.
  if (here.port === "3000") {
    here.port = ENV_API_PORT;
    return here.origin;
  }

  return here.origin;
}

function shouldUseStoredApiBase(origin: string): boolean {
  if (typeof window === "undefined") return true;
  if (sessionStorage.getItem(API_BASE_MANUAL_STORAGE_KEY) === "1") return true;
  const here = new URL(window.location.origin);
  if (here.port !== "3000") return true;
  const stored = new URL(origin);
  return stored.hostname === here.hostname;
}

export function getApiBase(): string {
  if (typeof window === "undefined") return ENV_API_BASE;
  const stored = sessionStorage.getItem(API_BASE_STORAGE_KEY) ?? "";
  if (stored.trim()) {
    try {
      const origin = normalizeOrigin(stored);
      if (shouldUseStoredApiBase(origin)) return origin;
    } catch {
      // Fall through to other sources.
    }
  }
  if (ENV_API_BASE) {
    try {
      return normalizeOrigin(ENV_API_BASE);
    } catch {
      // Fall back to inferred base below.
    }
  }
  return inferDefaultApiBaseFromWindow();
}

export function setApiBase(base: string | null): void {
  if (typeof window === "undefined") return;
  const trimmed = (base ?? "").trim();
  if (!trimmed) {
    sessionStorage.removeItem(API_BASE_STORAGE_KEY);
    sessionStorage.removeItem(API_BASE_MANUAL_STORAGE_KEY);
    return;
  }
  const origin = normalizeOrigin(trimmed);
  sessionStorage.setItem(API_BASE_STORAGE_KEY, origin);
  sessionStorage.setItem(API_BASE_MANUAL_STORAGE_KEY, "1");
}

export function apiUrl(path: string): string {
  const base = getApiBase();
  const p = path.startsWith("/") ? path : `/${path}`;
  return new URL(p, base).toString();
}

export function wsUrl(path: string): string {
  if (typeof window === "undefined") return path;
  const u = new URL(getApiBase());
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  u.pathname = path;
  u.search = "";
  u.hash = "";
  return u.toString();
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

function errorCodeFor(status: number): ApiErrorCode {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 400 || status === 409 || status === 413) return "invalid";
  return "failed";
}

/**
 * Sends one request to db-web carrying the session token and maps transport
 * and HTTP failures onto `ApiError`. `action` names the operation in messages.
 */
export async function httpRequest(
  action: string,
  path: string,
  token: string,
  init: RequestInit = {},
): Promise<Response> {
  let resp: Response;
  try {
    resp = await fetch(apiUrl(path), {
      ...init,
      headers: { ...init.headers, Authorization: `Bearer ${token}` },
    });
  } catch (e) {
    throw new ApiError("failed", networkErrorMessage(action, e));
  }
  if (resp.ok) return resp;
  const text = (await resp.text().catch(() => "")).trim();
  throw new ApiError(errorCodeFor(resp.status), `${action}: HTTP ${resp.status}${text ? ` ${text}` : ""}`);
}
