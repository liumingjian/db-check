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

function currentOrigin(): string {
  if (typeof window === "undefined") return "unknown";
  return window.location.origin;
}

function networkErrorMessage(action: string, apiBase: string, cause: unknown): string {
  return [
    `${action}: 浏览器无法连接 db-web API。`,
    `当前 API 地址: ${apiBase || "未配置"}`,
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
 * The message for a failed response. db-web's error envelope is
 * `{"code", "message"}` with a Chinese message the console shows as-is; any
 * other body (a proxy, for one)
 * falls back to the raw status and body.
 */
function errorMessageFor(action: string, status: number, body: string): string {
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === "object" && "message" in parsed && typeof parsed.message === "string") {
      return parsed.message;
    }
  } catch {
    // Not the envelope.
  }
  return `${action}: HTTP ${status}${body ? ` ${body}` : ""}`;
}

/** A POST to db-web: what it does (`action`, for fallback messages), where, as whom, and its JSON body, if any. */
export interface JsonPost {
  action: string;
  path: string;
  token: string | null;
  body?: unknown;
}

/** How each domain's HTTP implementation reaches db-web. */
export interface HttpClient {
  /**
   * Sends one request, with the session token unless it is `null`, and maps
   * transport and HTTP failures onto `ApiError`. `action` names the
   * operation in fallback messages.
   */
  request(action: string, path: string, token: string | null, init?: RequestInit): Promise<Response>;
  /** Like `request`, as a POST sending `body` (if any) as JSON. */
  post(req: JsonPost): Promise<Response>;
  /** The WebSocket URL for an API path. */
  wsUrl(path: string): string;
}

/**
 * A client for db-web at `baseUrl` or, without one, at the base the browser
 * resolves (`getApiBase`). The contract tests pass the test server's URL.
 */
export function createHttpClient(baseUrl?: string): HttpClient {
  const base = () => baseUrl ?? getApiBase();

  async function request(action: string, path: string, token: string | null, init: RequestInit = {}) {
    const headers = new Headers(init.headers);
    if (token !== null) headers.set("Authorization", `Bearer ${token}`);
    let resp: Response;
    try {
      resp = await fetch(new URL(path, base()).toString(), { ...init, headers });
    } catch (e) {
      throw new ApiError("failed", networkErrorMessage(action, base(), e));
    }
    if (resp.ok) return resp;
    const text = (await resp.text().catch(() => "")).trim();
    throw new ApiError(errorCodeFor(resp.status), errorMessageFor(action, resp.status, text));
  }

  return {
    request,

    post({ action, path, token, body }) {
      if (body === undefined) return request(action, path, token, { method: "POST" });
      return request(action, path, token, {
        method: "POST",
        body: JSON.stringify(body),
        headers: { "Content-Type": "application/json" },
      });
    },

    wsUrl(path) {
      // With neither a base nor a window there is no origin to resolve against.
      if (baseUrl === undefined && typeof window === "undefined") return path;
      const u = new URL(base());
      u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
      u.pathname = path;
      u.search = "";
      u.hash = "";
      return u.toString();
    },
  };
}
