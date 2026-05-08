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
