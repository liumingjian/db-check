const ENV_API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "").trim();
const API_BASE_STORAGE_KEY = "dbcheck_api_base";

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
  // This avoids the common misconfig where requests go to http://127.0.0.1:3000/api/... (404).
  if (here.port === "3000") {
    // Prefer loopback for backend, even when the frontend is opened via the dev server
    // "Network" address (e.g. 192.168.x.x). db-web is commonly bound to 127.0.0.1.
    here.hostname = "127.0.0.1";
    here.port = "8080";
    return here.origin;
  }

  return here.origin;
}

export function getApiBase(): string {
  if (typeof window === "undefined") return ENV_API_BASE;
  const stored = sessionStorage.getItem(API_BASE_STORAGE_KEY) ?? "";
  if (stored.trim()) {
    try {
      return normalizeOrigin(stored);
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
    return;
  }
  const origin = normalizeOrigin(trimmed);
  sessionStorage.setItem(API_BASE_STORAGE_KEY, origin);
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
