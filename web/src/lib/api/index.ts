/**
 * The API the UI uses. `NEXT_PUBLIC_API_MODE` (inlined at build time) selects
 * the implementation: `mock` (default) keeps all data in this browser's local
 * storage; `real` talks to db-web. There is no fallback between the two.
 */
import type { DbCheckApi } from "@/lib/api/contract";
import { createHttpApi } from "@/lib/api/http";
import { createMockApi } from "@/lib/api/mock";

export type ApiMode = "mock" | "real";

function readApiMode(): ApiMode {
  const raw = (process.env.NEXT_PUBLIC_API_MODE ?? "").trim() || "mock";
  if (raw !== "mock" && raw !== "real") {
    throw new Error(`NEXT_PUBLIC_API_MODE must be "mock" or "real", got "${raw}"`);
  }
  return raw;
}

/** Defers to `window.localStorage` per call, so importing this module is safe during prerendering. */
const browserLocalStorage: Storage = {
  get length() {
    return window.localStorage.length;
  },
  clear: () => window.localStorage.clear(),
  getItem: (key) => window.localStorage.getItem(key),
  key: (index) => window.localStorage.key(index),
  removeItem: (key) => window.localStorage.removeItem(key),
  setItem: (key, value) => window.localStorage.setItem(key, value),
};

export const apiMode: ApiMode = readApiMode();

const mockApi = apiMode === "mock" ? createMockApi({ storage: browserLocalStorage }) : null;

export const api: DbCheckApi = mockApi ?? createHttpApi();

/** Restores the mock seed; `null` in real mode, where there is nothing to reset. */
export const resetMockData: (() => void) | null = mockApi ? () => mockApi.resetMockData() : null;

export * from "@/lib/api/contract";
