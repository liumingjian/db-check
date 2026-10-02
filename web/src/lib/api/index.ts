/**
 * The API the UI uses. `NEXT_PUBLIC_API_MODE` (inlined at build time) selects
 * the implementation: `real` (default) talks to db-web; `mock` keeps all data
 * in this browser's local storage. There is no fallback between the two.
 */
import type { DbCheckApi } from "@/lib/api/contract";
import { createHttpApi } from "@/lib/api/http";
import { createMockApi } from "@/lib/api/mock";
import { type ApiMode, parseApiMode } from "@/lib/api/mode";

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

export const apiMode: ApiMode = parseApiMode(process.env.NEXT_PUBLIC_API_MODE);

const mockApi = apiMode === "mock" ? createMockApi({ storage: browserLocalStorage }) : null;

export const api: DbCheckApi = mockApi ?? createHttpApi();

/** Restores the mock seed; `null` in real mode, where there is nothing to reset. */
export const resetMockData: (() => void) | null = mockApi ? () => mockApi.resetMockData() : null;

export * from "@/lib/api/contract";
