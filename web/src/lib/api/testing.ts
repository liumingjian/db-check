/**
 * Shared fixtures for the contract behaviour suites. Every suite runs against
 * each entry of `contractImplementations`; add the real implementation here
 * once a test backend exists.
 */
import type { DbCheckApi, ReportEvent } from "@/lib/api/contract";
import { createMockApi } from "@/lib/api/mock";

export function memoryStorage(): Storage {
  const data = new Map<string, string>();
  return {
    get length() {
      return data.size;
    },
    clear: () => data.clear(),
    getItem: (key) => data.get(key) ?? null,
    key: (index) => Array.from(data.keys())[index] ?? null,
    removeItem: (key) => void data.delete(key),
    setItem: (key, value) => void data.set(key, String(value)),
  };
}

export const contractImplementations: Array<[name: string, makeApi: () => DbCheckApi]> = [
  ["mock", () => createMockApi({ storage: memoryStorage(), stepDelayMs: 0 })],
];

/** Collects a task's events until it ends with `done` or `error`. */
export function watchToEnd(api: DbCheckApi, token: string, taskId: string): Promise<ReportEvent[]> {
  return new Promise((resolve) => {
    const events: ReportEvent[] = [];
    const stop = api.reports.watch(token, taskId, (event) => {
      events.push(event);
      if (event.type === "done" || event.type === "error") {
        stop();
        resolve(events);
      }
    });
  });
}

export function zipFile(name: string): File {
  return new File(["PK"], name, { type: "application/zip" });
}
