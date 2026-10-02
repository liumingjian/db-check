/**
 * Shared fixtures for the contract behaviour suites. Every suite runs against
 * each entry of an implementations list: the mock, plus the real backend
 * once the suite's domain opts in (`contractImplementationsWithReal`).
 */
import { strToU8, zipSync } from "fflate";
import type { DbCheckApi, ReportEvent } from "@/lib/api/contract";
import { createMockApi } from "@/lib/api/mock";
import { createTestServerApi } from "@/lib/api/test-server";
import type { DbType } from "@/lib/types";

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

export interface ContractOptions {
  /** Pins the clock (epoch ms) for time-dependent rules such as report retention. */
  now?: () => number;
}

/**
 * The suites' default clock. The seed fixture's times are offsets from now,
 * so pinning it here gives the seed records fixed dates the suites can name.
 */
export const SEED_NOW = Date.parse("2026-10-01T08:00:00Z");

export type ContractImplementation = [name: string, makeApi: (options?: ContractOptions) => DbCheckApi];

const mock: ContractImplementation = [
  "mock",
  (options) => createMockApi({ storage: memoryStorage(), stepDelayMs: 0, now: () => SEED_NOW, ...options }),
];

/** db-web itself, as the Go contract test server (see test-server.ts). */
const real: ContractImplementation = ["real", (options) => createTestServerApi({ now: () => SEED_NOW, ...options })];

/** For suites whose domain db-web does not serve yet: the mock only. */
export const contractImplementations: ContractImplementation[] = [mock];

/**
 * For suites whose domain db-web serves: the mock and the real backend. A
 * suite opts in by running `describe.each(contractImplementationsWithReal)`
 * instead of `contractImplementations`, once its domain's routes exist.
 */
export const contractImplementationsWithReal: ContractImplementation[] = [mock, real];

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

/**
 * A tiny real collector ZIP: a manifest naming its result JSON, whose `meta`
 * carries the collector version (`null` writes none). The real backend reads
 * both from the ZIP itself, so they must match the item's `dbType` and
 * `collectorVersion`.
 */
export function zipFile(name: string, dbType: DbType = "mysql", collectorVersion: string | null = "1.2.0"): File {
  const manifest = { schema_version: "1.0", db_type: dbType, artifacts: { result: "result.json" } };
  const result = { meta: { schema_version: "2.0", ...(collectorVersion === null ? {} : { collector_version: collectorVersion }) } };
  const zip = zipSync({
    "manifest.json": strToU8(JSON.stringify(manifest)),
    "result.json": strToU8(JSON.stringify(result)),
  });
  return new File([zip], name, { type: "application/zip" });
}
