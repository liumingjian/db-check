/**
 * The "real" contract entry: the HTTP implementation driving the Go contract
 * test server (reporter/internal/testserver), which `test-server.global-setup.ts`
 * builds and starts once per `npm test`.
 */
import { inject } from "vitest";
import type { DbCheckApi } from "@/lib/api/contract";
import { createHttpApi } from "@/lib/api/http";

async function pinClock(baseUrl: string, path: "/test/reset" | "/test/clock", now: number): Promise<void> {
  const resp = await fetch(new URL(path, baseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ now: new Date(now).toISOString() }),
  });
  if (!resp.ok) throw new Error(`test server ${path}: HTTP ${resp.status} ${await resp.text()}`);
}

/**
 * Runs `before` ahead of every operation. `watch` is synchronous and passes
 * straight through; suites sign in (and so wait for the reset) before they
 * watch anything.
 */
function gated(api: DbCheckApi, before: () => Promise<void>): DbCheckApi {
  const wrap = <T extends object>(domain: T): T =>
    Object.fromEntries(
      Object.entries(domain).map(([name, op]) => [
        name,
        name === "watch"
          ? op
          : async (...args: unknown[]) => {
              await before();
              return op(...args);
            },
      ]),
    ) as T;
  return {
    auth: wrap(api.auth),
    users: wrap(api.users),
    releases: wrap(api.releases),
    downloads: wrap(api.downloads),
    reports: wrap(api.reports),
  };
}

/**
 * An API over the test server, reset to the seed fixture with the clock
 * pinned at `now()`. Before each operation the server's clock follows
 * `now()`, so a suite that advances its clock moves the server's too.
 *
 * The server is shared, so suites on the real entry must not run in
 * parallel (vitest.config.ts turns file parallelism off).
 */
export function createTestServerApi({ now }: { now: () => number }): DbCheckApi {
  const baseUrl = inject("testServerUrl");
  let pinned = now();
  const reset = pinClock(baseUrl, "/test/reset", pinned);
  return gated(createHttpApi({ baseUrl }), async () => {
    await reset;
    const current = now();
    if (current !== pinned) {
      pinned = current;
      await pinClock(baseUrl, "/test/clock", current);
    }
  });
}
