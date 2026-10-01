import { describe, expect, it } from "vitest";
import { createMockApi } from "@/lib/api/mock";
import { memoryStorage, watchToEnd, zipFile } from "@/lib/api/testing";

/** Mock-only behaviour: persistence across reloads and the reset action. */
describe("mock API state", () => {
  async function generateFinishedTask(api: ReturnType<typeof createMockApi>) {
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, {
      items: [{ zip: zipFile("mysql-prod-01.zip"), dbType: "mysql", collectorVersion: "1.2.0", diagnostics: [] }],
    });
    await watchToEnd(api, token, taskId);
    return { token, taskId };
  }

  it("survives a reload: a new instance on the same storage keeps sessions and tasks", async () => {
    const storage = memoryStorage();
    const { token, taskId } = await generateFinishedTask(createMockApi({ storage, stepDelayMs: 0 }));

    const reloaded = createMockApi({ storage, stepDelayMs: 0 });
    const report = await reloaded.reports.download(token, taskId);
    expect(report.size).toBeGreaterThan(0);
  });

  it("fixes the seed's dates at the first read, so later reads don't shift them", async () => {
    let now = Date.parse("2026-10-01T08:00:00Z");
    const api = createMockApi({ storage: memoryStorage(), stepDelayMs: 0, now: () => now });
    const { token } = await api.auth.signIn("user", "user");
    const firstRead = await api.reports.listOwn(token);

    now += 3 * 24 * 60 * 60 * 1000;
    const laterRead = await api.reports.listOwn(token);
    expect(laterRead.map((t) => t.createdAt)).toEqual(firstRead.map((t) => t.createdAt));
  });

  it("reset restores the seed: generated tasks and sessions are gone, seed accounts remain", async () => {
    const storage = memoryStorage();
    const api = createMockApi({ storage, stepDelayMs: 0 });
    const { token, taskId } = await generateFinishedTask(api);

    api.resetMockData();

    await expect(api.reports.download(token, taskId)).rejects.toMatchObject({ code: "unauthorized" });
    const fresh = await api.auth.signIn("user", "user");
    await expect(api.reports.download(fresh.token, taskId)).rejects.toMatchObject({ code: "not_found" });
    const seeded = await api.reports.download(fresh.token, "task-seed-001");
    expect(seeded.size).toBeGreaterThan(0);
  });
});
