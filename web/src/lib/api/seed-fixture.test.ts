import { describe, expect, it } from "vitest";
import { createMockApi } from "@/lib/api/mock";
import { seedTime } from "@/lib/api/seed-fixture";
import { memoryStorage, SEED_NOW } from "@/lib/api/testing";
import { DAY_MS } from "@/lib/time";

/** The seed times the mock shows when first read at `now`, one record per domain. */
async function seedTimesAt(now: number) {
  const api = createMockApi({ storage: memoryStorage(), stepDelayMs: 0, now: () => now });
  const admin = await api.auth.signIn("admin", "admin");
  const engineer = await api.auth.signIn("user", "user");
  const users = await api.users.list(admin.token);
  const releases = await api.releases.list(admin.token);
  const records = await api.downloads.records(admin.token);
  const tasks = await api.reports.listOwn(engineer.token);
  const disabled = users.find((u) => u.id === "u-disabled-001");
  return {
    appliedAt: users.find((u) => u.id === "u-admin-001")?.appliedAt,
    actionAt: disabled?.actions.find((a) => a.action === "disable")?.at,
    publishedAt: releases.find((r) => r.version === "1.1.0")?.publishedAt,
    downloadedAt: records.find((r) => r.id === "d-seed-001")?.at,
    createdAt: tasks.find((t) => t.id === "task-seed-003")?.createdAt,
  };
}

describe("seed fixture", () => {
  it("resolves the Phase 1 seed dates when read at the reference time", async () => {
    expect(await seedTimesAt(SEED_NOW)).toEqual({
      appliedAt: "2026-08-01T09:00:00.000Z",
      actionAt: "2026-09-10T10:00:00.000Z",
      publishedAt: "2026-08-20T07:02:00.000Z",
      downloadedAt: "2026-08-02T01:10:00.000Z",
      createdAt: "2026-10-01T07:57:00.000Z",
    });
  });

  it("moves every seed time along with now", async () => {
    expect(await seedTimesAt(SEED_NOW + DAY_MS)).toEqual({
      appliedAt: "2026-08-02T09:00:00.000Z",
      actionAt: "2026-09-11T10:00:00.000Z",
      publishedAt: "2026-08-21T07:02:00.000Z",
      downloadedAt: "2026-08-03T01:10:00.000Z",
      createdAt: "2026-10-02T07:57:00.000Z",
    });
  });

  it.each([
    ["now", "2026-10-01T08:00:00.000Z"],
    ["now-45m", "2026-10-01T07:15:00.000Z"],
    ["now-1d2h3m", "2026-09-30T05:57:00.000Z"],
  ])("reads the offset %s", (offset, expected) => {
    expect(seedTime(offset, SEED_NOW)).toBe(expected);
  });

  it.each(["2026-08-01T09:00:00Z", "now+1d", "now-1h1d", "now-", "now-1w"])("refuses the malformed offset %s", (offset) => {
    expect(() => seedTime(offset, 0)).toThrow(offset);
  });
});
