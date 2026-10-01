import { describe, expect, it } from "vitest";
import { contractImplementations } from "@/lib/api/testing";

describe.each(contractImplementations)("%s downloads contract", (_name, makeApi) => {
  it("records one download record per package download", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    const before = await api.downloads.records(admin.token);

    const blob = await api.downloads.download(engineer.token, "1.2.0", "windows-arm64");
    expect(blob.size).toBeGreaterThan(0);

    const after = await api.downloads.records(admin.token);
    expect(after).toHaveLength(before.length + 1);
    const added = after.filter((r) => !before.some((b) => b.id === r.id));
    expect(added).toEqual([
      expect.objectContaining({ userId: engineer.user.id, version: "1.2.0", platform: "windows-arm64" }),
    ]);
    expect(Number.isNaN(Date.parse(added[0].at))).toBe(false);
  });

  it("lists download records newest first", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    await api.downloads.download(engineer.token, "1.1.0", "linux-amd64");
    const records = await api.downloads.records(admin.token);
    expect(records[0]).toMatchObject({ version: "1.1.0", platform: "linux-amd64" });
    const times = records.map((r) => r.at);
    expect(times).toEqual([...times].sort().reverse());
  });

  it.each(["1.3.0-rc1", "1.0.0"])("refuses an engineer the hidden release %s and records nothing", async (version) => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    const before = await api.downloads.records(admin.token);
    await expect(api.downloads.download(engineer.token, version, "linux-amd64")).rejects.toMatchObject({
      code: "not_found",
    });
    expect(await api.downloads.records(admin.token)).toHaveLength(before.length);
  });

  it.each([
    ["revoked", "1.0.0"],
    ["pre-release", "1.3.0-rc1"],
  ])("lets an admin download the %s release %s", async (_status, version) => {
    const api = makeApi();
    const admin = await api.auth.signIn("admin", "admin");
    const blob = await api.downloads.download(admin.token, version, "linux-arm64");
    expect(blob.size).toBeGreaterThan(0);
    expect((await api.downloads.records(admin.token))[0]).toMatchObject({
      userId: admin.user.id,
      version,
      platform: "linux-arm64",
    });
  });

  it("refuses an unknown package", async () => {
    const api = makeApi();
    const admin = await api.auth.signIn("admin", "admin");
    await expect(api.downloads.download(admin.token, "9.9.9", "linux-amd64")).rejects.toMatchObject({
      code: "not_found",
    });
  });

  it("keeps download records from engineers", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    await expect(api.downloads.records(engineer.token)).rejects.toMatchObject({ code: "forbidden" });
  });

  it("refuses to download without a valid session", async () => {
    const api = makeApi();
    await expect(api.downloads.download("not-a-session", "1.2.0", "linux-amd64")).rejects.toMatchObject({
      code: "unauthorized",
    });
  });

  describe("filters", () => {
    async function seeded() {
      const api = makeApi();
      const admin = await api.auth.signIn("admin", "admin");
      return { api, admin };
    }

    it("narrows the records to one user", async () => {
      const { api, admin } = await seeded();
      const records = await api.downloads.records(admin.token, { userId: "u-admin-001" });
      expect(records.map((r) => r.id)).toEqual(["d-seed-006", "d-seed-005"]);
    });

    it("narrows the records to one release", async () => {
      const { api, admin } = await seeded();
      const records = await api.downloads.records(admin.token, { version: "1.2.0" });
      expect(records.map((r) => r.id)).toEqual(["d-seed-005", "d-seed-004", "d-seed-003"]);
    });

    it("narrows the records to a time range, start inclusive and end exclusive", async () => {
      const { api, admin } = await seeded();
      const records = await api.downloads.records(admin.token, {
        from: "2026-08-22T06:00:00Z",
        to: "2026-09-12T08:30:00Z",
      });
      expect(records.map((r) => r.id)).toEqual(["d-seed-003", "d-seed-002"]);
    });

    it("accepts an open-ended time range", async () => {
      const { api, admin } = await seeded();
      const since = await api.downloads.records(admin.token, { from: "2026-09-15T00:00:00Z" });
      expect(since.map((r) => r.id)).toEqual(["d-seed-006", "d-seed-005"]);
      const until = await api.downloads.records(admin.token, { to: "2026-08-03T00:00:00Z" });
      expect(until.map((r) => r.id)).toEqual(["d-seed-001"]);
    });

    it("combines filters", async () => {
      const { api, admin } = await seeded();
      const records = await api.downloads.records(admin.token, {
        userId: "u-user-001",
        version: "1.2.0",
        from: "2026-09-12T00:00:00Z",
      });
      expect(records.map((r) => r.id)).toEqual(["d-seed-004"]);
    });

    it("includes a fresh download in a filtered list", async () => {
      const { api, admin } = await seeded();
      const engineer = await api.auth.signIn("user", "user");
      await api.downloads.download(engineer.token, "1.1.0", "windows-amd64");
      const records = await api.downloads.records(admin.token, { userId: engineer.user.id, version: "1.1.0" });
      expect(records.map((r) => r.platform)).toEqual(["windows-amd64", "linux-arm64"]);
    });

    it("keeps filtered records from engineers", async () => {
      const api = makeApi();
      const engineer = await api.auth.signIn("user", "user");
      await expect(api.downloads.records(engineer.token, { userId: engineer.user.id })).rejects.toMatchObject({
        code: "forbidden",
      });
    });
  });
});
