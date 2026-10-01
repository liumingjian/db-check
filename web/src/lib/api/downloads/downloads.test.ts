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

  it("lets an admin download a revoked release", async () => {
    const api = makeApi();
    const admin = await api.auth.signIn("admin", "admin");
    await api.downloads.download(admin.token, "1.0.0", "linux-arm64");
    expect((await api.downloads.records(admin.token))[0]).toMatchObject({
      userId: admin.user.id,
      version: "1.0.0",
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
});
