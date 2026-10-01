import { describe, expect, it } from "vitest";
import type { DbCheckApi } from "@/lib/api/contract";
import { contractImplementations } from "@/lib/api/testing";

describe.each(contractImplementations)("%s releases contract", (_name, makeApi) => {
  it("shows an engineer exactly the latest and deprecated seed releases, newest first", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const releases = await api.releases.list(token);
    expect(releases.map((r) => [r.version, r.status])).toEqual([
      ["1.2.0", "latest"],
      ["1.1.0", "deprecated"],
    ]);
  });

  it("shows an admin every seed release, pre-release and revoked included", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("admin", "admin");
    const releases = await api.releases.list(token);
    expect(releases.map((r) => [r.version, r.status])).toEqual([
      ["1.3.0-rc1", "pre-release"],
      ["1.2.0", "latest"],
      ["1.1.0", "deprecated"],
      ["1.0.0", "revoked"],
    ]);
    expect(releases.find((r) => r.status === "revoked")?.revokeReason).toBeTruthy();
  });

  it("ships every release as four .zip packages, one per platform", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("admin", "admin");
    for (const release of await api.releases.list(token)) {
      expect(release.packages.map((p) => p.platform)).toEqual([
        "linux-amd64",
        "linux-arm64",
        "windows-amd64",
        "windows-arm64",
      ]);
      for (const pkg of release.packages) {
        expect(pkg.fileName).toMatch(/\.zip$/);
        expect(pkg.sha256).toMatch(/^[0-9a-f]{64}$/);
        expect(pkg.size).toBeGreaterThan(0);
      }
    }
  });

  it("carries notes and supported database types on each release", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const [latest] = await api.releases.list(token);
    expect(latest.notes).not.toBe("");
    expect(latest.dbTypes).toEqual(["mysql", "oracle", "gaussdb"]);
  });

  it("refuses to list releases without a valid session", async () => {
    const api = makeApi();
    await expect(api.releases.list("not-a-session")).rejects.toMatchObject({ code: "unauthorized" });
  });

  describe("release status actions", () => {
    async function admin(api: DbCheckApi) {
      return (await api.auth.signIn("admin", "admin")).token;
    }

    async function statuses(api: DbCheckApi, token: string) {
      return Object.fromEntries((await api.releases.list(token)).map((r) => [r.version, r.status]));
    }

    it("promotes a pre-release to latest and turns the previous latest into deprecated", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.promote(token, "1.3.0-rc1");
      expect(await statuses(api, token)).toEqual({
        "1.3.0-rc1": "latest",
        "1.2.0": "deprecated",
        "1.1.0": "deprecated",
        "1.0.0": "revoked",
      });
    });

    it("promotes a deprecated release back to latest as the rollback path", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.promote(token, "1.1.0");
      expect(await statuses(api, token)).toMatchObject({ "1.1.0": "latest", "1.2.0": "deprecated" });
    });

    it("deprecates a pre-release", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.deprecate(token, "1.3.0-rc1");
      expect(await statuses(api, token)).toMatchObject({ "1.3.0-rc1": "deprecated", "1.2.0": "latest" });
    });

    it("leaves engineers with no latest release after the latest is deprecated, older ones still listed", async () => {
      const api = makeApi();
      await api.releases.deprecate(await admin(api), "1.2.0");
      const { token } = await api.auth.signIn("user", "user");
      expect(await statuses(api, token)).toEqual({ "1.2.0": "deprecated", "1.1.0": "deprecated" });
    });

    it("revokes the latest release with a reason, leaving no latest release and hiding it from engineers", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.revoke(token, "1.2.0", "  RAC 采集崩溃  ");
      const revoked = (await api.releases.list(token)).find((r) => r.version === "1.2.0");
      expect(revoked).toMatchObject({ status: "revoked", revokeReason: "RAC 采集崩溃" });
      const engineer = (await api.auth.signIn("user", "user")).token;
      expect(await statuses(api, engineer)).toEqual({ "1.1.0": "deprecated" });
    });

    it("revokes a pre-release and a deprecated release", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.revoke(token, "1.3.0-rc1", "构建产物损坏");
      await api.releases.revoke(token, "1.1.0", "已知锁表问题");
      expect(await statuses(api, token)).toMatchObject({ "1.3.0-rc1": "revoked", "1.1.0": "revoked" });
    });

    it("refuses to revoke without a reason and changes nothing", async () => {
      const api = makeApi();
      const token = await admin(api);
      await expect(api.releases.revoke(token, "1.2.0", "   ")).rejects.toMatchObject({ code: "invalid" });
      expect(await statuses(api, token)).toMatchObject({ "1.2.0": "latest" });
    });

    it("restores a revoked release to deprecated and drops its revocation reason", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.restore(token, "1.0.0");
      const restored = (await api.releases.list(token)).find((r) => r.version === "1.0.0");
      expect(restored?.status).toBe("deprecated");
      expect(restored?.revokeReason).toBeUndefined();
    });

    it.each([
      ["promote", "1.2.0"],
      ["promote", "1.0.0"],
      ["deprecate", "1.0.0"],
      ["revoke", "1.0.0"],
      ["restore", "1.2.0"],
      ["restore", "1.1.0"],
    ] as const)("refuses to %s release %s, which the status table does not allow", async (action, version) => {
      const api = makeApi();
      const token = await admin(api);
      const before = await statuses(api, token);
      const act = {
        promote: () => api.releases.promote(token, version),
        deprecate: () => api.releases.deprecate(token, version),
        revoke: () => api.releases.revoke(token, version, "原因"),
        restore: () => api.releases.restore(token, version),
      };
      await expect(act[action]()).rejects.toMatchObject({ code: "invalid" });
      expect(await statuses(api, token)).toEqual(before);
    });

    it("never leaves more than one latest release", async () => {
      const api = makeApi();
      const token = await admin(api);
      await api.releases.promote(token, "1.3.0-rc1");
      await api.releases.restore(token, "1.0.0");
      await api.releases.promote(token, "1.0.0");
      await api.releases.promote(token, "1.2.0");
      const latest = (await api.releases.list(token)).filter((r) => r.status === "latest");
      expect(latest.map((r) => r.version)).toEqual(["1.2.0"]);
    });

    it("refuses an unknown release", async () => {
      const api = makeApi();
      await expect(api.releases.promote(await admin(api), "9.9.9")).rejects.toMatchObject({ code: "not_found" });
    });

    it("refuses status actions from an engineer", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("user", "user");
      await expect(api.releases.deprecate(token, "1.2.0")).rejects.toMatchObject({ code: "forbidden" });
      await expect(api.releases.revoke(token, "1.2.0", "原因")).rejects.toMatchObject({ code: "forbidden" });
      expect(await statuses(api, await admin(api))).toMatchObject({ "1.2.0": "latest" });
    });
  });
});
