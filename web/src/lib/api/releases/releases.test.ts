import { describe, expect, it } from "vitest";
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
});
