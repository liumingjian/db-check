import { describe, expect, it } from "vitest";
import type { ReportItemInput } from "@/lib/api/contract";
import { contractImplementations, zipFile } from "@/lib/api/testing";

function item(name: string, collectorVersion: string | null): ReportItemInput {
  return { zip: zipFile(name, "mysql", collectorVersion), dbType: "mysql", collectorVersion, diagnostics: [] };
}

/** Seed releases: 1.3.0-rc1 pre-release, 1.2.0 latest, 1.1.0 deprecated, 1.0.0 revoked. */
const SEED_REVOKE_REASON = "Oracle 采集会在 11g 上锁表，请勿使用";

describe.each(contractImplementations)("%s collector version notices", (_name, makeApi) => {
  it("warns about the seed item from a revoked collector version, with the reason", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");

    const items = (await api.reports.listOwn(token)).flatMap((t) => t.items);
    const revoked = items.filter((i) => i.collectorVersion === "1.0.0");
    expect(revoked.length).toBeGreaterThan(0);
    for (const i of revoked) {
      expect(i.collectorNotice).toEqual({ status: "revoked", reason: SEED_REVOKE_REASON });
    }
  });

  it("gives each submitted item the notice its release status calls for; unknown versions get none", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");

    const { taskId } = await api.reports.generate(token, {
      items: [
        item("latest.zip", "1.2.0"),
        item("deprecated.zip", "1.1.0"),
        item("revoked.zip", "1.0.0"),
        item("pre.zip", "1.3.0-rc1"),
        item("unknown.zip", "0.9.9-dev"),
        item("no-version.zip", null),
      ],
    });

    const task = await api.reports.getTask(token, taskId);
    expect(task.items.map((i) => [i.collectorVersion, i.collectorNotice])).toEqual([
      ["1.2.0", null],
      ["1.1.0", { status: "deprecated" }],
      ["1.0.0", { status: "revoked", reason: SEED_REVOKE_REASON }],
      ["1.3.0-rc1", null],
      ["0.9.9-dev", null],
      [null, null],
    ]);
  });

  it("follows the release's current status: revoking later flags tasks already delivered, restoring clears it", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    const { taskId } = await api.reports.generate(engineer.token, { items: [item("mall.zip", "1.2.0")] });
    const notice = async () => (await api.reports.listOwn(engineer.token)).find((t) => t.id === taskId)?.items[0].collectorNotice;

    expect(await notice()).toBeNull();

    await api.releases.revoke(admin.token, "1.2.0", "  采集结果缺失表空间数据  ");
    expect(await notice()).toEqual({ status: "revoked", reason: "采集结果缺失表空间数据" });
    expect(await api.reports.getTask(admin.token, taskId)).toMatchObject({
      items: [{ collectorNotice: { status: "revoked", reason: "采集结果缺失表空间数据" } }],
    });

    await api.releases.restore(admin.token, "1.2.0");
    expect(await notice()).toEqual({ status: "deprecated" });
  });
});
