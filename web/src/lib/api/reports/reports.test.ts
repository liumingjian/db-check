import { describe, expect, it } from "vitest";
import type { ReportItemInput } from "@/lib/api/contract";
import { contractImplementations, watchToEnd, zipFile } from "@/lib/api/testing";
import type { DbType } from "@/lib/types";

function item(name: string, dbType: DbType = "mysql", collectorVersion: string | null = "1.2.0"): ReportItemInput {
  return { zip: zipFile(name), dbType, collectorVersion, diagnostics: [] };
}

function isNewestFirst(isoTimes: string[]): boolean {
  return isoTimes.every((at, i) => i === 0 || Date.parse(isoTimes[i - 1]) >= Date.parse(at));
}

describe.each(contractImplementations)("%s reports contract", (_name, makeApi) => {
  it("refuses to generate without a valid session", async () => {
    const api = makeApi();
    await expect(api.reports.generate("not-a-session", { items: [item("mysql-prod-01.zip")] })).rejects.toMatchObject({
      code: "unauthorized",
    });
  });

  it("generates a report task and streams it to a downloadable result", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");

    const submitted = await api.reports.generate(token, {
      items: [
        { ...item("oracle-a.zip", "oracle"), diagnostics: [new File(["<html>"], "awr-a.html")] },
        item("oracle-b.zip", "oracle"),
      ],
    });
    expect(submitted.total).toBe(2);

    const events = await watchToEnd(api, token, submitted.taskId);
    const progress = events.filter((e) => e.type === "progress");
    expect(progress.at(-1)).toMatchObject({ completed: 2, total: 2 });
    expect(events.at(-1)?.type).toBe("done");

    const report = await api.reports.download(token, submitted.taskId);
    expect(report.size).toBeGreaterThan(0);
  });

  it("records the task under the signed-in submitter, with each item's database type and collector version", async () => {
    const api = makeApi();
    const { token, user } = await api.auth.signIn("user", "user");

    const { taskId } = await api.reports.generate(token, {
      items: [item("ora-01.zip", "oracle", "1.1.0"), item("gauss-01.zip", "gaussdb", null), item("mall.zip", "mysql")],
    });

    const task = await api.reports.getTask(token, taskId);
    expect(task).toMatchObject({
      id: taskId,
      submitter: { id: user.id, displayName: user.displayName },
      status: "processing",
      items: [
        { fileName: "ora-01.zip", dbType: "oracle", collectorVersion: "1.1.0" },
        { fileName: "gauss-01.zip", dbType: "gaussdb", collectorVersion: null },
        { fileName: "mall.zip", dbType: "mysql", collectorVersion: "1.2.0" },
      ],
    });

    await watchToEnd(api, token, taskId);
    expect(await api.reports.getTask(token, taskId)).toMatchObject({ status: "done" });
  });

  it("records each item's outcome: processing while generating, done once finished", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, { items: [item("a.zip"), item("b.zip")] });

    const generating = await api.reports.getTask(token, taskId);
    expect(generating.items.map((i) => i.outcome)).toEqual([{ status: "processing" }, { status: "processing" }]);

    await watchToEnd(api, token, taskId);
    const finished = await api.reports.getTask(token, taskId);
    expect(finished.items.map((i) => i.outcome)).toEqual([{ status: "done" }, { status: "done" }]);
  });

  it("shows a task to its submitter and admins only", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(engineer.token, { items: [item("mall.zip")] });

    const admin = await api.auth.signIn("admin", "admin");
    const adminsTask = await api.reports.generate(admin.token, { items: [item("core.zip")] });

    await expect(api.reports.getTask(admin.token, taskId)).resolves.toMatchObject({ id: taskId });
    await expect(api.reports.getTask(engineer.token, adminsTask.taskId)).rejects.toMatchObject({ code: "not_found" });
  });

  it("lists only the caller's own tasks, newest first, admins included", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    const first = await api.reports.generate(engineer.token, { items: [item("first.zip")] });
    const adminsTask = await api.reports.generate(admin.token, { items: [item("core.zip")] });
    const second = await api.reports.generate(engineer.token, { items: [item("second.zip")] });

    const mine = await api.reports.listOwn(engineer.token);
    expect(mine.map((t) => t.id).slice(0, 2)).toEqual([second.taskId, first.taskId]);
    expect(mine.every((t) => t.submitter.id === engineer.user.id)).toBe(true);
    expect(mine.map((t) => t.id)).not.toContain(adminsTask.taskId);
    expect(isNewestFirst(mine.map((t) => t.createdAt))).toBe(true);

    const adminsOwn = await api.reports.listOwn(admin.token);
    expect(adminsOwn.map((t) => t.id)).toContain(adminsTask.taskId);
    expect(adminsOwn.every((t) => t.submitter.id === admin.user.id)).toBe(true);
  });

  it("expires a task's files 30 days after submission and refuses to download them", async () => {
    let now = Date.parse("2026-10-01T08:00:00Z");
    const api = makeApi({ now: () => now });
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, { items: [item("mall.zip")] });
    await watchToEnd(api, token, taskId);
    const listed = async () => (await api.reports.listOwn(token)).find((t) => t.id === taskId);

    now = Date.parse("2026-10-31T07:59:00Z");
    expect(await listed()).toMatchObject({ expired: false });
    await expect(api.reports.download(token, taskId)).resolves.toBeInstanceOf(Blob);

    now = Date.parse("2026-10-31T08:00:00Z");
    expect(await listed()).toMatchObject({ expired: true, status: "done" });
    await expect(api.reports.download(token, taskId)).rejects.toMatchObject({ code: "invalid" });
  });

  it("refuses to list without a valid session", async () => {
    const api = makeApi();
    await expect(api.reports.listOwn("not-a-session")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it("refuses an empty submission", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.reports.generate(token, { items: [] })).rejects.toMatchObject({ code: "invalid" });
  });

  it("refuses to download a task that is still generating", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, { items: [item("mysql-prod-01.zip")] });
    await expect(api.reports.download(token, taskId)).rejects.toMatchObject({ code: "invalid" });
  });

  it("reports an unknown task as not found", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.reports.download(token, "no-such-task")).rejects.toMatchObject({ code: "not_found" });
    await expect(api.reports.getTask(token, "no-such-task")).rejects.toMatchObject({ code: "not_found" });
  });
});
