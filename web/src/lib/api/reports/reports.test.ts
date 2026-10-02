import { describe, expect, it } from "vitest";
import type { ReportItemInput } from "@/lib/api/contract";
import { contractImplementationsWithReal,watchToEnd, zipFile } from "@/lib/api/testing";
import type { DbType } from "@/lib/types";

function item(name: string, dbType: DbType = "mysql", collectorVersion: string | null = "1.2.0"): ReportItemInput {
  return { zip: zipFile(name, dbType, collectorVersion), dbType, collectorVersion, diagnostics: [] };
}

function isNewestFirst(isoTimes: string[]): boolean {
  return isoTimes.every((at, i) => i === 0 || Date.parse(isoTimes[i - 1]) >= Date.parse(at));
}

describe.each(contractImplementationsWithReal)("%s reports contract", (_name, makeApi) => {
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

  it.each(["lisi", "zhaoliu"])("refuses report downloads to applicant %s", async (username) => {
    const api = makeApi();
    const { token } = await api.auth.signIn(username, username);
    await expect(api.reports.download(token, "t-any")).rejects.toMatchObject({ code: "forbidden" });
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

  it("finishes a task left mid-generation, with nobody watching it", async () => {
    let now = Date.parse("2026-10-01T08:00:00Z");
    const api = makeApi({ now: () => now });
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, { items: [item("a.zip"), item("b.zip")] });
    expect(await api.reports.getTask(token, taskId)).toMatchObject({ status: "processing" });

    now += 10 * 60 * 1000;
    const finished = (await api.reports.listOwn(token)).find((t) => t.id === taskId);
    expect(finished).toMatchObject({ status: "done", items: [{ outcome: { status: "done" } }, { outcome: { status: "done" } }] });
    await expect(api.reports.download(token, taskId)).resolves.toBeInstanceOf(Blob);
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

  it("lists every submitter's tasks to an admin, disabled users included, newest first", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    const admin = await api.auth.signIn("admin", "admin");
    const engineersTask = await api.reports.generate(engineer.token, { items: [item("mall.zip")] });
    const adminsTask = await api.reports.generate(admin.token, { items: [item("core.zip")] });

    const all = await api.reports.listAll(admin.token);
    expect(all.map((t) => t.id)).toEqual(expect.arrayContaining([engineersTask.taskId, adminsTask.taskId]));
    expect(new Set(all.map((t) => t.submitter.id))).toEqual(new Set(["u-user-001", "u-admin-001", "u-disabled-001"]));
    expect(all.find((t) => t.submitter.id === "u-disabled-001")?.submitter.displayName).toBe("王五");
    expect(isNewestFirst(all.map((t) => t.createdAt))).toBe(true);
  });

  it("narrows all tasks to one submitter", async () => {
    const api = makeApi();
    const admin = await api.auth.signIn("admin", "admin");

    const disabledUsers = await api.reports.listAll(admin.token, { submitterId: "u-disabled-001" });
    expect(disabledUsers.map((t) => t.id)).toEqual(["task-seed-009"]);

    const engineers = await api.reports.listAll(admin.token, { submitterId: "u-user-001" });
    expect(engineers.length).toBeGreaterThan(1);
    expect(engineers.every((t) => t.submitter.id === "u-user-001")).toBe(true);
    expect(engineers).toEqual(await api.reports.listOwn((await api.auth.signIn("user", "user")).token));

    expect(await api.reports.listAll(admin.token, { submitterId: "no-such-user" })).toEqual([]);
  });

  it("lists all tasks to admins only", async () => {
    const api = makeApi();
    const engineer = await api.auth.signIn("user", "user");
    await expect(api.reports.listAll(engineer.token)).rejects.toMatchObject({ code: "forbidden" });
    await expect(api.reports.listAll(engineer.token, { submitterId: engineer.user.id })).rejects.toMatchObject({
      code: "forbidden",
    });
    await expect(api.reports.listAll("not-a-session")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it("refuses to list without a valid session", async () => {
    const api = makeApi();
    await expect(api.reports.listOwn("not-a-session")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it("reports an unknown task as not found", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.reports.download(token, "no-such-task")).rejects.toMatchObject({ code: "not_found" });
    await expect(api.reports.getTask(token, "no-such-task")).rejects.toMatchObject({ code: "not_found" });
  });

  it("expires a task's files 30 days after submission and refuses to download them", async () => {
    let now = Date.parse("2026-10-01T08:00:00Z");
    const api = makeApi({ now: () => now });
    const submitted = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(submitted.token, { items: [item("mall.zip")] });
    await watchToEnd(api, submitted.token, taskId);

    now = Date.parse("2026-10-31T07:59:00Z");
    // Sessions last 7 days on the server, so sign in again a month later.
    const { token } = await api.auth.signIn("user", "user");
    const listed = async () => (await api.reports.listOwn(token)).find((t) => t.id === taskId);
    expect(await listed()).toMatchObject({ expired: false });
    await expect(api.reports.download(token, taskId)).resolves.toBeInstanceOf(Blob);

    now = Date.parse("2026-10-31T08:00:00Z");
    expect(await listed()).toMatchObject({ expired: true, status: "done" });
    await expect(api.reports.download(token, taskId)).rejects.toMatchObject({ code: "invalid" });
  });

  it("lists the seeded tasks older than 30 days as expired, and downloads only the others", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const mine = await api.reports.listOwn(token);
    expect(mine.find((t) => t.id === "task-seed-001")).toMatchObject({ status: "done", expired: false });
    expect(mine.find((t) => t.id === "task-seed-006")).toMatchObject({ status: "done", expired: true });

    await expect(api.reports.download(token, "task-seed-001")).resolves.toBeInstanceOf(Blob);
    await expect(api.reports.download(token, "task-seed-006")).rejects.toMatchObject({ code: "invalid" });
  });
});
