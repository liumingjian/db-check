import { describe, expect, it } from "vitest";
import { contractImplementations, watchToEnd, zipFile } from "@/lib/api/testing";

describe.each(contractImplementations)("%s reports contract", (_name, makeApi) => {
  it("refuses to generate without a valid session", async () => {
    const api = makeApi();
    await expect(
      api.reports.generate("not-a-session", {
        dbType: "mysql",
        items: [{ zip: zipFile("mysql-prod-01.zip"), diagnostics: [] }],
      }),
    ).rejects.toMatchObject({ code: "unauthorized" });
  });

  it("generates a report task and streams it to a downloadable result", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");

    const submitted = await api.reports.generate(token, {
      dbType: "oracle",
      items: [
        { zip: zipFile("oracle-a.zip"), diagnostics: [new File(["<html>"], "awr-a.html")] },
        { zip: zipFile("oracle-b.zip"), diagnostics: [] },
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

  it("refuses an empty submission", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.reports.generate(token, { dbType: "mysql", items: [] })).rejects.toMatchObject({
      code: "invalid",
    });
  });

  it("refuses to download a task that is still generating", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    const { taskId } = await api.reports.generate(token, {
      dbType: "mysql",
      items: [{ zip: zipFile("mysql-prod-01.zip"), diagnostics: [] }],
    });
    await expect(api.reports.download(token, taskId)).rejects.toMatchObject({ code: "invalid" });
  });

  it("reports an unknown task as not found", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.reports.download(token, "no-such-task")).rejects.toMatchObject({ code: "not_found" });
  });
});
