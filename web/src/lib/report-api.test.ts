import { afterEach, describe, expect, it, vi } from "vitest";
import {
  generateReportTask,
  getReportTaskStatus,
  ReportAPIError,
} from "@/lib/report-api";
import type { ZipFileEntry } from "@/lib/types";

const zipFile: ZipFileEntry = {
  id: "zip-1",
  file: new File(["report input"], "collector.zip", { type: "application/zip" }),
  name: "collector.zip",
  size: 12,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("generateReportTask", () => {
  it("exposes the capacity code from a busy response", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 404 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: "capacity_exhausted",
            error: "report task capacity exhausted",
          }),
          {
            status: 503,
            headers: { "Content-Type": "application/json" },
          },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      generateReportTask("token", "mysql", [zipFile], {}),
    ).rejects.toMatchObject<Partial<ReportAPIError>>({
      code: "capacity_exhausted",
      status: 503,
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1]?.[1]).toMatchObject({
      method: "POST",
      headers: { Authorization: "Bearer token" },
    });
  });

  it("sends the retained submission key on a retry", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 404 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            task_id: "task-1",
            status: "queued",
            total: 1,
            ws_url: "/api/reports/ws/task-1",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await generateReportTask("token", "mysql", [zipFile], {}, " retry-key ");

    expect(fetchMock.mock.calls[1]?.[1]).toMatchObject({
      method: "POST",
      headers: {
        Authorization: "Bearer token",
        "Idempotency-Key": "retry-key",
      },
    });
  });

  it("returns the authoritative task snapshot without dropping future fields", async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          task_id: "task-1",
          status: "processing",
          total: 1,
          completed: 0,
          current_file: "collector.zip",
          version: 4,
          future_field: "kept",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const snapshot = await getReportTaskStatus("token", "task/1");

    expect(snapshot).toMatchObject({
      task_id: "task-1",
      version: 4,
      future_field: "kept",
    });
    expect(fetchMock.mock.calls[0]?.[0]).toContain("task%2F1");
  });
});
