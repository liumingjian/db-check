import { afterEach, describe, expect, it, vi } from "vitest";
import { generateReportTask, ReportAPIError } from "@/lib/report-api";
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
});
