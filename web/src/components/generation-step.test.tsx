import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GenerationStep } from "@/components/generation-step";
import { useReportStore } from "@/stores/report-store";

function busyResponse(): Response {
  return new Response(
    JSON.stringify({
      code: "capacity_exhausted",
      error: "report task capacity exhausted",
    }),
    {
      status: 503,
      headers: { "Content-Type": "application/json" },
    },
  );
}

describe("GenerationStep", () => {
  beforeEach(() => {
    const file = new File(["report input"], "collector.zip", {
      type: "application/zip",
    });
    useReportStore.setState({
      token: "token",
      dbType: "mysql",
      zipFiles: [
        {
          id: "zip-1",
          file,
          name: file.name,
          size: file.size,
        },
      ],
      awrFiles: {},
      currentStep: 3,
      taskId: null,
      progress: { completed: 0, total: 0, currentFile: "" },
      logs: [],
      downloadUrl: null,
      isGenerating: false,
      isComplete: false,
      hasError: false,
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("keeps files after a busy response and retries only on the button click", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 404 }))
      .mockResolvedValueOnce(busyResponse())
      .mockResolvedValueOnce(new Response("", { status: 404 }))
      .mockResolvedValueOnce(busyResponse());
    vi.stubGlobal("fetch", fetchMock);

    render(<GenerationStep />);

    const retryButton = await screen.findByRole("button", { name: "手动重试" });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(useReportStore.getState().zipFiles).toHaveLength(1);
    expect(screen.getByText("服务当前任务已满。已保留所选文件，请在稍后手动重试。")).toBeInTheDocument();

    fireEvent.click(retryButton);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(4);
    });
    expect(useReportStore.getState().zipFiles).toHaveLength(1);
  });
});
