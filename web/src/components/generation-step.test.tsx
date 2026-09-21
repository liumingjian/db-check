import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Home from "@/app/page";
import { GenerationStep } from "@/components/generation-step";
import { ReportAPIError } from "@/lib/report-api";
import type { ReportTaskSnapshot } from "@/lib/types";
import { useReportStore } from "@/stores/report-store";

const reportAPIMocks = vi.hoisted(() => ({
  downloadReportBlob: vi.fn(),
  generateReportTask: vi.fn(),
  getReportTaskStatus: vi.fn(),
}));

vi.mock("@/lib/report-api", async () => {
  const actual = await vi.importActual<Record<string, unknown>>("@/lib/report-api");
  return { ...actual, ...reportAPIMocks };
});

class MockWebSocket {
  static instances: MockWebSocket[] = [];

  readonly url: string;
  readyState = 0;
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onclose: ((event: Event) => void) | null = null;

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  close() {
    this.readyState = 3;
    this.onclose?.(new Event("close"));
  }

  open() {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  message(value: unknown) {
    this.onmessage?.(
      new MessageEvent("message", { data: JSON.stringify(value) }),
    );
  }

  static reset() {
    MockWebSocket.instances = [];
  }
}

function snapshot(
  changes: Partial<ReportTaskSnapshot> = {},
): ReportTaskSnapshot {
  return {
    task_id: "task-1",
    status: "processing",
    total: 1,
    completed: 0,
    current_file: "input.zip",
    version: 1,
    items: [
      {
        id: "1",
        name: "input.zip",
        status: "processing",
      },
    ],
    ...changes,
  };
}

function setInitialState(taskId: string | null = null, currentStep: 1 | 2 | 3 = 3) {
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
    currentStep,
    taskId,
    progress: { completed: 0, total: 0, currentFile: "" },
    logs: [],
    downloadUrl: null,
    isGenerating: false,
    isComplete: false,
    hasError: false,
    submissionKey: null,
    submissionFingerprint: null,
  });
}

describe("GenerationStep", () => {
  beforeEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    MockWebSocket.reset();
    vi.stubGlobal("WebSocket", MockWebSocket);
    setInitialState();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("keeps files after a busy response and retries only on the button click", async () => {
    reportAPIMocks.generateReportTask
      .mockRejectedValueOnce(
        new ReportAPIError("report task capacity exhausted", 503, "capacity_exhausted"),
      )
      .mockRejectedValueOnce(
        new ReportAPIError("report task capacity exhausted", 503, "capacity_exhausted"),
      );

    render(<GenerationStep />);

    const retryButton = await screen.findByRole("button", { name: "手动重试" });
    expect(reportAPIMocks.generateReportTask).toHaveBeenCalledTimes(1);
    expect(useReportStore.getState().zipFiles).toHaveLength(1);
    expect(screen.getByText("服务当前任务已满。已保留所选文件，请在稍后手动重试。")).toBeInTheDocument();

    fireEvent.click(retryButton);

    await waitFor(() => {
      expect(reportAPIMocks.generateReportTask).toHaveBeenCalledTimes(2);
    });
    expect(useReportStore.getState().zipFiles).toHaveLength(1);
  });

  it("pauses after a storage fault without discarding selected files or retrying", async () => {
    reportAPIMocks.generateReportTask.mockRejectedValueOnce(
      new ReportAPIError("report task storage is unavailable", 503, "storage_unavailable"),
    );

    render(<GenerationStep />);

    expect(await screen.findByRole("alert", { name: "任务存储故障" })).toBeInTheDocument();
    expect(screen.getByText("报告任务已暂停")).toBeInTheDocument();
    expect(screen.getByText("请由运维人员修复存储后重启服务，再查看任务状态。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "手动重试" })).not.toBeInTheDocument();
    expect(useReportStore.getState().zipFiles).toHaveLength(1);
    expect(reportAPIMocks.generateReportTask).toHaveBeenCalledTimes(1);
  });

  it("keeps persisted progress and terminal state stable while a storage-fault snapshot is active", async () => {
    setInitialState("task-1");
    reportAPIMocks.getReportTaskStatus.mockResolvedValue(
      snapshot({
        total: 2,
        completed: 1,
        current_file: "saved.zip",
        storage_fault: {
          code: "storage_unavailable",
          message: "report task storage is unavailable",
        },
      }),
    );

    render(<GenerationStep />);

    await screen.findByRole("alert", { name: "任务存储故障" });
    expect(screen.getByText("1/2 完成")).toBeInTheDocument();
    expect(MockWebSocket.instances).toHaveLength(1);

    act(() => {
      MockWebSocket.instances[0].message({
        type: "progress",
        seq: 2,
        completed: 2,
        total: 2,
        current_file: "unsaved.zip",
      });
    });

    expect(screen.getByText("1/2 完成")).toBeInTheDocument();
    expect(screen.queryByText("2/2 完成")).not.toBeInTheDocument();

    const statusCallsBeforeDone = reportAPIMocks.getReportTaskStatus.mock.calls.length;
    act(() => {
      MockWebSocket.instances[0].message({
        type: "done",
        seq: 3,
        download_url: "/api/reports/download/task-1",
      });
    });

    expect(reportAPIMocks.getReportTaskStatus).toHaveBeenCalledTimes(statusCallsBeforeDone);
    expect(screen.queryByRole("button", { name: "下载报告" })).not.toBeInTheDocument();
  });

  it("reconciles a storage fault after the watcher closes", async () => {
    vi.useFakeTimers();
    setInitialState("task-1");
    reportAPIMocks.getReportTaskStatus
      .mockResolvedValueOnce(snapshot())
      .mockResolvedValueOnce(
        snapshot({
          storage_fault: {
            code: "storage_unavailable",
            message: "report task storage is unavailable",
          },
        }),
      );

    render(<GenerationStep />);

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(MockWebSocket.instances).toHaveLength(1);

    act(() => {
      MockWebSocket.instances[0].close();
    });
    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByRole("alert", { name: "任务存储故障" })).toBeInTheDocument();
    expect(reportAPIMocks.generateReportTask).not.toHaveBeenCalled();
  });

  it("restores a stored task and its download without submitting selected files", async () => {
    sessionStorage.setItem("dbcheck_task_id", "task-1");
    reportAPIMocks.getReportTaskStatus.mockResolvedValue(
      snapshot({
        status: "done",
        completed: 1,
        current_file: "",
        download_url: "/api/reports/download/task-1",
        items: [
          {
            id: "1",
            name: "input.zip",
            status: "done",
            report_docx: "report.docx",
          },
        ],
      }),
    );

    render(<GenerationStep />);

    await waitFor(() => {
      expect(reportAPIMocks.getReportTaskStatus).toHaveBeenCalledWith("token", "task-1");
    });
    expect(reportAPIMocks.generateReportTask).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "下载报告" })).toBeInTheDocument();
    expect(screen.getByText("input.zip")).toBeInTheDocument();
    expect(
      screen.getByText("实时日志仅供参考，断线期间可能不完整，请以任务状态为准。"),
    ).toBeInTheDocument();
  });

  it("routes a refreshed task into recovery instead of starting an upload", async () => {
    sessionStorage.setItem("dbcheck_task_id", "task-1");
    setInitialState(null, 1);
    reportAPIMocks.getReportTaskStatus.mockResolvedValue(
      snapshot({
        status: "done",
        completed: 1,
        current_file: "",
        download_url: "/api/reports/download/task-1",
      }),
    );

    render(<Home />);

    await waitFor(() => {
      expect(reportAPIMocks.getReportTaskStatus).toHaveBeenCalledWith("token", "task-1");
    });
    expect(useReportStore.getState().currentStep).toBe(3);
    expect(reportAPIMocks.generateReportTask).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "下载报告" })).toBeInTheDocument();
  });

  it("polls while disconnected and reconciles a snapshot after reconnect", async () => {
    vi.useFakeTimers();
    setInitialState("task-1");
    reportAPIMocks.getReportTaskStatus
      .mockResolvedValueOnce(snapshot())
      .mockResolvedValueOnce(snapshot({ version: 2 }))
      .mockResolvedValueOnce(snapshot({ version: 3 }));

    render(<GenerationStep />);

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(MockWebSocket.instances).toHaveLength(1);

    act(() => {
      MockWebSocket.instances[0].close();
    });
    expect(screen.getByText("实时连接已断开，正在查询任务状态。")).toBeInTheDocument();

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(reportAPIMocks.getReportTaskStatus).toHaveBeenCalledTimes(2);
    expect(MockWebSocket.instances).toHaveLength(2);

    act(() => {
      MockWebSocket.instances[1].open();
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(reportAPIMocks.getReportTaskStatus).toHaveBeenCalledTimes(3);

    act(() => {
      MockWebSocket.instances[1].message({
        ...snapshot({
          status: "done",
          completed: 1,
          current_file: "",
          version: 4,
          download_url: "/api/reports/download/task-1",
        }),
        type: "snapshot",
      });
    });

    expect(screen.getByRole("button", { name: "下载报告" })).toBeInTheDocument();
    expect(reportAPIMocks.generateReportTask).not.toHaveBeenCalled();
  });
});
