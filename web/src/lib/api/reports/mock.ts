import { requireSessionUser } from "@/lib/api/auth/mock";
import { ApiError, errorMessage } from "@/lib/api/errors";
import { delay, mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import { mockReleaseRecords } from "@/lib/api/releases/mock";
import {
  REPORT_RETENTION_DAYS,
  REPORT_RETENTION_MS,
  type CollectorNotice,
  type ReportEvent,
  type ReportsApi,
  type ReportTask,
} from "@/lib/api/reports/contract";
import { seedReportTasks, type MockReportTask } from "@/lib/api/reports/seed";
import { mockUserRecords } from "@/lib/api/users/mock";
import type { User } from "@/lib/auth-types";
import type { LogLevel } from "@/lib/types";

const SIMULATED_ITEM_LOGS: Array<{ level: LogLevel; message: string }> = [
  { level: "info", message: "解析 manifest.json..." },
  { level: "info", message: "校验 result.json schema..." },
  { level: "success", message: "Schema 校验通过" },
  { level: "info", message: "加载规则文件 rule.json..." },
  { level: "info", message: "执行规则分析..." },
  { level: "success", message: "生成 summary.json ✓" },
  { level: "info", message: "构建 ReportView..." },
  { level: "info", message: "渲染 report.docx..." },
  { level: "success", message: "报告生成完成 ✓" },
];

/**
 * How long the simulated backend takes per report item. It bounds the watched
 * simulation (9 steps of 250 ms), so a task left mid-generation still finishes.
 */
const GENERATION_MS_PER_ITEM = 5_000;

export function createMockReports(ctx: MockContext): ReportsApi {
  const tasks = mockReportTasks(ctx);

  /** The stored tasks, with every processing task past its `finishesAt` recorded as done. */
  function readTasks(): MockReportTask[] {
    const stored = tasks.read();
    const settled = stored.map((t) => (isDue(t) ? finished(t) : t));
    if (settled.some((t, i) => t !== stored[i])) tasks.write(settled);
    return settled;
  }

  function isDue(task: MockReportTask): boolean {
    return task.status === "processing" && task.finishesAt !== undefined && Date.parse(task.finishesAt) <= ctx.now();
  }

  function requireTask(taskId: string): MockReportTask {
    const task = readTasks().find((t) => t.id === taskId);
    if (!task) throw new ApiError("not_found", `报告任务不存在: ${taskId}`);
    return task;
  }

  /** Engineers see only their own tasks; admins see all (CONTEXT.md, Submitter). */
  function requireVisibleTask(caller: User, taskId: string): MockReportTask {
    const task = requireTask(taskId);
    if (caller.role !== "admin" && task.submitterId !== caller.id) {
      throw new ApiError("not_found", `报告任务不存在: ${taskId}`);
    }
    return task;
  }

  function toReportTask({ submitterId, ...task }: MockReportTask): ReportTask {
    const submitter = mockUserRecords(ctx)
      .read()
      .find((u) => u.id === submitterId);
    return {
      ...task,
      submitter: { id: submitterId, displayName: submitter?.displayName ?? submitterId },
      expired: isExpired(task),
      items: task.items.map((item) => ({ ...item, collectorNotice: collectorNotice(item.collectorVersion) })),
    };
  }

  /** Follows the release's current status, whoever reads: engineers see revoked warnings too. */
  function collectorNotice(version: string | null): CollectorNotice | null {
    const release = mockReleaseRecords(ctx)
      .read()
      .find((r) => r.version === version);
    if (release?.status === "deprecated") return { status: "deprecated" };
    if (release?.status === "revoked") return { status: "revoked", reason: release.revokeReason ?? "" };
    return null;
  }

  function isExpired({ createdAt }: { createdAt: string }): boolean {
    return ctx.now() - Date.parse(createdAt) >= REPORT_RETENTION_MS;
  }

  function markDone(taskId: string): void {
    tasks.write(readTasks().map((t) => (t.id === taskId ? finished(t) : t)));
  }

  /** Plays the backend's event stream for a task, then records it as done. */
  async function simulate(task: MockReportTask, emit: (event: ReportEvent) => void, stopped: () => boolean) {
    let seq = 0;
    const fileNames = task.items.map((item) => item.fileName);
    const total = fileNames.length;
    const log = (level: LogLevel, message: string) =>
      emit({ type: "log", seq: ++seq, timestamp: new Date().toISOString(), level, message });

    for (const [index, fileName] of fileNames.entries()) {
      log("info", `开始处理 ${fileName}`);
      for (const step of SIMULATED_ITEM_LOGS) {
        await delay(ctx.stepDelayMs);
        if (stopped()) return;
        log(step.level, `[${fileName}] ${step.message}`);
      }
      emit({ type: "progress", seq: ++seq, completed: index + 1, total, current_file: fileName });
    }
    log("success", total > 1 ? `全部 ${total} 份报告生成完成，正在打包...` : "报告生成完成");
    await delay(ctx.stepDelayMs);
    if (stopped()) return;
    markDone(task.id);
    emit({ type: "done", seq: ++seq, download_url: `/api/reports/download/${task.id}` });
  }

  return {
    async validate(token, input) {
      requireSessionUser(ctx, token);
      if (input.items.length === 0) throw new ApiError("invalid", "请至少上传一个 ZIP 文件");
      return input.items.flatMap((item, i) => item.diagnostics.map((file, k) => ({ itemPosition: i + 1, attachmentPosition: k + 1, fileName: file.name, kind: "checked", evidence: "unconfirmed" })));
    },
    async generate(token, input) {
      const submitter = requireSessionUser(ctx, token);
      if (input.items.length === 0) throw new ApiError("invalid", "请至少上传一个 ZIP 文件");
      if (input.items.some((item) => item.dbType === "oracle" && item.diagnostics.length > 1)) {
        throw new ApiError("invalid", "每个 ZIP 只能添加一份 Oracle AWR 文件");
      }
      const createdAt = ctx.now();
      const task: MockReportTask = {
        id: mockId("mock-task"),
        submitterId: submitter.id,
        items: input.items.map(({ zip, dbType, collectorVersion }) => ({
          fileName: zip.name,
          dbType,
          collectorVersion,
          outcome: { status: "processing" },
        })),
        status: "processing",
        createdAt: new Date(createdAt).toISOString(),
        finishesAt: new Date(createdAt + input.items.length * GENERATION_MS_PER_ITEM).toISOString(),
      };
      tasks.write([task, ...readTasks()]);
      return { taskId: task.id, total: task.items.length };
    },

    async listOwn(token) {
      const caller = requireSessionUser(ctx, token);
      return newestFirst(readTasks().filter((t) => t.submitterId === caller.id)).map(toReportTask);
    },

    async listAll(token, { submitterId } = {}) {
      if (requireSessionUser(ctx, token).role !== "admin") throw new ApiError("forbidden", "只有管理员可以查看全部报告");
      const all = readTasks();
      return newestFirst(submitterId ? all.filter((t) => t.submitterId === submitterId) : all).map(toReportTask);
    },

    async getTask(token, taskId) {
      return toReportTask(requireVisibleTask(requireSessionUser(ctx, token), taskId));
    },

    watch(token, taskId, onEvent) {
      let stopped = false;
      const emit = (event: ReportEvent) => {
        if (!stopped) onEvent(event);
      };
      (async () => {
        try {
          await simulate(requireVisibleTask(requireSessionUser(ctx, token), taskId), emit, () => stopped);
        } catch (e) {
          emit({ type: "error", seq: 0, message: errorMessage(e) });
        }
      })();
      return () => {
        stopped = true;
      };
    },

    async download(token, taskId) {
      const task = requireVisibleTask(requireSessionUser(ctx, token), taskId);
      if (task.status !== "done") throw new ApiError("invalid", "报告尚未生成完成");
      if (isExpired(task)) throw new ApiError("invalid", `报告已超过 ${REPORT_RETENTION_DAYS} 天保留期，文件已清理`);
      const content = [
        "DB-Check 巡检诊断报告集合 (mock)",
        `任务编号: ${task.id}`,
        `包含报告: ${task.items.map((item) => item.fileName).join(", ")}`,
      ].join("\n");
      return new Blob([content], { type: "application/zip" });
    },
  };
}

function finished(task: MockReportTask): MockReportTask {
  return { ...task, status: "done", items: task.items.map((item) => ({ ...item, outcome: { status: "done" } })) };
}

/** Stable, so tasks created in the same millisecond keep their newest-first storage order. */
function newestFirst(tasks: MockReportTask[]): MockReportTask[] {
  return [...tasks].sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));
}

function mockReportTasks(ctx: MockContext) {
  return mockCollection<MockReportTask[]>(ctx.storage, "report_tasks", () => seedReportTasks(ctx.now()));
}
