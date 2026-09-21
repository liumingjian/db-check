"use client";

import { Download, RotateCcw } from "lucide-react";
import { cn } from "@/lib/utils";
import { Progress } from "@/components/ui/progress";
import { LogTerminal } from "@/components/log-terminal";
import type {
  LogEntry,
  ProgressState,
  ReportTaskSnapshot,
  StorageFault,
} from "@/lib/types";

interface GenerationProgressProps {
  progress: ProgressState;
  logs: LogEntry[];
  isComplete: boolean;
  hasError: boolean;
  downloadUrl: string | null;
  isDownloading: boolean;
  onDownload: (() => void) | null;
  busyMessage: string | null;
  onRetry: (() => void) | null;
  onReset: () => void;
  taskItems?: ReportTaskSnapshot["items"];
  taskError?: string;
  succeededCount?: number;
  failedCount?: number;
  onRetryFailed: (() => void) | null;
  isLiveDisconnected?: boolean;
  storageFault?: StorageFault | null;
}

const itemStatusLabel: Record<string, string> = {
  queued: "等待处理",
  processing: "处理中",
  done: "已完成",
  failed: "失败",
};

export function GenerationProgress({
  progress,
  logs,
  isComplete,
  hasError,
  downloadUrl,
  isDownloading,
  onDownload,
  busyMessage,
  onRetry,
  onReset,
  taskItems,
  taskError,
  succeededCount,
  failedCount,
  onRetryFailed,
  isLiveDisconnected = false,
  storageFault = null,
}: GenerationProgressProps) {
  const pct =
    progress.total > 0
      ? Math.round((progress.completed / progress.total) * 100)
      : 0;
  const hasOutcomeCounts =
    succeededCount !== undefined && failedCount !== undefined;
  const isPartialSuccess =
    isComplete &&
    !hasError &&
    succeededCount !== undefined &&
    failedCount !== undefined &&
    succeededCount > 0 &&
    failedCount > 0;

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">
          {storageFault
            ? "报告任务已暂停"
            : busyMessage
            ? "服务当前任务已满"
            : isPartialSuccess
            ? "报告生成完成（部分成功）"
            : isComplete
            ? hasError
              ? "报告生成失败"
              : "报告生成完成"
            : "报告生成中..."}
        </h2>
        <span className="text-sm text-muted-foreground">
          {progress.completed}/{progress.total} 完成
        </span>
      </div>

      {/* Progress bar */}
      <Progress
        value={pct}
        className={cn(
          "h-2",
          isComplete && !hasError && "[&>div]:bg-primary",
          hasError && "[&>div]:bg-destructive",
        )}
      />

      {storageFault && (
        <section
          className="border-l-4 border-destructive bg-muted px-4 py-3 text-sm"
          role="alert"
          aria-label="任务存储故障"
        >
          <p className="font-medium">任务存储暂时不可用，报告处理已暂停。</p>
          <p className="mt-1 text-muted-foreground">
            请由运维人员修复存储后重启服务，再查看任务状态。
          </p>
          <p className="mt-1 text-muted-foreground">{storageFault.message}</p>
        </section>
      )}

      {/* Log terminal */}
      <LogTerminal logs={logs} className="h-72 sm:h-80" />
      <p className="text-xs text-muted-foreground" role="note">
        实时日志仅供参考，断线期间可能不完整，请以任务状态为准。
      </p>

      {isLiveDisconnected && !isComplete && !storageFault && (
        <p className="text-sm text-muted-foreground" role="status">
          实时连接已断开，正在查询任务状态。
        </p>
      )}

      {isComplete && hasOutcomeCounts && (
        <p className="text-sm" aria-label="任务结果统计">
          <span className="text-primary">成功 {succeededCount}</span>
          <span className="text-muted-foreground">，</span>
          <span className={failedCount > 0 ? "text-destructive" : "text-muted-foreground"}>
            失败 {failedCount}
          </span>
        </p>
      )}

      {isComplete && taskError && (
        <p className="text-sm text-destructive" role="status">
          {taskError}
        </p>
      )}

      {taskItems && taskItems.length > 0 && (
        <section className="space-y-2" aria-label="任务文件状态">
          <h3 className="text-sm font-medium">任务文件状态</h3>
          <ul className="divide-y divide-border border-y border-border text-sm">
            {taskItems.map((item) => (
              <li key={item.id} className="flex flex-wrap items-center gap-x-3 gap-y-1 py-2">
                <span className="min-w-0 flex-1 break-all">{item.name}</span>
                <span className="text-muted-foreground">
                  {itemStatusLabel[item.status] ?? item.status}
                </span>
                {item.error && (
                  <span className="basis-full text-xs text-destructive">{item.error}</span>
                )}
              </li>
            ))}
          </ul>
        </section>
      )}

      {busyMessage && (
        <div className="flex flex-col items-center gap-3 text-center" role="status">
          <p className="text-sm text-muted-foreground">{busyMessage}</p>
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              className={cn(
                "inline-flex items-center gap-2 rounded-lg border border-border px-6 py-2.5",
                "font-medium hover:bg-muted transition-colors duration-200 cursor-pointer",
              )}
            >
              <RotateCcw className="h-4 w-4" />
              手动重试
            </button>
          )}
        </div>
      )}

      {/* Actions */}
      {isComplete && (
        <div className="flex items-center justify-center gap-4">
          {downloadUrl && !hasError && onDownload && (
            <button
              type="button"
              onClick={onDownload}
              disabled={isDownloading}
              className={cn(
                "inline-flex items-center gap-2 rounded-lg px-6 py-2.5",
                "bg-primary text-primary-foreground font-medium",
                "hover:bg-primary/90 transition-colors duration-200",
                "cursor-pointer disabled:opacity-60 disabled:cursor-not-allowed",
              )}
            >
              <Download className="h-4 w-4" />
              {isDownloading ? "下载中..." : "下载报告"}
            </button>
          )}
          {failedCount !== undefined && failedCount > 0 && onRetryFailed && !storageFault && (
            <button
              type="button"
              onClick={onRetryFailed}
              className={cn(
                "inline-flex items-center gap-2 rounded-lg border border-border px-6 py-2.5",
                "font-medium hover:bg-muted transition-colors duration-200 cursor-pointer",
              )}
            >
              <RotateCcw className="h-4 w-4" />
              重新提交失败文件
            </button>
          )}
          <button
            type="button"
            onClick={onReset}
            className={cn(
              "inline-flex items-center gap-2 rounded-lg px-6 py-2.5",
              "border border-border font-medium",
              "hover:bg-muted transition-colors duration-200",
              "cursor-pointer",
            )}
          >
            <RotateCcw className="h-4 w-4" />
            重新开始
          </button>
        </div>
      )}
    </div>
  );
}
