"use client";

import { Download, RotateCcw } from "lucide-react";
import { cn } from "@/lib/utils";
import { Progress } from "@/components/ui/progress";
import { LogTerminal } from "@/components/log-terminal";
import type { LogEntry, ProgressState } from "@/lib/types";

interface GenerationProgressProps {
  progress: ProgressState;
  logs: LogEntry[];
  isComplete: boolean;
  hasError: boolean;
  downloadUrl: string | null;
  onReset: () => void;
}

export function GenerationProgress({
  progress,
  logs,
  isComplete,
  hasError,
  downloadUrl,
  onReset,
}: GenerationProgressProps) {
  const pct =
    progress.total > 0
      ? Math.round((progress.completed / progress.total) * 100)
      : 0;

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">
          {isComplete
            ? hasError
              ? "生成完成（存在错误）"
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

      {/* Log terminal */}
      <LogTerminal logs={logs} className="h-72 sm:h-80" />

      {/* Actions */}
      {isComplete && (
        <div className="flex items-center justify-center gap-4">
          {downloadUrl && !hasError && (
            <a
              href={downloadUrl}
              download
              className={cn(
                "inline-flex items-center gap-2 rounded-lg px-6 py-2.5",
                "bg-primary text-primary-foreground font-medium",
                "hover:bg-primary/90 transition-colors duration-200",
                "cursor-pointer",
              )}
            >
              <Download className="h-4 w-4" />
              下载报告
            </a>
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
