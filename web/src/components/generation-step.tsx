"use client";

import { useEffect, useRef, useState } from "react";
import { useReportStore } from "@/stores/report-store";
import { useAuthStore } from "@/stores/auth-store";
import { useHistoryStore } from "@/stores/history-store";
import { useNavStore } from "@/stores/nav-store";
import { GenerationProgress } from "@/components/generation-progress";
import { api, type ReportEvent } from "@/lib/api";
import { ArrowRight, ClipboardList } from "lucide-react";

function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

export function GenerationStep() {
  const zipFiles = useReportStore((s) => s.zipFiles);
  const awrFiles = useReportStore((s) => s.awrFiles);
  const dbType = useReportStore((s) => s.dbType) ?? "mysql";
  const progress = useReportStore((s) => s.progress);
  const logs = useReportStore((s) => s.logs);
  const isComplete = useReportStore((s) => s.isComplete);
  const hasError = useReportStore((s) => s.hasError);
  const downloadUrl = useReportStore((s) => s.downloadUrl);
  const taskId = useReportStore((s) => s.taskId);

  const setGenerating = useReportStore((s) => s.setGenerating);
  const setTaskId = useReportStore((s) => s.setTaskId);
  const setProgress = useReportStore((s) => s.setProgress);
  const addLog = useReportStore((s) => s.addLog);
  const setDownloadUrl = useReportStore((s) => s.setDownloadUrl);
  const setComplete = useReportStore((s) => s.setComplete);
  const setHasError = useReportStore((s) => s.setHasError);
  const reset = useReportStore((s) => s.reset);

  const token = useAuthStore((s) => s.token);
  const addTask = useHistoryStore((s) => s.addTask);
  const setActiveTab = useNavStore((s) => s.setActiveTab);

  const startedRef = useRef(false);
  const lastLogSeqRef = useRef<number>(0);
  const [isDownloading, setDownloading] = useState(false);

  useEffect(() => {
    if (startedRef.current || !token) return;
    startedRef.current = true;

    const total = zipFiles.length;
    const fileNames = zipFiles.map((z) => z.name);
    setGenerating(true);
    setProgress({ completed: 0, total, currentFile: "" });

    let cancelled = false;
    let stopWatching: (() => void) | null = null;

    function fail(message: string, taskId: string) {
      addLog({ id: generateLogId(), timestamp: new Date().toISOString(), level: "error", message });
      setGenerating(false);
      setHasError(true);
      setComplete(true);
      addTask({ id: taskId, createdAt: new Date().toISOString(), totalFiles: total, fileNames, status: "failed" });
    }

    function handleEvent(event: ReportEvent, taskId: string) {
      switch (event.type) {
        case "log":
          if (event.seq <= lastLogSeqRef.current) return;
          lastLogSeqRef.current = event.seq;
          addLog({ id: generateLogId(), timestamp: event.timestamp, level: event.level, message: event.message });
          break;
        case "progress":
          setProgress({ completed: event.completed, total: event.total, currentFile: event.current_file });
          break;
        case "done":
          setDownloadUrl(event.download_url);
          setGenerating(false);
          setComplete(true);
          addTask({ id: taskId, createdAt: new Date().toISOString(), totalFiles: total, fileNames, status: "success" });
          break;
        case "error":
          fail(event.message, taskId);
          break;
      }
    }

    api.reports
      .generate(token, {
        dbType,
        items: zipFiles.map((z) => ({ zip: z.file, diagnostics: awrFiles[z.id] ?? [] })),
      })
      .then(
        ({ taskId: id }) => {
          if (cancelled) return;
          setTaskId(id);
          stopWatching = api.reports.watch(token, id, (event) => handleEvent(event, id));
        },
        (e: unknown) => {
          if (!cancelled) fail(e instanceof Error ? e.message : String(e), `failed-${Date.now()}`);
        },
      );

    return () => {
      cancelled = true;
      stopWatching?.();
    };
  }, [addTask, awrFiles, dbType, setComplete, setDownloadUrl, setGenerating, setHasError, setProgress, setTaskId, token, zipFiles, addLog]);

  async function onDownload() {
    if (!token || !taskId) return;
    setDownloading(true);
    try {
      const blob = await api.reports.download(token, taskId);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `reports-${taskId}.zip`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      addLog({
        id: generateLogId(),
        timestamp: new Date().toISOString(),
        level: "error",
        message: e instanceof Error ? e.message : String(e),
      });
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div className="space-y-6">
      <GenerationProgress
        progress={progress}
        logs={logs}
        isComplete={isComplete}
        hasError={hasError}
        downloadUrl={downloadUrl}
        isDownloading={isDownloading}
        onDownload={downloadUrl ? onDownload : null}
        onReset={reset}
      />

      {isComplete && (
        <div className="flex flex-col sm:flex-row items-center justify-between gap-3 rounded-lg border border-border bg-card p-4 text-xs">
          <span className="text-muted-foreground">
            任务结果已自动保存至系统记录，支持随时调阅历史日志与重新下载。
          </span>
          <button
            type="button"
            onClick={() => setActiveTab("history")}
            className="inline-flex items-center gap-1.5 font-medium text-primary hover:underline cursor-pointer shrink-0"
          >
            <ClipboardList className="h-4 w-4" />
            前往任务记录查看
            <ArrowRight className="h-3.5 w-3.5" />
          </button>
        </div>
      )}
    </div>
  );
}
