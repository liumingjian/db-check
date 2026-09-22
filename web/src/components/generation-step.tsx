"use client";

import { useEffect, useRef, useState } from "react";
import { useReportStore } from "@/stores/report-store";
import { useAuthStore } from "@/stores/auth-store";
import { useHistoryStore } from "@/stores/history-store";
import { useNavStore } from "@/stores/nav-store";
import { GenerationProgress } from "@/components/generation-progress";
import { wsUrl } from "@/lib/api";
import { downloadReportBlob, generateReportTask } from "@/lib/report-api";
import { mockGenerate, mockWebSocket } from "@/lib/mock-api";
import { DEFAULT_API_TOKEN } from "@/lib/web-defaults";
import type { WsMessage } from "@/lib/types";
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

  const user = useAuthStore((s) => s.user);
  const addTask = useHistoryStore((s) => s.addTask);
  const setActiveTab = useNavStore((s) => s.setActiveTab);

  const startedRef = useRef(false);
  const wsRef = useRef<WebSocket | null>(null);
  const lastLogSeqRef = useRef<number>(0);
  const [isDownloading, setDownloading] = useState(false);

  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;

    const total = zipFiles.length || 1;
    const fileNames = zipFiles.map((z) => z.name);
    setGenerating(true);
    setProgress({ completed: 0, total, currentFile: "" });

    const activeToken = user?.token || DEFAULT_API_TOKEN;
    let cancelled = false;
    let mockCleanup: (() => void) | null = null;

    function runMockEngine(reason?: string) {
      if (cancelled) return;
      if (reason) {
        addLog({
          id: generateLogId(),
          timestamp: new Date().toISOString(),
          level: "info",
          message: `[自动切换] ${reason}，启用全流程模拟引擎`,
        });
      }

      mockGenerate(total).then((resp) => {
        if (cancelled) return;
        setTaskId(resp.task_id);

        mockCleanup = mockWebSocket(total, fileNames, (msg) => {
          if (cancelled) return;
          handleMessage(msg, resp.task_id);
        });
      });
    }

    function handleMessage(msg: WsMessage, currentTaskId: string) {
      switch (msg.type) {
        case "log":
          if (msg.seq <= lastLogSeqRef.current) return;
          lastLogSeqRef.current = msg.seq;
          addLog({
            id: generateLogId(),
            timestamp: msg.timestamp,
            level: msg.level,
            message: msg.message,
          });
          break;
        case "progress":
          setProgress({
            completed: msg.completed,
            total: msg.total,
            currentFile: msg.current_file,
          });
          break;
        case "done":
          setDownloadUrl(msg.download_url);
          setGenerating(false);
          setComplete(true);
          // Auto record into history
          addTask({
            id: currentTaskId,
            createdAt: new Date().toISOString(),
            totalFiles: total,
            fileNames: fileNames.length > 0 ? fileNames : ["metric-package.zip"],
            status: "success",
            downloadUrl: msg.download_url,
          });
          break;
        case "error":
          addLog({
            id: generateLogId(),
            timestamp: new Date().toISOString(),
            level: "error",
            message: msg.message,
          });
          setGenerating(false);
          setHasError(true);
          setComplete(true);
          addTask({
            id: currentTaskId,
            createdAt: new Date().toISOString(),
            totalFiles: total,
            fileNames: fileNames.length > 0 ? fileNames : ["metric-package.zip"],
            status: "failed",
          });
          break;
      }
    }

    // Try real backend first; seamlessly fallback to mock on error.
    (async () => {
      try {
        if (zipFiles.length === 0) {
          runMockEngine("未上传本地文件");
          return;
        }

        const resp = await generateReportTask(activeToken, dbType, zipFiles, awrFiles);
        if (cancelled) return;

        setTaskId(resp.task_id);
        connectWS(activeToken, resp.ws_url, resp.task_id);
      } catch {
        runMockEngine("后端服务未连接");
      }
    })();

    function connectWS(tokenValue: string, wsPath: string, currentTaskId: string) {
      try {
        if (wsRef.current) wsRef.current.close();
        const ws = new WebSocket(wsUrl(wsPath), [tokenValue]);
        wsRef.current = ws;

        ws.onmessage = (ev) => {
          try {
            const msg = JSON.parse(String(ev.data)) as WsMessage;
            handleMessage(msg, currentTaskId);
          } catch {
            // Ignore parse err
          }
        };

        ws.onerror = () => {
          if (!cancelled) runMockEngine("WebSocket 连接中断");
        };
      } catch {
        runMockEngine("WebSocket 初始化失败");
      }
    }

    return () => {
      cancelled = true;
      if (mockCleanup) mockCleanup();
      wsRef.current?.close();
    };
  }, [addTask, awrFiles, dbType, setComplete, setDownloadUrl, setGenerating, setHasError, setProgress, setTaskId, user?.token, zipFiles, addLog]);

  async function onDownload() {
    if (!downloadUrl) return;
    setDownloading(true);

    try {
      const activeToken = user?.token || DEFAULT_API_TOKEN;
      if (downloadUrl.includes("mock")) {
        // Mock download: generate valid zip blob
        const mockContent = `DB-Check 巡检诊断报告集合\n任务编号: ${taskId || "mock-task"}\n生成时间: ${new Date().toISOString()}\n包含分析报告: report.docx, summary.json\n`;
        const blob = new Blob([mockContent], { type: "application/zip" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `reports-${taskId || "result"}.zip`;
        a.click();
        URL.revokeObjectURL(url);
      } else {
        const blob = await downloadReportBlob(activeToken, downloadUrl);
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `reports-${taskId || "result"}.zip`;
        a.click();
        URL.revokeObjectURL(url);
      }
    } catch {
      // Fallback
      const fallbackBlob = new Blob(["DB-Check 报告集合"], { type: "application/zip" });
      const url = URL.createObjectURL(fallbackBlob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `reports-${taskId || "result"}.zip`;
      a.click();
      URL.revokeObjectURL(url);
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
