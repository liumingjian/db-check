"use client";

import { useEffect, useRef, useState } from "react";
import { useReportStore } from "@/stores/report-store";
import { GenerationProgress } from "@/components/generation-progress";
import { getApiBase, setApiBase } from "@/lib/api";
import {
  downloadReportBlob,
  generateReportTask,
  REPORT_API_ERROR_CODES,
  isReportAPIErrorCode,
} from "@/lib/report-api";
import { API_BASE_HINT, DEFAULT_API_TOKEN } from "@/lib/web-defaults";
import type { StorageFault } from "@/lib/types";
import { useTaskMonitor } from "@/hooks/use-task-monitor";

const TOKEN_STORAGE_KEY = "dbcheck_api_token";
const TASK_STORAGE_KEY = "dbcheck_task_id";
function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

export function GenerationStep() {
  const zipFiles = useReportStore((s) => s.zipFiles);
  const awrFiles = useReportStore((s) => s.awrFiles);
  const dbType = useReportStore((s) => s.dbType);
  const taskId = useReportStore((s) => s.taskId);
  const progress = useReportStore((s) => s.progress);
  const logs = useReportStore((s) => s.logs);
  const isComplete = useReportStore((s) => s.isComplete);
  const hasError = useReportStore((s) => s.hasError);
  const downloadUrl = useReportStore((s) => s.downloadUrl);
  const token = useReportStore((s) => s.token);

  const setGenerating = useReportStore((s) => s.setGenerating);
  const setTaskId = useReportStore((s) => s.setTaskId);
  const setProgress = useReportStore((s) => s.setProgress);
  const addLog = useReportStore((s) => s.addLog);
  const setComplete = useReportStore((s) => s.setComplete);
  const setHasError = useReportStore((s) => s.setHasError);
  const ensureSubmissionKey = useReportStore((s) => s.ensureSubmissionKey);
  const beginNewTask = useReportStore((s) => s.beginNewTask);
  const reset = useReportStore((s) => s.reset);
  const setToken = useReportStore((s) => s.setToken);

  const startedRef = useRef(false);

  const [apiBaseInput, setApiBaseInput] = useState("");
  const [tokenInput, setTokenInput] = useState(DEFAULT_API_TOKEN);
  const [isDownloading, setDownloading] = useState(false);
  const [busyMessage, setBusyMessage] = useState<string | null>(null);
  const [retryAttempt, setRetryAttempt] = useState(0);
  const [storageHydrated, setStorageHydrated] = useState(false);
  const {
    isLiveDisconnected,
    resetTaskMonitor,
    setTaskStorageFault,
    storageFault,
    taskSnapshot,
  } = useTaskMonitor({ storageHydrated, token, taskId });

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!token) {
      const saved = sessionStorage.getItem(TOKEN_STORAGE_KEY);
      if (saved) {
        setTokenInput(saved);
        setToken(saved);
      }
    }
    if (!taskId) {
      const savedTaskID = sessionStorage.getItem(TASK_STORAGE_KEY);
      if (savedTaskID) {
        setTaskId(savedTaskID);
      }
    }
    setStorageHydrated(true);
  }, [setTaskId, setToken, taskId, token]);

  useEffect(() => {
    if (typeof window === "undefined" || apiBaseInput) return;
    setApiBaseInput(getApiBase());
  }, [apiBaseInput]);

  useEffect(() => {
    if (!storageHydrated || !token || taskId || startedRef.current) return;
    startedRef.current = true;

    const total = zipFiles.length;
    setBusyMessage(null);
    resetTaskMonitor();
    setGenerating(true);
    setProgress({ completed: 0, total, currentFile: "" });

    let cancelled = false;
    void (async () => {
      try {
        if (!dbType) {
          throw new Error("未选择数据库类型");
        }
        const key = ensureSubmissionKey();
        const response = await generateReportTask(token, dbType, zipFiles, awrFiles, key);
        if (cancelled) return;

        setTaskId(response.task_id);
        if (typeof window !== "undefined") {
          sessionStorage.setItem(TASK_STORAGE_KEY, response.task_id);
        }
      } catch (e) {
        if (cancelled) return;
        if (isReportAPIErrorCode(e, REPORT_API_ERROR_CODES.capacityExhausted)) {
          setBusyMessage("服务当前任务已满。已保留所选文件，请在稍后手动重试。");
          setGenerating(false);
          return;
        }
        if (isReportAPIErrorCode(e, REPORT_API_ERROR_CODES.storageUnavailable)) {
          const fault: StorageFault = {
            code: REPORT_API_ERROR_CODES.storageUnavailable,
            message: "任务存储暂时不可用。",
          };
          setTaskStorageFault(fault);
          setGenerating(false);
          return;
        }
        const message = e instanceof Error ? e.message : String(e);
        addLog({
          id: generateLogId(),
          timestamp: new Date().toISOString(),
          level: "error",
          message: `生成任务失败: ${message}`,
        });
        setGenerating(false);
        setHasError(true);
        setComplete(true);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [
    addLog,
    awrFiles,
    dbType,
    ensureSubmissionKey,
    retryAttempt,
    setComplete,
    setGenerating,
    setHasError,
    setProgress,
    setTaskId,
    resetTaskMonitor,
    setTaskStorageFault,
    storageHydrated,
    taskId,
    token,
    zipFiles,
  ]);

  function onRetry() {
    startedRef.current = false;
    setBusyMessage(null);
    resetTaskMonitor();
    setHasError(false);
    setComplete(false);
    setRetryAttempt((attempt) => attempt + 1);
  }

  async function onConfirmToken() {
    const apiBase = apiBaseInput.trim();
    if (apiBase) {
      try {
        new URL(apiBase.includes("://") ? apiBase : `http://${apiBase}`);
      } catch (e) {
        addLog({
          id: generateLogId(),
          timestamp: new Date().toISOString(),
          level: "error",
          message: `API 地址无效: ${String(e)}`,
        });
        return;
      }
    }
    const value = tokenInput.trim();
    if (!value) return;
    if (apiBase && apiBase !== getApiBase()) {
      setApiBase(apiBase);
    }
    setToken(value);
    if (typeof window !== "undefined") {
      sessionStorage.setItem(TOKEN_STORAGE_KEY, value);
    }
  }

  async function onDownload() {
    if (!token || !downloadUrl) return;
    setDownloading(true);
    try {
      const blob = await downloadReportBlob(token, downloadUrl);
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "reports.zip";
      link.click();
      URL.revokeObjectURL(url);
    } finally {
      setDownloading(false);
    }
  }

  function onReset() {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem(TASK_STORAGE_KEY);
    }
    reset();
  }

  function onRetryFailed() {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem(TASK_STORAGE_KEY);
    }
    beginNewTask();
  }

  if (!token) {
    return (
      <div className="flex flex-col gap-4 max-w-md">
        <h2 className="text-lg font-semibold">输入访问 Token</h2>
        <p className="text-sm text-muted-foreground">
          Token 仅保存在当前浏览器会话（sessionStorage）。
        </p>
        <div className="space-y-2">
          <p className="text-sm font-medium">后端 API 地址（可选）</p>
          <input
            type="text"
            value={apiBaseInput}
            onChange={(event) => setApiBaseInput(event.target.value)}
            placeholder={API_BASE_HINT}
            className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
          />
          <p className="text-xs text-muted-foreground">
            不填写时会自动推断：如果页面在 <code>:3000</code>，默认后端为{" "}
            <code>:8080</code>；否则默认同源。
          </p>
        </div>
        <input
          type="password"
          value={tokenInput}
          onChange={(event) => setTokenInput(event.target.value)}
          placeholder="Bearer token（不含 Bearer 前缀）"
          className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
        />
        <button
          type="button"
          onClick={onConfirmToken}
          className="inline-flex items-center justify-center rounded-lg px-6 py-2.5 bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors duration-200 cursor-pointer"
        >
          开始生成
        </button>
      </div>
    );
  }

  return (
    <GenerationProgress
      progress={progress}
      logs={logs}
      isComplete={isComplete}
      hasError={hasError}
      downloadUrl={downloadUrl}
      isDownloading={isDownloading}
      onDownload={downloadUrl ? onDownload : null}
      busyMessage={busyMessage}
      onRetry={busyMessage ? onRetry : null}
      onReset={onReset}
      taskItems={taskSnapshot?.items}
      taskError={taskSnapshot?.error}
      succeededCount={taskSnapshot?.succeeded_count}
      failedCount={taskSnapshot?.failed_count}
      onRetryFailed={taskSnapshot?.failed_count ? onRetryFailed : null}
      isLiveDisconnected={isLiveDisconnected}
      storageFault={storageFault}
    />
  );
}
