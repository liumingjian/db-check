"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useReportStore } from "@/stores/report-store";
import { GenerationProgress } from "@/components/generation-progress";
import { wsUrl, getApiBase, setApiBase } from "@/lib/api";
import {
  downloadReportBlob,
  generateReportTask,
  getReportTaskStatus,
  ReportAPIError,
} from "@/lib/report-api";
import { API_BASE_HINT, DEFAULT_API_TOKEN } from "@/lib/web-defaults";
import type { ReportTaskSnapshot, WsMessage } from "@/lib/types";

const TOKEN_STORAGE_KEY = "dbcheck_api_token";
const TASK_STORAGE_KEY = "dbcheck_task_id";
const RECONNECT_DELAY_MS = 1_000;
const STATUS_POLL_INTERVAL_MS = 15_000;

function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

function isTerminalStatus(status: ReportTaskSnapshot["status"]): boolean {
  return status === "done" || status === "failed";
}

function taskWebSocketPath(taskID: string): string {
  return `/api/reports/ws/${taskID}`;
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
  const setDownloadUrl = useReportStore((s) => s.setDownloadUrl);
  const setComplete = useReportStore((s) => s.setComplete);
  const setHasError = useReportStore((s) => s.setHasError);
  const ensureSubmissionKey = useReportStore((s) => s.ensureSubmissionKey);
  const clearSubmissionKey = useReportStore((s) => s.clearSubmissionKey);
  const reset = useReportStore((s) => s.reset);
  const setToken = useReportStore((s) => s.setToken);

  const startedRef = useRef(false);
  const isCompleteRef = useRef(false);
  const hasErrorRef = useRef(false);
  const lastLogSeqRef = useRef(0);
  const taskVersionRef = useRef(-1);
  const snapshotTaskIDRef = useRef<string | null>(null);
  const statusWarningShownRef = useRef(false);
  const terminalErrorRef = useRef<string | null>(null);

  const [apiBaseInput, setApiBaseInput] = useState("");
  const [tokenInput, setTokenInput] = useState(DEFAULT_API_TOKEN);
  const [isDownloading, setDownloading] = useState(false);
  const [busyMessage, setBusyMessage] = useState<string | null>(null);
  const [retryAttempt, setRetryAttempt] = useState(0);
  const [storageHydrated, setStorageHydrated] = useState(false);
  const [isLiveDisconnected, setLiveDisconnected] = useState(false);
  const [taskSnapshot, setTaskSnapshot] = useState<ReportTaskSnapshot | null>(null);

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
    isCompleteRef.current = isComplete;
  }, [isComplete]);

  useEffect(() => {
    hasErrorRef.current = hasError;
  }, [hasError]);

  const applySnapshot = useCallback(
    (snapshot: ReportTaskSnapshot): boolean => {
      if (snapshotTaskIDRef.current !== snapshot.task_id) {
        snapshotTaskIDRef.current = snapshot.task_id;
        taskVersionRef.current = -1;
      }
      if (snapshot.version < taskVersionRef.current) {
        return isCompleteRef.current;
      }
      taskVersionRef.current = snapshot.version;
      setTaskSnapshot(snapshot);
      setTaskId(snapshot.task_id);
      if (typeof window !== "undefined") {
        sessionStorage.setItem(TASK_STORAGE_KEY, snapshot.task_id);
      }
      setProgress({
        completed: snapshot.completed,
        total: snapshot.total,
        currentFile: snapshot.current_file,
      });

      const terminal = isTerminalStatus(snapshot.status);
      const failed = snapshot.status === "failed";
      isCompleteRef.current = terminal;
      hasErrorRef.current = failed;
      setComplete(terminal);
      setHasError(failed);
      setGenerating(!terminal);

      if (snapshot.download_url) {
        setDownloadUrl(snapshot.download_url);
      }
      if (failed && snapshot.error) {
        const errorKey = `${snapshot.task_id}:${snapshot.version}:${snapshot.error}`;
        if (terminalErrorRef.current !== errorKey) {
          terminalErrorRef.current = errorKey;
          addLog({
            id: generateLogId(),
            timestamp: new Date().toISOString(),
            level: "error",
            message: snapshot.error,
          });
        }
      }
      return terminal;
    },
    [addLog, setComplete, setDownloadUrl, setGenerating, setHasError, setProgress, setTaskId],
  );

  useEffect(() => {
    if (!storageHydrated || !token || !taskId) return;
    const activeToken = token;
    const activeTaskID = taskId;

    let cancelled = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let pollTimer: number | null = null;

    function stopPolling() {
      if (pollTimer !== null) {
        window.clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    async function refreshTask(): Promise<boolean> {
      try {
        const snapshot = await getReportTaskStatus(activeToken, activeTaskID);
        if (cancelled) return false;
        statusWarningShownRef.current = false;
        const terminal = applySnapshot(snapshot);
        if (terminal) {
          stopPolling();
          setLiveDisconnected(false);
        }
        return terminal;
      } catch (e) {
        if (!cancelled && !statusWarningShownRef.current) {
          statusWarningShownRef.current = true;
          const message = e instanceof Error ? e.message : String(e);
          addLog({
            id: generateLogId(),
            timestamp: new Date().toISOString(),
            level: "warn",
            message: `无法获取任务最新状态，正在等待连接恢复: ${message}`,
          });
        }
        return false;
      }
    }

    function startPolling() {
      if (pollTimer !== null) return;
      pollTimer = window.setInterval(() => {
        void refreshTask();
      }, STATUS_POLL_INTERVAL_MS);
    }

    function scheduleReconnect() {
      if (reconnectTimer !== null) return;
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = null;
        connect();
      }, RECONNECT_DELAY_MS);
    }

    function connect() {
      if (cancelled || isCompleteRef.current || hasErrorRef.current) return;
      const ws = new WebSocket(wsUrl(taskWebSocketPath(activeTaskID)), [activeToken]);
      socket = ws;

      ws.onopen = () => {
        if (cancelled || socket !== ws) return;
        setLiveDisconnected(false);
        stopPolling();
        void refreshTask().then((terminal) => {
          if (terminal && socket === ws) {
            ws.close();
          }
        });
      };

      ws.onmessage = (event) => {
        if (cancelled || socket !== ws) return;
        try {
          const message = JSON.parse(String(event.data)) as WsMessage;
          switch (message.type) {
            case "snapshot":
              if (applySnapshot(message) && socket === ws) {
                ws.close();
              }
              break;
            case "log":
              if (message.seq <= lastLogSeqRef.current) return;
              lastLogSeqRef.current = message.seq;
              addLog({
                id: generateLogId(),
                timestamp: message.timestamp,
                level: message.level,
                message: message.message,
              });
              break;
            case "progress":
              if (message.seq <= taskVersionRef.current) return;
              taskVersionRef.current = message.seq;
              setProgress({
                completed: message.completed,
                total: message.total,
                currentFile: message.current_file,
              });
              break;
            case "done":
            case "error":
              // Notifications are advisory. Re-read durable state before showing a terminal outcome.
              void refreshTask();
              break;
          }
        } catch (e) {
          const message = e instanceof Error ? e.message : String(e);
          addLog({
            id: generateLogId(),
            timestamp: new Date().toISOString(),
            level: "warn",
            message: `收到无法识别的实时消息: ${message}`,
          });
        }
      };

      ws.onclose = () => {
        if (cancelled || socket !== ws) return;
        socket = null;
        if (isCompleteRef.current || hasErrorRef.current) return;
        setLiveDisconnected(true);
        startPolling();
        scheduleReconnect();
      };
    }

    void refreshTask().then((terminal) => {
      if (!cancelled && !terminal) {
        connect();
      }
    });

    return () => {
      cancelled = true;
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      stopPolling();
      socket?.close();
    };
  }, [addLog, applySnapshot, setProgress, storageHydrated, taskId, token]);

  useEffect(() => {
    if (!storageHydrated || !token || taskId || startedRef.current) return;
    startedRef.current = true;

    const total = zipFiles.length;
    setBusyMessage(null);
    setTaskSnapshot(null);
    setLiveDisconnected(false);
    taskVersionRef.current = -1;
    snapshotTaskIDRef.current = null;
    isCompleteRef.current = false;
    hasErrorRef.current = false;
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
        if (e instanceof ReportAPIError && e.code === "capacity_exhausted") {
          setBusyMessage("服务当前任务已满。已保留所选文件，请在稍后手动重试。");
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
        isCompleteRef.current = true;
        hasErrorRef.current = true;
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
    storageHydrated,
    taskId,
    token,
    zipFiles,
  ]);

  function onRetry() {
    startedRef.current = false;
    isCompleteRef.current = false;
    hasErrorRef.current = false;
    setBusyMessage(null);
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
    clearSubmissionKey();
    reset();
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
      isLiveDisconnected={isLiveDisconnected}
    />
  );
}
