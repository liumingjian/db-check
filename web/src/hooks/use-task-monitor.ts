"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { wsUrl } from "@/lib/api";
import { getReportTaskStatus } from "@/lib/report-api";
import type { ReportTaskSnapshot, StorageFault, WsMessage } from "@/lib/types";
import { useReportStore } from "@/stores/report-store";

const TASK_STORAGE_KEY = "dbcheck_task_id";
const RECONNECT_DELAY_MS = 1_000;
const STATUS_POLL_INTERVAL_MS = 15_000;

type TaskMonitorOptions = {
  storageHydrated: boolean;
  token: string | null;
  taskId: string | null;
};

function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

function isTerminalStatus(status: ReportTaskSnapshot["status"]): boolean {
  return status === "done" || status === "failed";
}

function taskWebSocketPath(taskID: string): string {
  return `/api/reports/ws/${taskID}`;
}

export function useTaskMonitor({ storageHydrated, token, taskId }: TaskMonitorOptions) {
  const isComplete = useReportStore((state) => state.isComplete);
  const hasError = useReportStore((state) => state.hasError);
  const setGenerating = useReportStore((state) => state.setGenerating);
  const setTaskId = useReportStore((state) => state.setTaskId);
  const setProgress = useReportStore((state) => state.setProgress);
  const addLog = useReportStore((state) => state.addLog);
  const setDownloadUrl = useReportStore((state) => state.setDownloadUrl);
  const setComplete = useReportStore((state) => state.setComplete);
  const setHasError = useReportStore((state) => state.setHasError);

  const isCompleteRef = useRef(false);
  const hasErrorRef = useRef(false);
  const lastLogSeqRef = useRef(0);
  const lastProgressSeqRef = useRef(0);
  const taskVersionRef = useRef(-1);
  const snapshotTaskIDRef = useRef<string | null>(null);
  const statusWarningShownRef = useRef(false);
  const terminalErrorRef = useRef<string | null>(null);
  const storageFaultRef = useRef<StorageFault | null>(null);

  const [isLiveDisconnected, setLiveDisconnected] = useState(false);
  const [taskSnapshot, setTaskSnapshot] = useState<ReportTaskSnapshot | null>(null);
  const [storageFault, setStorageFault] = useState<StorageFault | null>(null);

  useEffect(() => {
    isCompleteRef.current = isComplete;
  }, [isComplete]);

  useEffect(() => {
    hasErrorRef.current = hasError;
  }, [hasError]);

  const resetTaskMonitor = useCallback(() => {
    isCompleteRef.current = false;
    hasErrorRef.current = false;
    lastLogSeqRef.current = 0;
    lastProgressSeqRef.current = 0;
    taskVersionRef.current = -1;
    snapshotTaskIDRef.current = null;
    statusWarningShownRef.current = false;
    terminalErrorRef.current = null;
    storageFaultRef.current = null;
    setLiveDisconnected(false);
    setTaskSnapshot(null);
    setStorageFault(null);
  }, []);

  const setTaskStorageFault = useCallback((fault: StorageFault | null) => {
    storageFaultRef.current = fault;
    setStorageFault(fault);
  }, []);

  const applySnapshot = useCallback(
    (snapshot: ReportTaskSnapshot): boolean => {
      if (snapshotTaskIDRef.current !== snapshot.task_id) {
        snapshotTaskIDRef.current = snapshot.task_id;
        taskVersionRef.current = -1;
        lastProgressSeqRef.current = 0;
      }
      if (snapshot.version < taskVersionRef.current) {
        return isCompleteRef.current;
      }

      const incomingStorageFault = snapshot.storage_fault ?? null;
      taskVersionRef.current = snapshot.version;
      storageFaultRef.current = incomingStorageFault;
      setStorageFault(incomingStorageFault);
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
      setGenerating(!terminal && !incomingStorageFault);

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
      } catch (error) {
        if (!cancelled && !statusWarningShownRef.current) {
          statusWarningShownRef.current = true;
          const message = error instanceof Error ? error.message : String(error);
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
      lastProgressSeqRef.current = 0;
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
              if (storageFaultRef.current || message.seq <= lastProgressSeqRef.current) return;
              lastProgressSeqRef.current = message.seq;
              setProgress({
                completed: message.completed,
                total: message.total,
                currentFile: message.current_file,
              });
              break;
            case "done":
            case "error":
              // Notifications are advisory. Re-read durable state before showing a terminal outcome.
              if (!storageFaultRef.current) {
                void refreshTask();
              }
              break;
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
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

  return {
    isLiveDisconnected,
    resetTaskMonitor,
    setTaskStorageFault,
    storageFault,
    taskSnapshot,
  };
}
