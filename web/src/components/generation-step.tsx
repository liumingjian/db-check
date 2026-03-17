"use client";

import { useEffect, useRef, useState } from "react";
import { useReportStore } from "@/stores/report-store";
import { GenerationProgress } from "@/components/generation-progress";
import { apiUrl, wsUrl, getApiBase, setApiBase } from "@/lib/api";
import type { WsMessage, GenerateResponse, ZipFileEntry } from "@/lib/types";

function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

const TOKEN_STORAGE_KEY = "dbcheck_api_token";
const TASK_STORAGE_KEY = "dbcheck_task_id";
const API_BASE_HINT = "http://127.0.0.1:8080";

export function GenerationStep() {
  const zipFiles = useReportStore((s) => s.zipFiles);
  const awrFiles = useReportStore((s) => s.awrFiles);
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
  const reset = useReportStore((s) => s.reset);
  const setToken = useReportStore((s) => s.setToken);

  const startedRef = useRef(false);
  const wsRef = useRef<WebSocket | null>(null);
  const lastLogSeqRef = useRef<number>(0);
  const reconnectTimerRef = useRef<number | null>(null);
  const isCompleteRef = useRef<boolean>(isComplete);
  const hasErrorRef = useRef<boolean>(hasError);

  const [apiBaseInput, setApiBaseInput] = useState("");
  const [tokenInput, setTokenInput] = useState("");
  const [isDownloading, setDownloading] = useState(false);

  useEffect(() => {
    // Hydrate token/taskId from sessionStorage for reconnect/recovery.
    if (typeof window === "undefined") return;
    if (!token) {
      const saved = sessionStorage.getItem(TOKEN_STORAGE_KEY);
      if (saved) setToken(saved);
    }
  }, [setToken, token]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!apiBaseInput) {
      // Prefill from inferred/env/stored base so the user can see where requests will go.
      setApiBaseInput(getApiBase());
    }
  }, [apiBaseInput]);

  useEffect(() => {
    isCompleteRef.current = isComplete;
  }, [isComplete]);

  useEffect(() => {
    hasErrorRef.current = hasError;
  }, [hasError]);

  useEffect(() => {
    if (!token) return;
    if (startedRef.current) return;
    startedRef.current = true;

    const total = zipFiles.length;

    setGenerating(true);
    setProgress({ completed: 0, total, currentFile: "" });

    let cancelled = false;

    (async () => {
      try {
        const resp = await generate(token, zipFiles, awrFiles);
        if (cancelled) return;

        setTaskId(resp.task_id);
        if (typeof window !== "undefined") {
          sessionStorage.setItem(TASK_STORAGE_KEY, resp.task_id);
        }

        connectWS(token, resp.ws_url);
      } catch (e) {
        // Most common local misconfig: frontend calls its own origin (e.g. :3000) and gets 404.
        // Provide a clearer hint than the raw error.
        const msg = String(e);
        const help =
          msg.includes("generate failed: 404") || msg.includes("404")
            ? `（提示：接口 404 通常是前端没有连到 db-web；请确认 API 地址为后端地址，例如 ${API_BASE_HINT}，或设置 NEXT_PUBLIC_API_BASE）`
            : "";
        addLog({
          id: generateLogId(),
          timestamp: new Date().toISOString(),
          level: "error",
          message: `生成任务失败: ${msg}${help}`,
        });
        setGenerating(false);
        setHasError(true);
        setComplete(true);
      }
    })();

    function connectWS(tokenValue: string, wsPath: string) {
      if (wsRef.current) wsRef.current.close();
      const ws = new WebSocket(wsUrl(wsPath), [tokenValue]);
      wsRef.current = ws;

      ws.onmessage = (ev) => {
        const msg = JSON.parse(String(ev.data)) as WsMessage;
        handleMessage(msg);
      };

      ws.onclose = () => {
        if (cancelled) return;
        if (isCompleteRef.current || hasErrorRef.current) return;
        // Simple reconnect with a small delay.
        if (reconnectTimerRef.current) {
          window.clearTimeout(reconnectTimerRef.current);
        }
        reconnectTimerRef.current = window.setTimeout(() => {
          connectWS(tokenValue, wsPath);
        }, 1000);
      };
    }

    function handleMessage(msg: WsMessage) {
      switch (msg.type) {
        case "log":
          // Dedup logs using seq on reconnect (progress snapshot may reuse seq).
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
          break;
      }
    }

    return () => {
      cancelled = true;
      if (reconnectTimerRef.current) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      wsRef.current?.close();
    };
  }, [addLog, awrFiles, setComplete, setDownloadUrl, setGenerating, setHasError, setProgress, setTaskId, token, zipFiles]);

  async function onConfirmToken() {
    const apiBase = apiBaseInput.trim();
    if (apiBase) {
      try {
        setApiBase(apiBase);
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
    const v = tokenInput.trim();
    if (!v) return;
    setToken(v);
    if (typeof window !== "undefined") {
      sessionStorage.setItem(TOKEN_STORAGE_KEY, v);
    }
  }

  async function onDownload() {
    if (!token || !downloadUrl) return;
    setDownloading(true);
    try {
      const resp = await fetch(apiUrl(downloadUrl), {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!resp.ok) {
        throw new Error(`download failed: ${resp.status}`);
      }
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "reports.zip";
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      setDownloading(false);
    }
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
            onChange={(e) => setApiBaseInput(e.target.value)}
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
          onChange={(e) => setTokenInput(e.target.value)}
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
      onReset={reset}
    />
  );
}

async function generate(
  token: string,
  zipFiles: ZipFileEntry[],
  awrFiles: Record<string, File>,
): Promise<GenerateResponse> {
  const form = new FormData();
  zipFiles.forEach((z) => {
    form.append("zips", z.file, z.name);
  });
  zipFiles.forEach((z, idx) => {
    const awr = awrFiles[z.id];
    if (awr) {
      form.append(`awr_${idx + 1}`, awr, awr.name);
    }
  });

  const resp = await fetch(apiUrl("/api/reports/generate"), {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(text || `generate failed: ${resp.status}`);
  }
  return (await resp.json()) as GenerateResponse;
}
