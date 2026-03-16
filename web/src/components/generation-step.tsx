"use client";

import { useEffect, useRef } from "react";
import { useReportStore } from "@/stores/report-store";
import { mockGenerate, mockWebSocket } from "@/lib/mock-api";
import { GenerationProgress } from "@/components/generation-progress";
import type { WsMessage } from "@/lib/types";

function generateLogId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

export function GenerationStep() {
  const zipFiles = useReportStore((s) => s.zipFiles);
  const progress = useReportStore((s) => s.progress);
  const logs = useReportStore((s) => s.logs);
  const isComplete = useReportStore((s) => s.isComplete);
  const hasError = useReportStore((s) => s.hasError);
  const downloadUrl = useReportStore((s) => s.downloadUrl);
  const isGenerating = useReportStore((s) => s.isGenerating);

  const setGenerating = useReportStore((s) => s.setGenerating);
  const setTaskId = useReportStore((s) => s.setTaskId);
  const setProgress = useReportStore((s) => s.setProgress);
  const addLog = useReportStore((s) => s.addLog);
  const setDownloadUrl = useReportStore((s) => s.setDownloadUrl);
  const setComplete = useReportStore((s) => s.setComplete);
  const setHasError = useReportStore((s) => s.setHasError);
  const reset = useReportStore((s) => s.reset);

  const startedRef = useRef(false);

  useEffect(() => {
    if (startedRef.current || isGenerating || isComplete) return;
    startedRef.current = true;

    const fileNames = zipFiles.map((z) => z.name);
    const total = zipFiles.length;

    setGenerating(true);
    setProgress({ completed: 0, total, currentFile: "" });

    let cleanup: (() => void) | undefined;

    (async () => {
      const resp = await mockGenerate(total);
      setTaskId(resp.task_id);

      function handleMessage(msg: WsMessage) {
        switch (msg.type) {
          case "log":
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
            setHasError(true);
            break;
        }
      }

      cleanup = mockWebSocket(total, fileNames, handleMessage);
    })();

    return () => {
      cleanup?.();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <GenerationProgress
      progress={progress}
      logs={logs}
      isComplete={isComplete}
      hasError={hasError}
      downloadUrl={downloadUrl}
      onReset={reset}
    />
  );
}
