"use client";

import { useEffect, useRef, useState } from "react";
import { useDialogs } from "@/components/console/dialog-host";
import { useBlobDownload } from "@/components/console/use-blob-download";
import { api, errorMessage, type CollectorNotice, type ReportEvent, type ReportTaskInput } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";

/** A submitted task while it generates, and how it ended. */
export interface Run {
  taskId: string;
  total: number;
  completed: number;
  outcome: "running" | "done" | "error";
  error?: string;
  /** Per item, in row order; empty until the task is read back. */
  notices: Array<CollectorNotice | null>;
  /** Why the task could not be read back for its collector notices. */
  noticesError?: string;
}

/**
 * One report task from submission to download: submits, reads the task back
 * for its collector notices, follows its events, and downloads the result.
 * Leaving the page stops the watch; the task itself goes on.
 */
export function useReportRun() {
  const token = useAuthStore((s) => s.token);
  const { toast } = useDialogs();
  const { download: saveFile, downloading } = useBlobDownload();
  const [run, setRun] = useState<Run | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const stopWatching = useRef<(() => void) | null>(null);

  useEffect(() => () => stopWatching.current?.(), []);

  const update = (taskId: string, change: (r: Run) => Run) => setRun((r) => (r?.taskId === taskId ? change(r) : r));

  /** The notices come from the recorded task, joined with each release's current status. */
  function readNotices(token: string, taskId: string) {
    api.reports.getTask(token, taskId).then(
      (task) => update(taskId, (r) => ({ ...r, notices: task.items.map((i) => i.collectorNotice) })),
      (e: unknown) => update(taskId, (r) => ({ ...r, noticesError: errorMessage(e) })),
    );
  }

  async function submit(input: ReportTaskInput) {
    if (!token) return;
    setSubmitting(true);
    try {
      const { taskId, total } = await api.reports.generate(token, input);
      setRun({ taskId, total, completed: 0, outcome: "running", notices: [] });
      readNotices(token, taskId);
      stopWatching.current = api.reports.watch(token, taskId, (event) => update(taskId, (r) => applyEvent(r, event)));
    } catch (e) {
      toast(errorMessage(e));
    } finally {
      setSubmitting(false);
    }
  }

  function download() {
    if (token && run) void saveFile(() => api.reports.download(token, run.taskId), `reports-${run.taskId}.zip`);
  }

  function reset() {
    stopWatching.current?.();
    stopWatching.current = null;
    setRun(null);
  }

  return { run, submitting, downloading, submit, download, reset };
}

function applyEvent(run: Run, event: ReportEvent): Run {
  if (event.type === "progress") return { ...run, completed: event.completed, total: event.total };
  if (event.type === "done") return { ...run, completed: run.total, outcome: "done" };
  if (event.type === "error") return { ...run, outcome: "error", error: event.message };
  return run;
}
