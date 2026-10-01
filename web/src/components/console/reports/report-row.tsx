"use client";

import { useState } from "react";
import { ArrowDown, ChevronDown, TriangleAlert } from "lucide-react";
import { useDialogs } from "@/components/console/dialog-host";
import { api, ApiError, type ReportTask } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

/**
 * One report task as a hairline row: time, file name, one-click re-download,
 * and a marker only when something is off. 部分失败 and 失败 expand to each
 * failed item's reason. A revoked collector version adds a yellow warning
 * line. Shared by 我的报告 and 管理 → 全部报告
 * (`showSubmitter`).
 */
export function ReportRow({ task, showSubmitter = false }: { task: ReportTask; showSubmitter?: boolean }) {
  const token = useAuthStore((s) => s.token);
  const { toast } = useDialogs();
  const [open, setOpen] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const marker = markerFor(task);
  const failures = failedItems(task);
  const expandable = (marker === "failed" || marker === "partial") && failures.length > 0;
  const downloadable = task.status === "done" && !task.expired;

  async function download() {
    if (!token) return;
    setDownloading(true);
    try {
      saveBlob(await api.reports.download(token, task.id), `reports-${task.id}.zip`);
    } catch (e) {
      toast(e instanceof ApiError ? e.message : `下载失败：${String(e)}`);
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div className="py-4">
      <div className="flex items-center gap-6">
        <span className="w-28 shrink-0 text-sm text-muted-foreground tabular-nums">{when(task.createdAt)}</span>
        {showSubmitter && <span className="w-20 shrink-0 truncate text-sm font-semibold">{task.submitter.displayName}</span>}
        <span className="min-w-0 flex-1 truncate text-base">
          {task.items[0]?.fileName}
          {task.items.length > 1 && <span className="text-muted-foreground"> 等 {task.items.length} 份</span>}
        </span>
        {marker &&
          (expandable ? (
            <button
              type="button"
              aria-expanded={open}
              onClick={() => setOpen((o) => !o)}
              className="flex cursor-pointer items-center gap-1 text-sm text-[#5a5a5a] hover:text-foreground"
            >
              {MARKER_LABEL[marker]}
              <ChevronDown className={cn("h-3.5 w-3.5", open && "rotate-180")} />
            </button>
          ) : (
            <span className="text-sm text-[#5a5a5a]">{MARKER_LABEL[marker]}</span>
          ))}
        {downloadable && (
          <button
            type="button"
            disabled={downloading}
            onClick={download}
            className="flex cursor-pointer items-center gap-1 text-sm font-semibold text-primary hover:underline disabled:cursor-wait disabled:opacity-60"
          >
            下载 <ArrowDown className="h-4 w-4" />
          </button>
        )}
      </div>
      {revokedVersions(task).map(({ version, reason }) => (
        <p key={version} className={cn("mt-1.5 flex items-start gap-1.5 text-sm text-primary", showSubmitter ? "pl-60" : "pl-34")}>
          <TriangleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            采集器 v{version} 已撤回：{reason}
          </span>
        </p>
      ))}
      {expandable && open && (
        <ul className="mt-3 space-y-1 pl-34 text-xs">
          {failures.map((f) => (
            <li key={f.fileName}>
              <span className="font-mono">{f.fileName}</span>
              <span className="text-muted-foreground"> · {f.reason}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** The one marker a row shows when something is off; a plain successful task has none (spec #19). */
type Marker = "processing" | "failed" | "expired" | "partial";

const MARKER_LABEL: Record<Marker, string> = {
  processing: "生成中",
  failed: "失败",
  expired: "已过期",
  partial: "部分失败",
};

function markerFor(task: ReportTask): Marker | null {
  if (task.status === "processing") return "processing";
  if (task.status === "failed") return "failed";
  if (task.expired) return "expired";
  if (task.items.some((item) => item.outcome.status === "failed")) return "partial";
  return null;
}

function failedItems(task: ReportTask): Array<{ fileName: string; reason: string }> {
  return task.items.flatMap((item) =>
    item.outcome.status === "failed" ? [{ fileName: item.fileName, reason: item.outcome.reason }] : [],
  );
}

/**
 * Report lists repeat only the revoked warning, never the deprecated notice
 * (spec #19). One line per revoked version, however many items used it.
 */
function revokedVersions(task: ReportTask): Array<{ version: string; reason: string }> {
  const revoked = new Map<string, string>();
  for (const { collectorVersion, collectorNotice } of task.items) {
    if (collectorVersion && collectorNotice?.status === "revoked") revoked.set(collectorVersion, collectorNotice.reason);
  }
  return Array.from(revoked, ([version, reason]) => ({ version, reason }));
}

/** 今天 14:32, 昨天 09:15, 9月20日 14:32; the year shows only when it isn't this year. */
function when(iso: string): string {
  const at = new Date(iso);
  const time = at.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false });
  const today = new Date();
  const yesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1);
  const sameDay = (a: Date, b: Date) => a.toDateString() === b.toDateString();
  if (sameDay(at, today)) return `今天 ${time}`;
  if (sameDay(at, yesterday)) return `昨天 ${time}`;
  const year = at.getFullYear() === today.getFullYear() ? "" : `${at.getFullYear()}年`;
  return `${year}${at.getMonth() + 1}月${at.getDate()}日 ${time}`;
}

function saveBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}

