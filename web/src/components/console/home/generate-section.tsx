"use client";

import { useEffect, useRef, useState } from "react";
import { ArrowDown, ArrowRight, Check, FileText, TriangleAlert, X } from "lucide-react";
import { ConsoleSection } from "@/components/console/console-shell";
import { useDialogs } from "@/components/console/dialog-host";
import { CAPTION, PRESS, YellowButton } from "@/components/console/kit";
import { api, type CollectorNotice } from "@/lib/api";
import {
  collectUnpaired,
  fileKey,
  inspectDrop,
  pairDiagnostic,
  toTaskInput,
  type InspectedZip,
} from "@/lib/report-input/inspect";
import { DB_LABEL, type DbType } from "@/lib/types";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

/** The HTML a row's slot takes, by database type; mysql rows have no slot. */
const SLOT_LABEL: Partial<Record<DbType, string>> = { oracle: "AWR", gaussdb: "WDR" };

/** Drag type for an HTML moved from 待配对 onto a row; the payload is its `fileKey`. */
const DIAGNOSTIC_DRAG = "application/x-dbcheck-diagnostic";

const STATS = [
  ["3", "种数据库", "MySQL · Oracle · GaussDB"],
  ["4", "个平台", "Linux / Windows · x86 / ARM"],
  ["30", "天保留", "在「我的报告」随时重新下载"],
] as const;

/** A submitted task while it generates, and how it ended. */
interface Run {
  taskId: string;
  total: number;
  completed: number;
  outcome: "running" | "done" | "error";
  error?: string;
  /** Per item, in row order; empty until the task is read back. */
  notices: Array<CollectorNotice | null>;
}

function formatSize(bytes: number): string {
  if (bytes < 1024 * 1024) return `${Math.max(1, Math.round(bytes / 1024))} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function saveBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}

/** 生成报告: the drop zone, one row per report item, overall progress, then the «报告好了。» band. */
export function GenerateSection() {
  const token = useAuthStore((s) => s.token);
  const { toast } = useDialogs();
  const [items, setItems] = useState<InspectedZip[]>([]);
  const [unpaired, setUnpaired] = useState<File[]>([]);
  const [inspecting, setInspecting] = useState(0);
  const [run, setRun] = useState<Run | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [dragging, setDragging] = useState(false);
  const dragDepth = useRef(0);
  const picker = useRef<HTMLInputElement>(null);
  const stopWatching = useRef<(() => void) | null>(null);

  useEffect(() => () => stopWatching.current?.(), []);

  const busy = run !== null;
  const taskInput = toTaskInput(items, unpaired);

  async function add(files: File[]) {
    if (busy || files.length === 0) return;
    setUnpaired((current) => [...current, ...collectUnpaired(files, items, current)]);
    setInspecting((n) => n + 1);
    try {
      const fresh = await inspectDrop(files, items);
      // Filter again: another drop may have landed while this one was read.
      setItems((current) => [...current, ...fresh.filter((f) => !current.some((c) => c.file.name === f.file.name))]);
    } finally {
      setInspecting((n) => n - 1);
    }
  }

  /** Pairs the 待配对 file with this key onto the row, or toasts why the row refuses it. */
  function pair(item: InspectedZip, key: string) {
    const file = unpaired.find((f) => fileKey(f) === key);
    if (!file) return;
    const outcome = pairDiagnostic(item, file);
    if (!outcome.ok) {
      toast(outcome.reason);
      return;
    }
    setItems((current) => current.map((c) => (c === item ? outcome.item : c)));
    setUnpaired((current) => current.filter((f) => f !== file));
  }

  function unpair(item: InspectedZip, file: File) {
    setItems((current) => current.map((c) => (c === item ? { ...c, diagnostics: c.diagnostics.filter((d) => d !== file) } : c)));
    setUnpaired((current) => [...current, file]);
  }

  function removeItem(item: InspectedZip) {
    setItems((current) => current.filter((c) => c !== item));
    setUnpaired((current) => [...current, ...item.diagnostics]);
  }

  async function submit() {
    if (!token || !taskInput) return;
    setSubmitting(true);
    try {
      const { taskId, total } = await api.reports.generate(token, taskInput);
      setRun({ taskId, total, completed: 0, outcome: "running", notices: [] });
      // The notices come from the recorded task, joined with each release's current status.
      api.reports
        .getTask(token, taskId)
        .then((task) =>
          setRun((r) => (r?.taskId === taskId ? { ...r, notices: task.items.map((i) => i.collectorNotice) } : r)),
        )
        // Without the read-back the rows just show no notice; generation goes on.
        .catch(() => {});
      stopWatching.current = api.reports.watch(token, taskId, (event) => {
        if (event.type === "progress") {
          setRun((r) => r && { ...r, completed: event.completed, total: event.total });
        } else if (event.type === "done") {
          setRun((r) => r && { ...r, completed: r.total, outcome: "done" });
        } else if (event.type === "error") {
          setRun((r) => r && { ...r, outcome: "error", error: event.message });
        }
      });
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function download() {
    if (!token || !run) return;
    setDownloading(true);
    try {
      saveBlob(await api.reports.download(token, run.taskId), `reports-${run.taskId}.zip`);
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e));
    } finally {
      setDownloading(false);
    }
  }

  function reset() {
    stopWatching.current?.();
    stopWatching.current = null;
    setRun(null);
    setItems([]);
    setUnpaired([]);
  }

  const carriesFiles = (e: React.DragEvent) => e.dataTransfer.types.includes("Files");
  const dropProps = busy
    ? {}
    : {
        onDragEnter: (e: React.DragEvent) => {
          if (!carriesFiles(e)) return;
          dragDepth.current += 1;
          setDragging(true);
        },
        onDragOver: (e: React.DragEvent) => {
          if (carriesFiles(e)) e.preventDefault();
        },
        onDragLeave: () => {
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (dragDepth.current === 0) setDragging(false);
        },
        onDrop: (e: React.DragEvent) => {
          e.preventDefault();
          dragDepth.current = 0;
          setDragging(false);
          void add(Array.from(e.dataTransfer.files));
        },
      };

  if (run?.outcome === "done") {
    return (
      <ConsoleSection id="new-report">
        <div className="flex min-h-[min(100vh,860px)] flex-col bg-primary text-primary-foreground">
          <div className="mx-auto flex w-full max-w-[1240px] flex-1 flex-col justify-center px-8 py-16">
            <p className="text-[12px] font-semibold tracking-[1.5px] uppercase opacity-60">{run.taskId}</p>
            <h1 className="mt-4 text-[96px] leading-[1] font-bold tracking-[-3.5px]">报告好了。</h1>
            <p className="mt-6 text-lg opacity-70">{run.total} 份报告 · 30 天内可在「我的报告」重新下载</p>
            <div className="mt-12 flex items-center gap-6">
              <button
                type="button"
                disabled={downloading}
                onClick={() => void download()}
                className={cn(
                  "inline-flex h-16 cursor-pointer items-center gap-3 rounded-xl bg-background px-8 text-lg font-semibold text-primary disabled:opacity-40",
                  PRESS,
                )}
              >
                下载报告 <ArrowDown className="h-5 w-5" />
              </button>
              <button type="button" onClick={reset} className="cursor-pointer text-base font-semibold underline underline-offset-4">
                再来一份
              </button>
            </div>
          </div>
        </div>
      </ConsoleSection>
    );
  }

  const overall = run && run.total > 0 ? Math.round((run.completed / run.total) * 100) : 0;

  return (
    <ConsoleSection id="new-report">
      <div
        {...dropProps}
        className={cn(
          "flex min-h-[min(100vh,860px)] flex-col transition-[background-color,color] duration-200",
          dragging && "bg-primary text-primary-foreground",
        )}
      >
        <input
          ref={picker}
          type="file"
          multiple
          accept=".zip,.html,.htm"
          className="hidden"
          onChange={(e) => {
            void add(Array.from(e.target.files ?? []));
            e.target.value = "";
          }}
        />
        <div className="mx-auto grid w-full max-w-[1240px] flex-1 grid-cols-[1.3fr_1fr] items-center gap-16 px-8 py-16">
          <div>
            {dragging ? (
              <h1 className="text-[120px] leading-[1] font-bold tracking-[-4px]">松手。</h1>
            ) : run ? (
              <>
                <p className={CAPTION}>{run.outcome === "error" ? "生成失败" : "正在生成"}</p>
                <p
                  className={cn(
                    "mt-4 text-[160px] leading-[0.9] font-bold tracking-[-6px] tabular-nums",
                    run.outcome === "error" ? "text-destructive" : "text-primary",
                  )}
                >
                  {overall}
                  <span className="text-[64px] tracking-[-2px]">%</span>
                </p>
                {run.outcome === "error" ? (
                  <>
                    <p className="mt-6 text-lg text-destructive">{run.error}</p>
                    <button type="button" onClick={reset} className="mt-6 cursor-pointer text-base font-semibold underline underline-offset-4">
                      重新开始
                    </button>
                  </>
                ) : (
                  <p className="mt-6 text-lg text-[#ccc] tabular-nums">
                    {run.completed} / {run.total} 份报告
                  </p>
                )}
              </>
            ) : (
              <>
                <h1 className="text-[88px] leading-[1.02] font-bold tracking-[-3px]">
                  拖进 ZIP，
                  <br />
                  <span className="text-primary">拿走报告。</span>
                </h1>
                <p className="mt-8 max-w-md text-lg leading-relaxed text-[#ccc]">
                  把采集器生成的 ZIP 拖到这一屏任意位置。数据库类型自动识别，一次可以放多台主机。Oracle 的 AWR、GaussDB
                  的 WDR 报告可以一起拖进来，再配对到对应的采集包。
                </p>
                <div className="mt-10">
                  <YellowButton onClick={() => picker.current?.click()}>
                    选择采集包 <ArrowRight className="h-4 w-4" />
                  </YellowButton>
                </div>
              </>
            )}
          </div>

          <div className={cn(dragging && "invisible")}>
            {items.length > 0 || unpaired.length > 0 ? (
              <div className="rounded-2xl bg-card p-2">
                {items.map((item, index) => (
                  <ItemRow
                    key={item.file.name}
                    item={item}
                    stage={run ? (index < run.completed ? "done" : index === run.completed ? "current" : "queued") : null}
                    notice={run?.notices[index] ?? null}
                    onRemove={() => removeItem(item)}
                    onPair={(key) => pair(item, key)}
                    onUnpair={(file) => unpair(item, file)}
                  />
                ))}
                {!run && unpaired.length > 0 && (
                  <Unpaired files={unpaired} onRemove={(file) => setUnpaired((current) => current.filter((f) => f !== file))} />
                )}
                {inspecting > 0 && <p className="px-4 py-3.5 text-xs text-muted-foreground">正在读取 manifest…</p>}
                {!run && (
                  <>
                    <YellowButton
                      onClick={() => void submit()}
                      disabled={!taskInput || inspecting > 0 || submitting}
                      className="mt-2 h-14 w-full text-base"
                    >
                      生成 {items.length} 份报告 <ArrowRight className="h-4 w-4" />
                    </YellowButton>
                    {items.some((i) => !i.inspection.ok) && (
                      <p className="px-4 pt-3 pb-1 text-xs text-destructive">移除标红的采集包后才能生成。</p>
                    )}
                    {unpaired.length > 0 && (
                      <p className="px-4 pt-3 pb-1 text-xs text-muted-foreground">把待配对的文件拖到对应的采集包上，或移除它们，才能生成。</p>
                    )}
                  </>
                )}
              </div>
            ) : inspecting > 0 ? (
              <p className="text-sm text-muted-foreground">正在读取 manifest…</p>
            ) : (
              <div className="grid grid-cols-2 gap-x-10 gap-y-12">
                {STATS.map(([n, unit, sub]) => (
                  <div key={unit}>
                    <p className="text-[56px] leading-none font-bold tracking-[-1.5px] text-primary tabular-nums">{n}</p>
                    <p className="mt-2 text-base font-semibold">{unit}</p>
                    <p className="mt-0.5 text-sm text-muted-foreground">{sub}</p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </ConsoleSection>
  );
}

/**
 * One report item: file name, what its manifest says, its paired AWR/WDR files,
 * and its progress once submitted. Before submission it takes drops from 待配对.
 */
function ItemRow({
  item: { file, inspection, diagnostics },
  stage,
  notice,
  onRemove,
  onPair,
  onUnpair,
}: {
  item: InspectedZip;
  stage: "done" | "current" | "queued" | null;
  /** Shown only while generating; report lists repeat only the revoked warning. */
  notice: CollectorNotice | null;
  onRemove: () => void;
  onPair: (key: string) => void;
  onUnpair: (file: File) => void;
}) {
  const [target, setTarget] = useState(false);
  const carriesDiagnostic = (e: React.DragEvent) => e.dataTransfer.types.includes(DIAGNOSTIC_DRAG);
  const pairProps =
    stage === null
      ? {
          onDragOver: (e: React.DragEvent) => {
            if (!carriesDiagnostic(e)) return;
            e.preventDefault();
            setTarget(true);
          },
          onDragLeave: () => setTarget(false),
          onDrop: (e: React.DragEvent) => {
            if (!carriesDiagnostic(e)) return;
            e.preventDefault();
            e.stopPropagation();
            setTarget(false);
            onPair(e.dataTransfer.getData(DIAGNOSTIC_DRAG));
          },
        }
      : {};
  const slot = inspection.ok ? SLOT_LABEL[inspection.dbType] : undefined;

  return (
    <div
      {...pairProps}
      className={cn(
        "rounded-xl px-4 py-3.5 hover:bg-muted",
        !inspection.ok && "ring-1 ring-destructive ring-inset",
        target && "bg-muted ring-1 ring-primary ring-inset",
      )}
    >
      <div className="flex items-center gap-3">
        <span className="min-w-0 flex-1">
          <span className={cn("block truncate font-mono text-sm", !inspection.ok && "text-destructive")}>{file.name}</span>
          <span className={cn("text-xs tabular-nums", inspection.ok ? "text-muted-foreground" : "text-destructive")}>
            {inspection.ok
              ? [DB_LABEL[inspection.dbType], formatSize(file.size), inspection.collectorVersion && `v${inspection.collectorVersion}`]
                  .filter(Boolean)
                  .join(" · ")
              : inspection.reason}
          </span>
        </span>
        {stage === null ? (
          <button type="button" aria-label="移除" onClick={onRemove} className="cursor-pointer text-[#5a5a5a] hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        ) : stage === "done" ? (
          <Check className="h-4 w-4 text-primary" />
        ) : (
          <span className="text-xs text-muted-foreground">{stage === "current" ? "生成中" : "排队中"}</span>
        )}
      </div>
      {slot && (diagnostics.length > 0 || stage === null) && (
        <div className="mt-2 flex flex-wrap items-center gap-1.5">
          {diagnostics.map((d) => (
            <span key={fileKey(d)} className="inline-flex max-w-full items-center gap-1.5 rounded-md bg-border px-2 py-1 text-xs">
              <span className="text-muted-foreground">{slot}</span>
              <span className="truncate font-mono">{d.name}</span>
              {stage === null && (
                <button
                  type="button"
                  aria-label={`取消配对 ${d.name}`}
                  onClick={() => onUnpair(d)}
                  className="cursor-pointer text-[#5a5a5a] hover:text-foreground"
                >
                  <X className="h-3 w-3" />
                </button>
              )}
            </span>
          ))}
          {stage === null && (slot === "WDR" || diagnostics.length === 0) && (
            <span className="text-xs text-[#5a5a5a]">{slot === "WDR" ? "可拖入多份 WDR（可选）" : "可拖入一份 AWR（可选）"}</span>
          )}
        </div>
      )}
      {notice && inspection.ok && <VersionNotice version={inspection.collectorVersion} notice={notice} />}
      {stage !== null && (
        <div className="mt-2.5 h-[3px] overflow-hidden rounded-full bg-border">
          <div className={cn("h-full bg-primary", stage === "done" ? "w-full" : stage === "current" ? "w-1/3 animate-pulse" : "w-0")} />
        </div>
      )}
    </div>
  );
}

/** Deprecated: a quiet notice. Revoked: a prominent warning with the revocation reason. */
function VersionNotice({ version, notice }: { version: string | null; notice: CollectorNotice }) {
  if (notice.status === "deprecated") {
    return <p className="mt-2 text-xs text-warning">采集器 v{version} 已弃用，建议下载最新版本重新采集。</p>;
  }
  return (
    <p className="mt-2 flex items-start gap-1.5 rounded-md bg-destructive/10 px-2.5 py-2 text-sm font-semibold text-destructive">
      <TriangleAlert className="mt-0.5 h-4 w-4 shrink-0" />
      <span>
        采集器 v{version} 已撤回：{notice.reason}
      </span>
    </p>
  );
}

/** 待配对 (Unpaired): dropped HTML files waiting to be dragged onto a report item row. */
function Unpaired({ files, onRemove }: { files: File[]; onRemove: (file: File) => void }) {
  return (
    <div className="mt-2 rounded-xl border border-dashed border-border px-4 py-3.5">
      <p className={CAPTION}>待配对 · {files.length}</p>
      <p className="mt-1 text-xs text-muted-foreground">把 AWR 拖到 Oracle 采集包上，WDR 拖到 GaussDB 采集包上。</p>
      <div className="mt-3 flex flex-wrap gap-2">
        {files.map((file) => (
          <span
            key={fileKey(file)}
            draggable
            onDragStart={(e) => {
              e.dataTransfer.setData(DIAGNOSTIC_DRAG, fileKey(file));
              e.dataTransfer.effectAllowed = "move";
            }}
            className="inline-flex max-w-full cursor-grab items-center gap-1.5 rounded-md bg-muted px-2.5 py-1.5 text-xs active:cursor-grabbing"
          >
            <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <span className="truncate font-mono">{file.name}</span>
            <button
              type="button"
              aria-label={`移除 ${file.name}`}
              onClick={() => onRemove(file)}
              className="cursor-pointer text-[#5a5a5a] hover:text-foreground"
            >
              <X className="h-3 w-3" />
            </button>
          </span>
        ))}
      </div>
    </div>
  );
}
