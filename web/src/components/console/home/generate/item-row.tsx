"use client";

import { useRef } from "react";
import { Check, TriangleAlert, X } from "lucide-react";
import type { CollectorNotice, DiagnosticValidation } from "@/lib/api";
import { formatSize } from "@/lib/files";
import { fileKey, type InspectedZip } from "@/lib/report-input/inspect";
import { DB_LABEL, type DbType } from "@/lib/types";
import { cn } from "@/lib/utils";

/** The HTML a row's slot takes, by database type; mysql rows have no slot. */
const SLOT_LABEL: Partial<Record<DbType, string>> = { oracle: "AWR", gaussdb: "WDR" };

/** A row's progress once submitted; `null` before submission. */
export type Stage = "done" | "current" | "queued" | null;

const REMOVE_BUTTON = "cursor-pointer text-[#5a5a5a] hover:text-foreground";

/**
 * One report item: file name, what its manifest says, its optional AWR/WDR files,
 * and its progress once submitted. Before submission it accepts diagnostic selections.
 */
export function ItemRow({ item: { file, inspection, diagnostics }, stage, notice, onRemove, onAttach, onRemoveDiagnostic, diagnosticCheck}: {
  item: InspectedZip;
  stage: Stage;
  /** Shown only while generating; report lists repeat only the revoked warning. */
  notice: CollectorNotice | null;
  onRemove: () => void;
  onAttach: (files: File[]) => void;
  onRemoveDiagnostic: (file: File) => void;
  diagnosticCheck: (file: File) => DiagnosticValidation | null;
}) {
  const slot = inspection.ok ? SLOT_LABEL[inspection.dbType] : undefined;

  return (
    <div
      className={cn(
        "rounded-xl px-4 py-3.5 hover:bg-muted",
        !inspection.ok && "ring-1 ring-destructive ring-inset",
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
        <StageMark stage={stage} onRemove={onRemove} />
      </div>
      {slot && (diagnostics.length > 0 || stage === null) && (
        <DiagnosticFiles slot={slot} files={diagnostics} editable={stage === null} onAttach={onAttach} onRemoveDiagnostic={onRemoveDiagnostic} diagnosticCheck={diagnosticCheck} />
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

/** × before submission; then ✓, 生成中 or 排队中. */
function StageMark({ stage, onRemove }: { stage: Stage; onRemove: () => void }) {
  if (stage === null) {
    return (
      <button type="button" aria-label="移除" onClick={onRemove} className={REMOVE_BUTTON}>
        <X className="h-4 w-4" />
      </button>
    );
  }
  if (stage === "done") return <Check className="h-4 w-4 text-primary" />;
  return <span className="text-xs text-muted-foreground">{stage === "current" ? "生成中" : "排队中"}</span>;
}

/** Files selected on this item, with individual removal and an optional picker. */
function DiagnosticFiles({ slot, files, editable, onAttach, onRemoveDiagnostic, diagnosticCheck }: {
  slot: string;
  files: File[];
  editable: boolean;
  onAttach: (files: File[]) => void;
  onRemoveDiagnostic: (file: File) => void;
  diagnosticCheck: (file: File) => DiagnosticValidation | null;
}) {
  const picker = useRef<HTMLInputElement>(null);
  const full = slot === "AWR" && files.length > 0;
  return (
    <div className="mt-2 space-y-2">
      <div className="flex flex-wrap items-center gap-1.5">
        {files.map((file) => (
          <span key={fileKey(file)} className="inline-flex max-w-full items-center gap-1.5 rounded-md bg-border px-2 py-1 text-xs">
            <span className="text-muted-foreground">{slot}</span>
            <span className="truncate font-mono">{file.name}</span>
            {editable && <button type="button" aria-label={`移除 ${file.name}`} onClick={() => onRemoveDiagnostic(file)} className={REMOVE_BUTTON}><X className="h-3 w-3" /></button>}
          </span>
        ))}
        {editable && <button type="button" disabled={full} onClick={() => picker.current?.click()} className="cursor-pointer rounded-md border border-border px-2.5 py-1.5 text-xs hover:bg-muted disabled:cursor-default disabled:opacity-50">添加 {slot}</button>}
      </div>
      {files.map((file) => {
        const check = diagnosticCheck(file);
        return check?.kind === "invalid" ? <p key={fileKey(file)} role="alert" className="text-xs text-destructive">{file.name}：{check.message}</p> : null;
      })}
      {editable && <>
        <input ref={picker} type="file" accept=".html,.htm" multiple={slot === "WDR"} aria-label={`选择 ${slot} 文件`} className="hidden" onChange={(event) => {
          onAttach(Array.from(event.target.files ?? []));
          event.target.value = "";
        }} />
        <p className="text-xs text-muted-foreground">可从其他平台导出后添加（可选）；未添加时，仅根据采集包生成报告。</p>
        <p className="text-xs text-muted-foreground">{slot === "AWR" ? "每个采集包支持一份 AWR，更换前请先移除已有附件。" : "每个采集包可添加多份 WDR。"}</p>
      </>}
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
