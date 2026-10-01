"use client";

import { useState } from "react";
import { Check, TriangleAlert, X } from "lucide-react";
import type { CollectorNotice } from "@/lib/api";
import { formatSize } from "@/lib/files";
import { fileKey, type InspectedZip } from "@/lib/report-input/inspect";
import { DB_LABEL, type DbType } from "@/lib/types";
import { cn } from "@/lib/utils";

/** Drag type for an HTML moved from 待配对 onto a row; the payload is its `fileKey`. */
export const DIAGNOSTIC_DRAG = "application/x-dbcheck-diagnostic";

/** The HTML a row's slot takes, by database type; mysql rows have no slot. */
const SLOT_LABEL: Partial<Record<DbType, string>> = { oracle: "AWR", gaussdb: "WDR" };

/** A row's progress once submitted; `null` before submission. */
export type Stage = "done" | "current" | "queued" | null;

const REMOVE_BUTTON = "cursor-pointer text-[#5a5a5a] hover:text-foreground";

/**
 * One report item: file name, what its manifest says, its paired AWR/WDR files,
 * and its progress once submitted. Before submission it takes drops from 待配对.
 */
export function ItemRow({
  item: { file, inspection, diagnostics },
  stage,
  notice,
  onRemove,
  onPair,
  onUnpair,
}: {
  item: InspectedZip;
  stage: Stage;
  /** Shown only while generating; report lists repeat only the revoked warning. */
  notice: CollectorNotice | null;
  onRemove: () => void;
  onPair: (key: string) => void;
  onUnpair: (file: File) => void;
}) {
  const { target, dropProps } = useDiagnosticDrop(stage === null, onPair);
  const slot = inspection.ok ? SLOT_LABEL[inspection.dbType] : undefined;

  return (
    <div
      {...dropProps}
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
        <StageMark stage={stage} onRemove={onRemove} />
      </div>
      {slot && (diagnostics.length > 0 || stage === null) && (
        <PairedFiles slot={slot} files={diagnostics} editable={stage === null} onUnpair={onUnpair} />
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

/** Lets a row take a 待配对 file dragged onto it, while `enabled`. */
function useDiagnosticDrop(enabled: boolean, onPair: (key: string) => void) {
  const [target, setTarget] = useState(false);
  const carriesDiagnostic = (e: React.DragEvent) => e.dataTransfer.types.includes(DIAGNOSTIC_DRAG);
  const dropProps = enabled
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
  return { target, dropProps };
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

/** The row's paired AWR/WDR chips and, while editable, a hint for its free slot. */
function PairedFiles({
  slot,
  files,
  editable,
  onUnpair,
}: {
  slot: string;
  files: File[];
  editable: boolean;
  onUnpair: (file: File) => void;
}) {
  return (
    <div className="mt-2 flex flex-wrap items-center gap-1.5">
      {files.map((d) => (
        <span key={fileKey(d)} className="inline-flex max-w-full items-center gap-1.5 rounded-md bg-border px-2 py-1 text-xs">
          <span className="text-muted-foreground">{slot}</span>
          <span className="truncate font-mono">{d.name}</span>
          {editable && (
            <button type="button" aria-label={`取消配对 ${d.name}`} onClick={() => onUnpair(d)} className={REMOVE_BUTTON}>
              <X className="h-3 w-3" />
            </button>
          )}
        </span>
      ))}
      {editable && (slot === "WDR" || files.length === 0) && (
        <span className="text-xs text-[#5a5a5a]">{slot === "WDR" ? "可拖入多份 WDR（可选）" : "可拖入一份 AWR（可选）"}</span>
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
