"use client";

import { FileText, X } from "lucide-react";
import { DIAGNOSTIC_DRAG } from "@/components/console/home/generate/item-row";
import { CAPTION } from "@/components/console/kit";
import { fileKey } from "@/lib/report-input/inspect";

/** 待配对 (Unpaired): dropped HTML files waiting to be dragged onto a report item row. */
export function Unpaired({ files, onRemove }: { files: File[]; onRemove: (file: File) => void }) {
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
