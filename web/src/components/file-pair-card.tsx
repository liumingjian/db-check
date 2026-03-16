"use client";

import { useRef } from "react";
import { FileArchive, FileText, X, Check } from "lucide-react";
import { cn } from "@/lib/utils";

interface FilePairCardProps {
  zipName: string;
  zipSize: number;
  awrLabel: string;
  awrFileName: string | null;
  onAwrSelect: (file: File) => void;
  onAwrRemove: () => void;
  onRemove: () => void;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function FilePairCard({
  zipName,
  zipSize,
  awrLabel,
  awrFileName,
  onAwrSelect,
  onAwrRemove,
  onRemove,
}: FilePairCardProps) {
  const awrInputRef = useRef<HTMLInputElement>(null);

  return (
    <div className="group relative rounded-lg border border-border bg-card p-4 transition-colors duration-150 hover:border-primary/30">
      {/* Delete button */}
      <button
        type="button"
        onClick={onRemove}
        className={cn(
          "absolute top-3 right-3 rounded-md p-1",
          "text-muted-foreground hover:text-destructive hover:bg-destructive/10",
          "cursor-pointer transition-colors duration-150",
        )}
        aria-label={`移除 ${zipName}`}
      >
        <X className="h-4 w-4" />
      </button>

      {/* ZIP info */}
      <div className="flex items-center gap-3 pr-8">
        <FileArchive className="h-5 w-5 shrink-0 text-accent" />
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium">{zipName}</p>
          <p className="text-xs text-muted-foreground">
            {formatSize(zipSize)}
          </p>
        </div>
      </div>

      {/* AWR/WDR pairing row */}
      {awrLabel && (
        <div className="mt-3 flex items-center gap-2 border-t border-border pt-3">
          <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
          <span className="text-xs text-muted-foreground whitespace-nowrap">
            {awrLabel}:
          </span>

          {awrFileName ? (
            <div className="flex items-center gap-1.5 min-w-0">
              <Check className="h-3.5 w-3.5 shrink-0 text-primary" />
              <span className="truncate text-xs text-primary">
                {awrFileName}
              </span>
              <button
                type="button"
                onClick={onAwrRemove}
                className="shrink-0 rounded p-0.5 text-muted-foreground hover:text-destructive cursor-pointer"
                aria-label={`移除 ${awrLabel}`}
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          ) : (
            <button
              type="button"
              onClick={() => awrInputRef.current?.click()}
              className={cn(
                "rounded px-2 py-0.5 text-xs",
                "border border-dashed border-border",
                "text-muted-foreground hover:text-foreground hover:border-primary/50",
                "cursor-pointer transition-colors duration-150",
              )}
            >
              选择 HTML 文件
            </button>
          )}

          <input
            ref={awrInputRef}
            type="file"
            accept=".html,.htm"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              if (file) onAwrSelect(file);
              e.target.value = "";
            }}
          />
        </div>
      )}
    </div>
  );
}
