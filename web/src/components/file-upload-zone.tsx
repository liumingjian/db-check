"use client";

import { useCallback, useRef, useState } from "react";
import { Upload } from "lucide-react";
import { cn } from "@/lib/utils";

interface FileUploadZoneProps {
  onFiles: (files: File[]) => void;
  accept?: string;
  label?: string;
  hint?: string;
}

export function FileUploadZone({
  onFiles,
  accept = ".zip",
  label = "拖拽 ZIP 采集包到此处，或点击选择文件",
  hint = "支持批量选择多个 .zip 文件",
}: FileUploadZoneProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFiles = useCallback(
    (fileList: FileList | null) => {
      if (!fileList) return;
      const files = Array.from(fileList).filter((f) =>
        f.name.toLowerCase().endsWith(".zip"),
      );
      if (files.length > 0) onFiles(files);
    },
    [onFiles],
  );

  return (
    <div
      role="button"
      tabIndex={0}
      onDragOver={(e) => {
        e.preventDefault();
        setIsDragOver(true);
      }}
      onDragLeave={() => setIsDragOver(false)}
      onDrop={(e) => {
        e.preventDefault();
        setIsDragOver(false);
        handleFiles(e.dataTransfer.files);
      }}
      onClick={() => inputRef.current?.click()}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") inputRef.current?.click();
      }}
      className={cn(
        "relative flex flex-col items-center justify-center gap-3",
        "rounded-xl border-2 border-dashed p-10",
        "cursor-pointer transition-all duration-200",
        isDragOver
          ? "drag-over border-primary bg-primary/5"
          : "border-border hover:border-primary/50 hover:bg-card/50",
      )}
    >
      <Upload
        className={cn(
          "h-10 w-10 transition-colors duration-200",
          isDragOver ? "text-primary" : "text-muted-foreground",
        )}
      />
      <p className="text-sm font-medium">{label}</p>
      <p className="text-xs text-muted-foreground">{hint}</p>

      <input
        ref={inputRef}
        type="file"
        accept={accept}
        multiple
        className="hidden"
        onChange={(e) => handleFiles(e.target.files)}
      />
    </div>
  );
}
