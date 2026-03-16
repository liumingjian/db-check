"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { useReportStore } from "@/stores/report-store";
import { DB_TYPE_OPTIONS } from "@/lib/types";
import { FileUploadZone } from "@/components/file-upload-zone";
import { FilePairCard } from "@/components/file-pair-card";

export function FileUploadStep() {
  const dbType = useReportStore((s) => s.dbType);
  const zipFiles = useReportStore((s) => s.zipFiles);
  const awrFiles = useReportStore((s) => s.awrFiles);
  const addZipFiles = useReportStore((s) => s.addZipFiles);
  const removeZipFile = useReportStore((s) => s.removeZipFile);
  const setAwrFile = useReportStore((s) => s.setAwrFile);
  const prevStep = useReportStore((s) => s.prevStep);
  const nextStep = useReportStore((s) => s.nextStep);

  const option = DB_TYPE_OPTIONS.find((o) => o.type === dbType);
  const awrLabel = option?.awrWdrLabel ?? "";
  const hasAwrWdr = option?.hasAwrWdr ?? false;

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={prevStep}
            className="rounded-md p-1.5 text-muted-foreground hover:text-foreground hover:bg-muted cursor-pointer transition-colors duration-150"
            aria-label="返回"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          <h2 className="text-lg font-semibold">
            已选：{option?.label ?? ""} {option?.versions ?? ""}
          </h2>
        </div>
      </div>

      {/* Upload zone */}
      <FileUploadZone onFiles={addZipFiles} />

      {/* File list */}
      {zipFiles.length > 0 && (
        <div className="space-y-3">
          <p className="text-sm text-muted-foreground">
            已上传文件 ({zipFiles.length})
          </p>
          <div className="grid gap-3">
            {zipFiles.map((z) => (
              <FilePairCard
                key={z.id}
                zipName={z.name}
                zipSize={z.size}
                awrLabel={hasAwrWdr ? awrLabel : ""}
                awrFileName={awrFiles[z.id]?.name ?? null}
                onAwrSelect={(file) => setAwrFile(z.id, file)}
                onAwrRemove={() => setAwrFile(z.id, null)}
                onRemove={() => removeZipFile(z.id)}
              />
            ))}
          </div>
        </div>
      )}

      {/* Next button */}
      {zipFiles.length > 0 && (
        <div className="flex justify-center">
          <button
            type="button"
            onClick={nextStep}
            className={cn(
              "inline-flex items-center gap-2 rounded-lg px-6 py-2.5",
              "bg-primary text-primary-foreground font-medium",
              "hover:bg-primary/90 transition-colors duration-200",
              "cursor-pointer",
            )}
          >
            生成报告
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}
