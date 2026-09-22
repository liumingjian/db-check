"use client";

import { ChevronRight, Wrench, Info } from "lucide-react";
import { cn } from "@/lib/utils";
import { useReportStore } from "@/stores/report-store";
import { useNavStore } from "@/stores/nav-store";
import { FileUploadZone } from "@/components/file-upload-zone";
import { FilePairCard } from "@/components/file-pair-card";

export function FileUploadStep() {
  const zipFiles = useReportStore((s) => s.zipFiles);
  const awrFiles = useReportStore((s) => s.awrFiles);
  const addZipFiles = useReportStore((s) => s.addZipFiles);
  const removeZipFile = useReportStore((s) => s.removeZipFile);
  const setAwrFile = useReportStore((s) => s.setAwrFile);
  const nextStep = useReportStore((s) => s.nextStep);
  const setActiveTab = useNavStore((s) => s.setActiveTab);

  return (
    <div className="flex flex-col gap-6">
      {/* Header & Description */}
      <div className="space-y-1">
        <h2 className="text-xl font-bold tracking-tight">上传巡检数据包</h2>
        <p className="text-sm text-muted-foreground">
          支持上传各数据库（MySQL、Oracle、GaussDB、PostgreSQL、达梦等）统一采集生成的指标 ZIP 包，平台引擎将通过 manifest 自动识别数据库类型并执行分析判定。
        </p>
      </div>

      {/* Guide Banner */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 rounded-lg border border-primary/20 bg-primary/5 px-4 py-3 text-xs">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Info className="h-4 w-4 text-primary shrink-0" />
          <span>尚未在内网完成指标采集？请先下载对应架构采集器并在主机执行。</span>
        </div>
        <button
          type="button"
          onClick={() => setActiveTab("tools")}
          className="inline-flex items-center gap-1.5 font-medium text-primary hover:underline cursor-pointer shrink-0"
        >
          <Wrench className="h-3.5 w-3.5" />
          前往巡检工具下载 →
        </button>
      </div>

      {/* Upload zone */}
      <FileUploadZone onFiles={addZipFiles} />

      {/* File list */}
      {zipFiles.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium">
              已选采集包 ({zipFiles.length})
            </p>
            <span className="text-xs text-muted-foreground">
              若采集包内已包含诊断文件（AWR/WDR），无需单独挂载
            </span>
          </div>

          <div className="grid gap-3">
            {zipFiles.map((z) => (
              <FilePairCard
                key={z.id}
                zipName={z.name}
                zipSize={z.size}
                awrLabel="关联诊断文件 (AWR/WDR/pg_profile，可选)"
                awrFileNames={(awrFiles[z.id] ?? []).map((file) => file.name)}
                allowMultiple={true}
                onAwrSelect={(files) => setAwrFile(z.id, files)}
                onAwrRemove={() => setAwrFile(z.id, null)}
                onRemove={() => removeZipFile(z.id)}
              />
            ))}
          </div>
        </div>
      )}

      {/* Next button */}
      {zipFiles.length > 0 && (
        <div className="flex justify-center pt-2">
          <button
            type="button"
            onClick={nextStep}
            className={cn(
              "inline-flex items-center gap-2 rounded-lg px-8 py-2.5",
              "bg-primary text-primary-foreground font-medium text-sm",
              "hover:bg-primary/90 transition-colors duration-200 shadow-md",
              "cursor-pointer",
            )}
          >
            开始生成报告
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}

