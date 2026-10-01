"use client";

import { ArrowRight } from "lucide-react";
import { ItemRow, type Stage } from "@/components/console/home/generate/item-row";
import { Unpaired } from "@/components/console/home/generate/unpaired";
import type { useReportInputs } from "@/components/console/home/generate/use-report-inputs";
import type { Run } from "@/components/console/home/generate/use-report-run";
import { YellowButton } from "@/components/console/kit";
import { REPORT_RETENTION_DAYS } from "@/lib/api";

const STATS = [
  ["3", "种数据库", "MySQL · Oracle · GaussDB"],
  ["4", "个平台", "Linux / Windows · x86 / ARM"],
  [String(REPORT_RETENTION_DAYS), "天保留", "在「我的报告」随时重新下载"],
] as const;

const READING = "正在读取 manifest…";

/**
 * The section's right column: the platform stats while empty, then one row per
 * report item, 待配对, and the submit button until a task runs.
 */
export function ItemsPanel({
  inputs,
  run,
  submitting,
  onSubmit,
}: {
  inputs: ReturnType<typeof useReportInputs>;
  run: Run | null;
  submitting: boolean;
  onSubmit: () => void;
}) {
  const { items, unpaired, inspecting } = inputs;
  if (items.length === 0 && unpaired.length === 0) {
    return inspecting ? <p className="text-sm text-muted-foreground">{READING}</p> : <Stats />;
  }
  return (
    <div className="rounded-2xl bg-card p-2">
      {items.map((item, index) => (
        <ItemRow
          key={item.file.name}
          item={item}
          stage={stageOf(run, index)}
          notice={run?.notices[index] ?? null}
          onRemove={() => inputs.removeItem(item)}
          onPair={(key) => inputs.pair(item, key)}
          onUnpair={(file) => inputs.unpair(item, file)}
        />
      ))}
      {run?.noticesError && (
        <p className="px-4 pt-3 pb-1 text-xs text-muted-foreground">采集器版本提示读取失败：{run.noticesError}</p>
      )}
      {!run && unpaired.length > 0 && <Unpaired files={unpaired} onRemove={inputs.removeUnpaired} />}
      {inspecting && <p className="px-4 py-3.5 text-xs text-muted-foreground">{READING}</p>}
      {!run && <SubmitBar inputs={inputs} submitting={submitting} onSubmit={onSubmit} />}
    </div>
  );
}

function SubmitBar({
  inputs: { items, unpaired, inspecting, taskInput },
  submitting,
  onSubmit,
}: {
  inputs: ReturnType<typeof useReportInputs>;
  submitting: boolean;
  onSubmit: () => void;
}) {
  return (
    <>
      <YellowButton onClick={onSubmit} disabled={!taskInput || inspecting || submitting} className="mt-2 h-14 w-full text-base">
        生成 {items.length} 份报告 <ArrowRight className="h-4 w-4" />
      </YellowButton>
      {items.some((i) => !i.inspection.ok) && <p className="px-4 pt-3 pb-1 text-xs text-destructive">移除标红的采集包后才能生成。</p>}
      {unpaired.length > 0 && (
        <p className="px-4 pt-3 pb-1 text-xs text-muted-foreground">把待配对的文件拖到对应的采集包上，或移除它们，才能生成。</p>
      )}
    </>
  );
}

function Stats() {
  return (
    <div className="grid grid-cols-2 gap-x-10 gap-y-12">
      {STATS.map(([n, unit, sub]) => (
        <div key={unit}>
          <p className="text-[56px] leading-none font-bold tracking-[-1.5px] text-primary tabular-nums">{n}</p>
          <p className="mt-2 text-base font-semibold">{unit}</p>
          <p className="mt-0.5 text-sm text-muted-foreground">{sub}</p>
        </div>
      ))}
    </div>
  );
}

function stageOf(run: Run | null, index: number): Stage {
  if (!run) return null;
  return index < run.completed ? "done" : index === run.completed ? "current" : "queued";
}
