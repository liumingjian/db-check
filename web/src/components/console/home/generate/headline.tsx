"use client";

import { ArrowDown, ArrowRight } from "lucide-react";
import type { Run } from "@/components/console/home/generate/use-report-run";
import { CAPTION, PRESS, YellowButton } from "@/components/console/kit";
import { REPORT_RETENTION_DAYS } from "@/lib/api";
import { cn } from "@/lib/utils";

const RESTART_LINK = "cursor-pointer text-base font-semibold underline underline-offset-4";

/** The section's left column before submission: the pitch and the file picker. */
export function Intro({ onPick }: { onPick: () => void }) {
  return (
    <>
      <h1 className="text-display font-bold">
        拖进 ZIP，
        <br />
        <span className="text-primary">拿走报告。</span>
      </h1>
      <p className="mt-8 max-w-md text-lg leading-relaxed text-subtle-foreground">
        把采集器生成的 ZIP 拖到这一屏任意位置。数据库类型自动识别，一次可以放多台主机。Oracle 的 AWR、GaussDB
        的 WDR 可在识别数据库类型后，添加到对应采集包。
      </p>
      <div className="mt-10">
        <YellowButton onClick={onPick}>
          选择采集包 <ArrowRight className="h-4 w-4" />
        </YellowButton>
      </div>
    </>
  );
}

/** The left column while a task generates: the overall percentage, or the error and 重新开始. */
export function RunProgress({ run, onRestart }: { run: Run; onRestart: () => void }) {
  const failed = run.outcome === "error";
  const overall = run.total > 0 ? Math.round((run.completed / run.total) * 100) : 0;
  return (
    <>
      <p className={CAPTION}>{failed ? "生成失败" : "正在生成"}</p>
      <p
        className={cn(
          "mt-4 text-giant font-bold tabular-nums",
          failed ? "text-destructive" : "text-primary",
        )}
      >
        {overall}
        <span className="text-[0.4em] tracking-[-0.03em]">%</span>
      </p>
      {failed ? (
        <>
          <p className="mt-6 text-lg text-destructive">{run.error}</p>
          <button type="button" onClick={onRestart} className={cn("mt-6", RESTART_LINK)}>
            重新开始
          </button>
        </>
      ) : (
        <p className="mt-6 text-lg text-subtle-foreground tabular-nums">
          {run.completed} / {run.total} 份报告
        </p>
      )}
    </>
  );
}

/** The whole section once the task is done: «报告好了。», download, and 再来一份. */
export function DoneBand({
  run,
  downloading,
  onDownload,
  onRestart,
}: {
  run: Run;
  downloading: boolean;
  onDownload: () => void;
  onRestart: () => void;
}) {
  return (
    <div className="flex min-h-[min(100vh,860px)] flex-col bg-primary text-primary-foreground">
      <div className="mx-auto flex w-full max-w-[1240px] flex-1 flex-col justify-center px-8 py-16">
        <p className="text-[12px] font-semibold tracking-[1.5px] uppercase opacity-60">{run.taskId}</p>
        <h1 className="mt-4 text-display font-bold">报告好了。</h1>
        <p className="mt-6 text-lg opacity-70">
          {run.total} 份报告 · {REPORT_RETENTION_DAYS} 天内可在「我的报告」重新下载
        </p>
        <div className="mt-12 flex items-center gap-6">
          <button
            type="button"
            disabled={downloading}
            onClick={onDownload}
            className={cn(
              "inline-flex h-16 cursor-pointer items-center gap-3 rounded-xl bg-background px-8 text-lg font-semibold text-primary disabled:opacity-40",
              PRESS,
            )}
          >
            下载报告 <ArrowDown className="h-5 w-5" />
          </button>
          <button type="button" onClick={onRestart} className={RESTART_LINK}>
            再来一份
          </button>
        </div>
      </div>
    </div>
  );
}
