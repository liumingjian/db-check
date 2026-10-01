// PROTOTYPE — throwaway. Small leaf pieces shared by the console variants.
// Layout is deliberately NOT shared; each variant owns its own structure.
"use client";

import { useState } from "react";
import { AlertTriangle, Check, Copy, FileBarChart, OctagonAlert } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  ACCOUNT_STATUS_CLASS,
  ACCOUNT_STATUS_LABEL,
  RELEASE_STATUS_CLASS,
  RELEASE_STATUS_LABEL,
  TASK_STATUS_CLASS,
  TASK_STATUS_LABEL,
  type AccountStatus,
  type ReleaseStatus,
  type ReportTask,
} from "./console-state";

export function Pill({ className, children }: { className: string; children: React.ReactNode }) {
  return (
    <span className={cn("inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold whitespace-nowrap", className)}>
      {children}
    </span>
  );
}

export const ReleasePill = ({ status }: { status: ReleaseStatus }) => (
  <Pill className={RELEASE_STATUS_CLASS[status]}>{RELEASE_STATUS_LABEL[status]}</Pill>
);
export const AccountPill = ({ status }: { status: AccountStatus }) => (
  <Pill className={ACCOUNT_STATUS_CLASS[status]}>{ACCOUNT_STATUS_LABEL[status]}</Pill>
);
export const TaskPill = ({ status }: { status: ReportTask["status"] }) => (
  <Pill className={TASK_STATUS_CLASS[status]}>{TASK_STATUS_LABEL[status]}</Pill>
);

export function CopySha({ sha, short = true }: { sha: string; short?: boolean }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      title={sha}
      onClick={() => {
        void navigator.clipboard?.writeText(sha);
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      }}
      className="inline-flex items-center gap-1 font-mono text-[11px] text-muted-foreground hover:text-foreground cursor-pointer"
    >
      {short ? `${sha.slice(0, 12)}…` : sha}
      {copied ? <Check className="h-3 w-3 text-primary" /> : <Copy className="h-3 w-3" />}
    </button>
  );
}

export function VersionNotice({ notice }: { notice: { tone: "warn" | "danger"; text: string } | null }) {
  if (!notice) return null;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[11px]",
        notice.tone === "warn" ? "bg-warning/10 text-warning" : "bg-destructive/15 text-destructive font-semibold",
      )}
    >
      {notice.tone === "warn" ? <AlertTriangle className="h-3 w-3" /> : <OctagonAlert className="h-3 w-3" />}
      {notice.text}
    </span>
  );
}

export const QUICKSTART = [
  "将对应平台的采集器包上传到客户数据库主机并解压",
  "执行 ./db-collector --db-type mysql|oracle|gaussdb --host … 生成诊断 ZIP",
  "（可选）同时导出 AWR / WDR 报告",
  "回到平台「生成报告」上传 ZIP，生成巡检报告",
];

export function NewReportPlaceholder() {
  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-xl border border-dashed border-border py-20 text-center">
      <FileBarChart className="h-8 w-8 text-muted-foreground" />
      <p className="text-sm text-muted-foreground">生成报告：沿用现有上传与生成流程，本原型不涉及</p>
      <p className="text-xs text-muted-foreground">数据库类型仅 MySQL / Oracle / GaussDB</p>
    </div>
  );
}
