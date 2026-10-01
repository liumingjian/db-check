"use client";

import { Download } from "lucide-react";
import { useState } from "react";
import { Menu, type MenuItem } from "@/components/console/kit";
import type { CollectorRelease, ReleasePackage, ReleaseStatus } from "@/lib/api";
import { cn } from "@/lib/utils";

const HISTORY_STATUS: Record<Exclude<ReleaseStatus, "latest">, { label: string; className: string }> = {
  deprecated: { label: "已弃用 · 建议使用最新版本", className: "text-warning" },
  "pre-release": { label: "预发布 · 仅管理员可见", className: "text-muted-foreground" },
  revoked: { label: "已撤回", className: "text-destructive" },
};

/** 历史版本: every release but the latest, collapsed unless there is no latest. */
export function OlderReleases({
  releases,
  defaultOpen,
  menuFor,
  onDownload,
}: {
  releases: CollectorRelease[];
  defaultOpen: boolean;
  menuFor: (release: CollectorRelease) => MenuItem[];
  onDownload: (release: CollectorRelease, pkg: ReleasePackage) => void;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="mt-16">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className="cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground"
      >
        历史版本 {open ? "↑" : "↓"}
      </button>
      {open && (
        <div className="mt-4 divide-y divide-border border-y border-border">
          {releases.map((r) => {
            const status = r.status === "latest" ? null : HISTORY_STATUS[r.status];
            return (
              <div key={r.version} className="flex items-center gap-6 py-4 text-sm">
                <span className="w-28 text-base font-semibold tabular-nums">v{r.version}</span>
                <span className={cn("w-80 truncate text-sm", status?.className)} title={r.revokeReason}>
                  {status?.label}
                  {r.revokeReason && ` · ${r.revokeReason}`}
                </span>
                <span className="flex flex-1 flex-wrap gap-x-5 gap-y-2 text-sm">
                  {r.packages.map((pkg) => (
                    <button
                      key={pkg.platform}
                      type="button"
                      onClick={() => onDownload(r, pkg)}
                      className="inline-flex cursor-pointer items-center gap-1.5 text-muted-foreground underline-offset-4 hover:text-foreground hover:underline"
                    >
                      <Download className="size-3.5" aria-hidden />
                      {pkg.os} {pkg.arch}
                    </button>
                  ))}
                </span>
                <Menu items={menuFor(r)} label={`v${r.version} 的操作`} />
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
