"use client";

import { ArrowDown } from "lucide-react";
import { CopyText, EASE_OUT } from "@/components/console/kit";
import type { CollectorRelease, ReleasePackage } from "@/lib/api";
import { formatSize } from "@/lib/files";
import { DB_LABEL } from "@/lib/types";
import { cn } from "@/lib/utils";

/** The latest release: one tile per package, the SHA256 lines, tag, commit, date, and notes. */
export function LatestRelease({ release, onDownload }: { release: CollectorRelease; onDownload: (pkg: ReleasePackage) => void }) {
  return (
    <>
      <div className="mt-12 grid grid-cols-4 gap-4">
        {release.packages.map((pkg) => (
          <button
            key={pkg.platform}
            type="button"
            onClick={() => onDownload(pkg)}
            className={cn(
              "group flex h-60 cursor-pointer flex-col justify-between rounded-2xl bg-card p-6 text-left",
              `transition-[background-color,color,transform] duration-150 ${EASE_OUT} hover:bg-primary hover:text-primary-foreground active:scale-[0.98]`,
            )}
          >
            <span className="text-sm font-semibold text-muted-foreground group-hover:text-primary-foreground/60">{pkg.os}</span>
            <span className="text-[40px] leading-none font-bold tracking-[-1.5px]">{pkg.arch}</span>
            <span className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground tabular-nums group-hover:text-primary-foreground/60">
                {formatSize(pkg.size)} · .zip
              </span>
              <span className="flex items-center gap-1 font-semibold">
                下载 <ArrowDown className="h-4 w-4" />
              </span>
            </span>
          </button>
        ))}
      </div>
      <div className="mt-4 flex flex-wrap gap-x-8 gap-y-1">
        {release.packages.map((pkg) => (
          <CopyText
            key={pkg.platform}
            text={pkg.sha256}
            label={`${pkg.os} ${pkg.arch} sha256 ${pkg.sha256.slice(0, 12)}`}
            className="text-[#5a5a5a]"
          />
        ))}
      </div>
      <div className="mt-12 grid grid-cols-[1fr_1.6fr] gap-16 text-sm">
        <div className="space-y-2 text-muted-foreground">
          <p>
            发布于 {formatDate(release.publishedAt)} · <span className="font-mono">{release.tag}</span> ·{" "}
            <span className="font-mono">{release.commit}</span>
          </p>
          <p>支持数据库：{release.dbTypes.map((t) => DB_LABEL[t]).join(" / ")}</p>
        </div>
        <ReleaseNotes notes={release.notes} />
      </div>
    </>
  );
}

function ReleaseNotes({ notes }: { notes: string }) {
  const lines = notes
    .split("\n")
    .map((l) => l.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
  return (
    <ul className="space-y-1.5 text-[#ccc]">
      {lines.map((line) => (
        <li key={line} className="flex gap-3">
          <span className="text-primary">—</span>
          {line}
        </li>
      ))}
    </ul>
  );
}

/** In place of the latest release while an admin has deprecated or revoked it and promoted none. */
export function NoLatest({ hasOlder }: { hasOlder: boolean }) {
  return (
    <div className="mt-12 rounded-2xl bg-card p-10">
      <p className="text-[32px] leading-tight font-bold tracking-[-1px]">暂无推荐版本，请联系管理员</p>
      {hasOlder && <p className="mt-3 text-sm text-muted-foreground">下方历史版本仍可下载。</p>}
    </div>
  );
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("zh-CN");
}
