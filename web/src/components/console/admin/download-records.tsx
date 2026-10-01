"use client";

import { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { appliedAtLabel } from "@/components/console/account/form";
import { Chip } from "@/components/console/kit";
import { api, ApiError, type Account, type CollectorRelease, type DownloadRecord, type ReleaseStatus } from "@/lib/api";
import type { DownloadRecordFilter } from "@/lib/api/downloads/contract";
import { DAY_MS } from "@/lib/time";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

/** URL query of 管理 → 下载记录; every key is optional. `days` keeps the last N days. */
export interface DownloadRecordsQuery {
  user?: string;
  release?: string;
  days?: string;
}

const QUERY_KEYS = ["user", "release", "days"] as const;

const DAY_CHOICES = ["7", "30", "90"] as const;

const STATUS_NOTE: Partial<Record<ReleaseStatus, { label: string; className: string }>> = {
  deprecated: { label: "已弃用", className: "text-warning" },
  "pre-release": { label: "预发布", className: "text-muted-foreground" },
  revoked: { label: "已撤回", className: "text-destructive" },
};

/** Link to 下载记录 with filters applied, e.g. from a release's `···` menu. */
export function downloadRecordsHref(query: DownloadRecordsQuery): string {
  const params = new URLSearchParams();
  for (const key of QUERY_KEYS) {
    const value = query[key];
    if (value) params.set(key, value);
  }
  const search = params.toString();
  return search ? `/admin/downloads?${search}` : "/admin/downloads";
}

/**
 * 管理 → 下载记录: every download record, filtered by user, release, and time
 * through the URL query. `now` (epoch ms) anchors the 近 N 天 filters.
 */
export function DownloadRecords({ now = Date.now }: { now?: () => number }) {
  const token = useAuthStore((s) => s.token);
  const router = useRouter();
  const params = useSearchParams();
  const query: DownloadRecordsQuery = {
    user: params.get("user") ?? undefined,
    release: params.get("release") ?? undefined,
    days: params.get("days") ?? undefined,
  };
  const [records, setRecords] = useState<DownloadRecord[] | null>(null);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [releases, setReleases] = useState<CollectorRelease[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!token) return;
    const fail = (e: unknown) => setError(errorText(e));
    api.users.list(token).then(setAccounts, fail);
    api.releases.list(token).then(setReleases, fail);
  }, [token]);

  const { user, release, days } = query;
  useEffect(() => {
    if (!token) return;
    api.downloads.records(token, toFilter({ user, release, days }, now())).then(setRecords, (e: unknown) => setError(errorText(e)));
  }, [token, user, release, days, now]);

  // Replace, not push: flipping chips shouldn't fill the back button's history.
  const set = (change: DownloadRecordsQuery) => router.replace(downloadRecordsHref({ ...query, ...change }));

  const nameOf = (userId: string) => accounts.find((a) => a.id === userId)?.displayName ?? userId;
  const statusOf = (version: string) => releases.find((r) => r.version === version)?.status;
  // Applicants never got console access, so they have no downloads to filter by.
  const people = accounts.filter((a) => a.status === "active" || a.status === "disabled");

  return (
    <>
      <div className="space-y-1">
        <FilterRow label="用户">
          <Chip on={!user} onClick={() => set({ user: undefined })}>全部</Chip>
          {people.map((a) => (
            <Chip key={a.id} on={user === a.id} onClick={() => set({ user: a.id })}>
              {a.displayName}
            </Chip>
          ))}
        </FilterRow>
        <FilterRow label="版本">
          <Chip on={!release} onClick={() => set({ release: undefined })}>全部</Chip>
          {releases.map((r) => (
            <Chip key={r.version} on={release === r.version} onClick={() => set({ release: r.version })}>
              v{r.version}
            </Chip>
          ))}
        </FilterRow>
        <FilterRow label="时间">
          <Chip on={!days} onClick={() => set({ days: undefined })}>全部</Chip>
          {DAY_CHOICES.map((d) => (
            <Chip key={d} on={days === d} onClick={() => set({ days: d })}>
              近 {d} 天
            </Chip>
          ))}
        </FilterRow>
      </div>

      {error && <p className="mt-8 text-sm text-destructive">{error}</p>}
      {records && (
        <>
          <p className="mt-8 text-sm text-muted-foreground tabular-nums">{records.length} 条记录</p>
          {records.length === 0 ? (
            <p className="mt-4 border-y border-border py-10 text-center text-sm text-muted-foreground">没有符合条件的下载记录。</p>
          ) : (
            <div className="mt-4 divide-y divide-border border-y border-border">
              {records.map((r) => {
                const note = STATUS_NOTE[statusOf(r.version) ?? "latest"];
                return (
                  <div key={r.id} className="flex items-center gap-6 py-4">
                    <span className="w-28 shrink-0 text-sm text-muted-foreground tabular-nums">{appliedAtLabel(r.at)}</span>
                    <span className="w-28 shrink-0 truncate text-sm font-semibold">{nameOf(r.userId)}</span>
                    <span className="w-28 shrink-0 text-base font-semibold tabular-nums">v{r.version}</span>
                    <span className="flex-1 font-mono text-sm text-[#ccc]">{r.platform}</span>
                    {note && <span className={cn("text-xs", note.className)}>{note.label}</span>}
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}
    </>
  );
}

function errorText(e: unknown): string {
  return e instanceof ApiError ? e.message : String(e);
}

function toFilter(query: DownloadRecordsQuery, now: number): DownloadRecordFilter {
  const days = Number(query.days);
  return {
    userId: query.user,
    version: query.release,
    from: days > 0 ? new Date(now - days * DAY_MS).toISOString() : undefined,
  };
}

function FilterRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-wrap items-center gap-2 py-2">
      <span className="w-12 shrink-0 text-xs font-semibold text-[#5a5a5a]">{label}</span>
      {children}
    </div>
  );
}
