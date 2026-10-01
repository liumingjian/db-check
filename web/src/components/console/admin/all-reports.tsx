"use client";

import { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Chip } from "@/components/console/kit";
import { ReportRow } from "@/components/console/reports/report-row";
import { api, ApiError, type Account, type ReportTask } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";

/**
 * 管理 → 全部报告: every user's report tasks with a submitter column, narrowed
 * by a submitter chip kept in the URL query (`?user=<id>`). Rows are the same
 * as 我的报告: markers, failure reasons, re-download.
 */
export function AllReports() {
  const token = useAuthStore((s) => s.token);
  const router = useRouter();
  const user = useSearchParams().get("user") ?? undefined;
  const [tasks, setTasks] = useState<ReportTask[] | null>(null);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (token) api.users.list(token).then(setAccounts, (e: unknown) => setError(errorText(e)));
  }, [token]);

  useEffect(() => {
    if (token) api.reports.listAll(token, { submitterId: user }).then(setTasks, (e: unknown) => setError(errorText(e)));
  }, [token, user]);

  // Replace, not push: flipping chips shouldn't fill the back button's history.
  const show = (id?: string) => router.replace(id ? `/admin/reports?${new URLSearchParams({ user: id })}` : "/admin/reports");

  // Applicants never got console access, so they have no tasks. Disabled users keep theirs.
  const submitters = accounts.filter((a) => a.status === "active" || a.status === "disabled");

  return (
    <>
      <div className="flex flex-wrap items-center gap-2 py-2">
        <span className="w-12 shrink-0 text-xs font-semibold text-[#5a5a5a]">提交人</span>
        <Chip on={!user} onClick={() => show()}>全部</Chip>
        {submitters.map((a) => (
          <Chip key={a.id} on={user === a.id} onClick={() => show(a.id)}>
            {a.displayName}
          </Chip>
        ))}
      </div>

      {error && <p className="mt-8 text-sm text-destructive">{error}</p>}
      {tasks && (
        <>
          <p className="mt-8 text-sm text-muted-foreground tabular-nums">{tasks.length} 个报告任务</p>
          {tasks.length === 0 ? (
            <p className="mt-4 border-y border-border py-10 text-center text-sm text-muted-foreground">没有符合条件的报告。</p>
          ) : (
            <div className="mt-4 divide-y divide-border border-y border-border">
              {tasks.map((task) => (
                <ReportRow key={task.id} task={task} showSubmitter />
              ))}
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
