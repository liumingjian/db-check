"use client";

import { useEffect, useRef, useState } from "react";
import { ConsoleSection } from "@/components/console/console-shell";
import { CAPTION } from "@/components/console/kit";
import { ReportRow } from "@/components/console/reports/report-row";
import { api, type ReportTask } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";

/** While a listed task is still 生成中, re-read the list this often. */
const POLL_MS = 2000;

/**
 * 我的报告: the signed-in user's own report tasks, newest first, as a plain
 * hairline list. The list is re-read whenever the section scrolls into view,
 * so a task just submitted in the drop zone shows up, and polled while any
 * task is still generating, so 生成中 clears without a reload.
 */
export function ReportsSection() {
  const token = useAuthStore((s) => s.token);
  const sectionRef = useRef<HTMLDivElement>(null);
  const [inView, setInView] = useState(false);
  const [tasks, setTasks] = useState<ReportTask[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const el = sectionRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(([entry]) => setInView(entry.isIntersecting));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!token || !inView) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = () =>
      api.reports.listOwn(token).then(
        (list) => {
          if (cancelled) return;
          setTasks(list);
          setError(null);
          if (list.some((t) => t.status === "processing")) timer = setTimeout(load, POLL_MS);
        },
        (e: unknown) => {
          if (!cancelled) setError(e instanceof Error ? e.message : String(e));
        },
      );
    load();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [token, inView]);

  return (
    <ConsoleSection id="reports">
      <div ref={sectionRef} className="mx-auto grid max-w-[1240px] grid-cols-[1fr_1.6fr] gap-16 px-8 py-24">
        <div>
          <p className={CAPTION}>My reports</p>
          <h2 className="mt-4 text-[40px] leading-[1.15] font-bold tracking-[-1.5px]">
            忘了下载？
            <br />
            都在这里。
          </h2>
          <p className="mt-4 text-base text-muted-foreground">报告保留 30 天。</p>
        </div>
        <div className="divide-y divide-border border-y border-border">
          {error && <p className="py-10 text-sm text-destructive">{error}</p>}
          {!error && tasks?.length === 0 && <p className="py-10 text-sm text-muted-foreground">还没有生成过报告</p>}
          {tasks?.map((task) => <ReportRow key={task.id} task={task} />)}
        </div>
      </div>
    </ConsoleSection>
  );
}
