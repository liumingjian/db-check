"use client";

import { HistoryPage } from "@/components/history-page";
import { ConsoleSection } from "@/components/console/console-shell";
import { CAPTION } from "@/components/console/kit";

/** 我的报告. Placeholder holding the old history page until the report list replaces it (#29). */
export function ReportsSection() {
  return (
    <ConsoleSection id="reports">
      <div className="mx-auto max-w-[1240px] px-8 py-24">
        <p className={CAPTION}>My reports</p>
        <h2 className="mt-4 mb-12 text-[56px] leading-[1.05] font-bold tracking-[-2px]">我的报告</h2>
        <HistoryPage />
      </div>
    </ConsoleSection>
  );
}
