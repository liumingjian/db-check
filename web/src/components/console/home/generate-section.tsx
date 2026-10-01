"use client";

import { ReportPage } from "@/components/report-page";
import { ConsoleSection } from "@/components/console/console-shell";
import { CAPTION } from "@/components/console/kit";

/** 生成报告. Placeholder holding the old wizard until the drop zone replaces it (#27). */
export function GenerateSection() {
  return (
    <ConsoleSection id="new-report">
      <div className="mx-auto max-w-[1240px] px-8 py-24">
        <p className={CAPTION}>New report</p>
        <h2 className="mt-4 mb-12 text-[56px] leading-[1.05] font-bold tracking-[-2px]">生成报告</h2>
        <div className="max-w-4xl">
          <ReportPage />
        </div>
      </div>
    </ConsoleSection>
  );
}
