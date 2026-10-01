"use client";

import { ToolsPage } from "@/components/tools-page";
import { ConsoleSection } from "@/components/console/console-shell";
import { CAPTION } from "@/components/console/kit";

/** 采集器. Placeholder holding the old tools page until the collector tiles replace it (#24). */
export function CollectorsSection() {
  return (
    <ConsoleSection id="collectors">
      <div className="mx-auto max-w-[1240px] px-8 py-24">
        <p className={CAPTION}>Collector</p>
        <h2 className="mt-4 mb-12 text-[56px] leading-[1.05] font-bold tracking-[-2px]">采集器</h2>
        <ToolsPage />
      </div>
    </ConsoleSection>
  );
}
