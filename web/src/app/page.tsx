"use client";

import { ConsoleShell } from "@/components/console/console-shell";
import { CollectorsSection } from "@/components/console/home/collectors-section";
import { GenerateSection } from "@/components/console/home/generate-section";
import { ReportsSection } from "@/components/console/home/reports-section";

/** The console home: one long page, sections in priority order (spec #19, Navigation). */
export default function HomePage() {
  return (
    <ConsoleShell>
      <GenerateSection />
      <CollectorsSection />
      <ReportsSection />
    </ConsoleShell>
  );
}
