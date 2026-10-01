"use client";

import { DialogHost } from "@/components/console/dialog-host";
import { Rail, RAIL_WIDTH } from "@/components/console/rail";
import { SessionGuard, type RouteAccess } from "@/components/console/session-guard";
import type { SectionKey } from "@/components/console/sections";

/** Every signed-in screen: the session guard, the left rail, and the dialog host. */
export function ConsoleShell({ access = "console", children }: { access?: Exclude<RouteAccess, "public">; children: React.ReactNode }) {
  return (
    <SessionGuard access={access}>
      <DialogHost>
        <Rail />
        <div className={RAIL_WIDTH}>
          {children}
          <footer className="border-t border-border px-8 py-10 pb-32 text-center text-xs text-[#5a5a5a]">DB-Check · 数据库巡检平台</footer>
        </div>
      </DialogHost>
    </SessionGuard>
  );
}

/** One band of the home page; `id` is its anchor and the rail's section key. */
export function ConsoleSection({ id, children }: { id: SectionKey; children: React.ReactNode }) {
  return (
    <section id={id} className="min-h-[60vh] border-t border-border first:border-t-0">
      {children}
    </section>
  );
}
