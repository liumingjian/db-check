"use client";

import { ConsoleShell } from "@/components/console/console-shell";
import { AdminTabs } from "@/components/console/admin/admin-tabs";
import { CAPTION } from "@/components/console/kit";

/** The 管理 view: admins only, tabs 用户 / 下载记录 / 全部报告, one route each. */
export default function AdminLayout({ children }: { children: React.ReactNode }) {
  return (
    <ConsoleShell access="admin">
      <div className="mx-auto max-w-[1240px] px-8 pt-16 pb-24">
        <p className={CAPTION}>Admin</p>
        <div className="mt-8">
          <AdminTabs />
        </div>
        <div className="mt-10">{children}</div>
      </div>
    </ConsoleShell>
  );
}
