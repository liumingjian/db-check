"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { apiMode, resetMockData } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";
import { POP_IN, PRESS } from "@/components/console/kit";

const ROLE_LABEL = { admin: "管理员", user: "工程师" } as const;

/** The avatar at the foot of the rail; its popover opens to the right. */
export function AccountMenu() {
  const user = useAuthStore((s) => s.user);
  const logout = useAuthStore((s) => s.logout);
  const pathname = usePathname();
  const [open, setOpen] = useState(false);
  if (!user) return null;

  const inAdmin = pathname.startsWith("/admin");
  const item = "flex w-full cursor-pointer items-center justify-between rounded-lg px-3 py-2 text-left text-sm hover:bg-muted";

  return (
    <div className="relative">
      <button
        type="button"
        aria-label="账号"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className={cn("flex h-9 w-9 cursor-pointer items-center justify-center rounded-full bg-muted text-sm font-semibold hover:bg-[#2f2f2f]", PRESS)}
      >
        {user.displayName.slice(0, 1)}
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className={cn("absolute bottom-0 left-full z-50 ml-3 w-60 origin-bottom-left rounded-xl bg-popover p-1 shadow-2xl ring-1 ring-border", POP_IN)}>
            <div className="px-3 pt-2 pb-3">
              <p className="text-sm font-semibold">{user.displayName}</p>
              <p className="mt-0.5 text-xs text-muted-foreground">
                {ROLE_LABEL[user.role]} · @{user.username}
                {apiMode === "mock" && " · Mock 数据"}
              </p>
            </div>
            <div className="h-px bg-border" />
            <div className="pt-1" onClick={() => setOpen(false)}>
              {user.role === "admin" && !inAdmin && (
                <Link href="/admin/users" className={item}>
                  管理
                </Link>
              )}
              {inAdmin && (
                <Link href="/" className={item}>
                  回到工作台
                </Link>
              )}
              {resetMockData && (
                <button
                  type="button"
                  className={item}
                  onClick={() => {
                    // Restart from the seed: mock data and this tab's session both go.
                    resetMockData?.();
                    sessionStorage.clear();
                    window.location.assign("/login");
                  }}
                >
                  重置 Mock 数据
                </button>
              )}
              <button type="button" className={cn(item, "text-muted-foreground")} onClick={() => void logout()}>
                退出登录
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
