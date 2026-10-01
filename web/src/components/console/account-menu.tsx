"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { apiMode } from "@/lib/api";
import { cn } from "@/lib/utils";
import { usePendingCount } from "@/stores/users-store";
import { useAuthStore } from "@/stores/auth-store";
import { POP_IN, PRESS } from "@/components/console/kit";
import { ResetMockDataButton } from "@/components/console/reset-mock-data";

const ROLE_LABEL = { admin: "管理员", user: "工程师" } as const;

/** The avatar and its 账号 caption at the foot of the rail; its popover opens to the right. */
export function AccountMenu() {
  const user = useAuthStore((s) => s.user);
  const logout = useAuthStore((s) => s.logout);
  const pathname = usePathname();
  const pending = usePendingCount();
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
        title={`${user.displayName} · 账号`}
        onClick={() => setOpen(!open)}
        className={cn("group flex cursor-pointer flex-col items-center gap-2", PRESS)}
      >
        <span className="relative flex h-9 w-9 items-center justify-center rounded-full bg-muted text-sm font-semibold group-hover:bg-[#2f2f2f]">
          {user.displayName.slice(0, 1)}
          {user.role === "admin" && pending > 0 && (
            <span aria-hidden className="absolute -top-0.5 -right-0.5 h-2.5 w-2.5 rounded-full bg-primary ring-2 ring-background" />
          )}
        </span>
        <span aria-hidden className={cn("pl-[0.35em] text-[11px] font-semibold tracking-[0.35em]", open ? "text-foreground" : "text-[#5a5a5a] group-hover:text-[#bbb]")}>
          账号
        </span>
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
                  {pending > 0 && <span className="text-xs font-semibold text-primary tabular-nums">{pending} 待审批</span>}
                </Link>
              )}
              {inAdmin && (
                <Link href="/" className={item}>
                  回到工作台
                </Link>
              )}
              <ResetMockDataButton className={item} />
              <Link href="/change-password" className={item}>
                修改密码
              </Link>
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
