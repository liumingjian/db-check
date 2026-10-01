"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

const TABS = [
  { href: "/admin/users", label: "用户" },
  { href: "/admin/downloads", label: "下载记录" },
  { href: "/admin/reports", label: "全部报告" },
] as const;

/** The 管理 view's big text tabs; each is its own route. */
export function AdminTabs() {
  const pathname = usePathname();
  return (
    <nav className="flex items-baseline gap-10 border-b border-border pb-5">
      {TABS.map(({ href, label }) => {
        const on = pathname === href;
        return (
          <Link
            key={href}
            href={href}
            aria-current={on ? "page" : undefined}
            className={cn("text-[32px] leading-none font-bold tracking-[-1px]", on ? "text-foreground" : "text-[#3a3a3a] hover:text-[#5a5a5a]")}
          >
            {label}
          </Link>
        );
      })}
    </nav>
  );
}
