"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";
import { AccountMenu } from "@/components/console/account-menu";
import { Bars } from "@/components/console/kit";
import { SECTIONS, scrollToSection, type SectionKey } from "@/components/console/sections";

export const RAIL_WIDTH = "pl-[72px]";

/**
 * Tracks which home section sits in the middle band of the viewport. Also
 * honours a section hash on arrival: the browser cannot, because the sections
 * render only after the session resolves.
 */
function useSectionInView(enabled: boolean): SectionKey {
  const [active, setActive] = useState<SectionKey>("new-report");
  useEffect(() => {
    if (!enabled) return;
    const arrival = SECTIONS.find(({ key }) => `#${key}` === window.location.hash);
    if (arrival) scrollToSection(arrival.key);
    const observer = new IntersectionObserver(
      (entries) => {
        const hit = entries.find((e) => e.isIntersecting);
        if (hit) setActive(hit.target.id as SectionKey);
      },
      { rootMargin: "-45% 0px -50% 0px" },
    );
    SECTIONS.forEach(({ key }) => {
      const el = document.getElementById(key);
      if (el) observer.observe(el);
    });
    return () => observer.disconnect();
  }, [enabled]);
  return active;
}

function RailLabel({ label, on, onClick, href }: { label: string; on: boolean; onClick?: () => void; href?: string }) {
  const className = cn("relative flex cursor-pointer flex-col items-center gap-2", on ? "text-foreground" : "text-[#5a5a5a] hover:text-[#bbb]");
  const content = (
    <>
      <span className="text-[14px] font-semibold tracking-[0.35em] [writing-mode:vertical-rl]">{label}</span>
      {/* No transition: the marker follows frequent navigation, which never animates. */}
      {on && <span aria-hidden className="absolute top-0 -left-[22px] h-full w-[2px] bg-primary" />}
    </>
  );
  if (href) {
    return (
      <Link href={href} className={className} aria-current={on ? "page" : undefined}>
        {content}
      </Link>
    );
  }
  return (
    <button type="button" onClick={onClick} className={className} aria-current={on ? "location" : undefined}>
      {content}
    </button>
  );
}

/**
 * The slim fixed left rail: brand, the home section labels set vertically,
 * 管理 for admins, and the account menu. On the home page the labels jump to
 * their section and the one in view is marked; elsewhere they lead home.
 */
export function Rail() {
  const pathname = usePathname();
  const router = useRouter();
  const isAdmin = useAuthStore((s) => s.user?.role === "admin");
  const onHome = pathname === "/";
  const inView = useSectionInView(onHome);

  function go(key: SectionKey) {
    if (onHome) scrollToSection(key);
    else router.push(`/#${key}`);
  }

  return (
    <aside className="fixed inset-y-0 left-0 z-40 flex w-[72px] flex-col items-center justify-between border-r border-sidebar-border bg-background py-7">
      <button type="button" aria-label="DB-Check" onClick={() => go("new-report")} className="cursor-pointer">
        <Bars size={22} />
      </button>
      <nav className="flex flex-col items-center gap-9">
        {SECTIONS.map(({ key, label }) => (
          <RailLabel key={key} label={label} on={onHome && inView === key} onClick={() => go(key)} />
        ))}
        {isAdmin && <RailLabel label="管理" href="/admin/users" on={pathname.startsWith("/admin")} />}
      </nav>
      <AccountMenu />
    </aside>
  );
}
