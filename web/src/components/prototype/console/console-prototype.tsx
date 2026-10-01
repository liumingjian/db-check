// PROTOTYPE — throwaway. Switcher for the console layout variants.
"use client";

import { useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  SCREEN_LABEL,
  screensFor,
  useConsoleState,
  type ConsoleState,
  type Persona,
  type Screen,
} from "./console-state";
import { VariantA } from "./variant-a";
import { VariantB } from "./variant-b";
import { VariantC } from "./variant-c";

export interface VariantProps {
  state: ConsoleState;
  screen: Screen;
  setScreen: (s: Screen) => void;
}

const VARIANTS = [
  { key: "A", name: "侧边栏 + 密集表格", Component: VariantA },
  { key: "B", name: "顶部导航 + 主推下载卡片", Component: VariantB },
  { key: "C", name: "图标栏 + 主从分栏", Component: VariantC },
] as const;

const PERSONAS: { key: Persona; label: string }[] = [
  { key: "engineer", label: "普通用户" },
  { key: "admin", label: "管理员" },
  { key: "pending", label: "待审批" },
  { key: "rejected", label: "已拒绝" },
];

export function ConsolePrototype() {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();
  const current = Math.max(0, VARIANTS.findIndex((v) => v.key === (params.get("variant") ?? "A")));
  const [persona, setPersona] = useState<Persona>("engineer");
  const [screen, setScreen] = useState<Screen>("collectors");
  const state = useConsoleState(persona);

  const screens = screensFor(state.me.role);
  const shownScreen = screens.includes(screen) ? screen : "collectors";

  function go(delta: number) {
    const next = VARIANTS[(current + delta + VARIANTS.length) % VARIANTS.length];
    router.replace(`${pathname}?variant=${next.key}`, { scroll: false });
  }

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const el = document.activeElement as HTMLElement | null;
      if (el && (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT" || el.isContentEditable)) return;
      if (e.key === "ArrowLeft") go(-1);
      if (e.key === "ArrowRight") go(1);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  const { Component, key, name } = VARIANTS[current];
  const showShell = state.me.status === "active";

  return (
    <>
      <Component state={state} screen={shownScreen} setScreen={setScreen} />

      {process.env.NODE_ENV !== "production" && (
        <div className="fixed bottom-4 left-1/2 z-[100] -translate-x-1/2 flex flex-col items-center gap-1.5">
          <div className="max-w-[90vw] truncate rounded-full bg-white/90 px-3 py-1 text-[11px] text-slate-700 shadow">
            最新：{state.latest?.version ?? "无"} · 待审批：{state.pendingCount} · 下载记录：{state.downloads.length} · 当前身份：
            {state.me.displayName}（{state.me.role === "admin" ? "管理员" : "普通用户"} / {state.me.status}）· 最近操作：{state.lastEvent}
          </div>
          <div className="flex items-center gap-2 rounded-full bg-white px-2 py-1.5 text-xs text-slate-900 shadow-xl ring-1 ring-black/10">
            <button type="button" onClick={() => go(-1)} className="rounded-full p-1 hover:bg-slate-200 cursor-pointer" aria-label="上一个方案">
              <ChevronLeft className="h-4 w-4" />
            </button>
            <span className="min-w-40 text-center font-semibold">
              {key}（{name}）
            </span>
            <button type="button" onClick={() => go(1)} className="rounded-full p-1 hover:bg-slate-200 cursor-pointer" aria-label="下一个方案">
              <ChevronRight className="h-4 w-4" />
            </button>
            <span className="mx-1 h-4 w-px bg-slate-300" />
            {PERSONAS.map((p) => (
              <button
                key={p.key}
                type="button"
                onClick={() => setPersona(p.key)}
                className={cn(
                  "rounded-full px-2 py-0.5 cursor-pointer",
                  persona === p.key ? "bg-slate-900 text-white" : "hover:bg-slate-200",
                )}
              >
                {p.label}
              </button>
            ))}
            {showShell && (
              <>
                <span className="mx-1 h-4 w-px bg-slate-300" />
                <select
                  value={shownScreen}
                  onChange={(e) => setScreen(e.target.value as Screen)}
                  className="rounded-full bg-slate-100 px-2 py-0.5 outline-none cursor-pointer"
                >
                  {screens.map((s) => (
                    <option key={s} value={s}>
                      {SCREEN_LABEL[s]}
                    </option>
                  ))}
                </select>
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
}
