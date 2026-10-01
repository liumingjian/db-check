// PROTOTYPE — throwaway. Switcher for the console layout variants.
// Round 1 (A sidebar, C master-detail) lost to B; round 2 refined B into
// B2/B3; round 3 (D1 Raycast, D2 Vercel, D3 ClickHouse) left the old layout.
// D3 won (commit d689d58 has every earlier variant). Round 4 compares three
// head treatments of D3.
"use client";

import { useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { useConsoleState, type ConsoleState, type Persona, type Role, type Screen } from "./console-state";
import { VariantD3 } from "./variant-d3-voltage";

const SCREENS = (role: Role): { key: Screen; label: string }[] => [
  { key: "new-report", label: "生成报告" },
  { key: "collectors", label: "采集器" },
  { key: "reports", label: "我的报告" },
  ...(role === "admin" ? [{ key: "users" as Screen, label: "管理" }] : []),
];

export interface VariantProps {
  state: ConsoleState;
  screen: Screen;
  setScreen: (s: Screen) => void;
}

const VARIANTS: {
  key: string;
  name: string;
  Component: (p: VariantProps) => React.ReactNode;
  screens: (role: Role) => { key: Screen; label: string }[];
}[] = [
  { key: "D3a", name: "无头部 · 右侧索引", Component: (p) => <VariantD3 {...p} head="none" />, screens: SCREENS },
  { key: "D3b", name: "竖排侧栏", Component: (p) => <VariantD3 {...p} head="rail" />, screens: SCREENS },
  { key: "D3", name: "顶栏（第三轮对照）", Component: (p) => <VariantD3 {...p} head="bar" />, screens: SCREENS },
];

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
  const current = Math.max(0, VARIANTS.findIndex((v) => v.key === (params.get("variant") ?? "D3b")));
  const [persona, setPersona] = useState<Persona>((params.get("as") as Persona) ?? "engineer");
  const [screen, setScreen] = useState<Screen>((params.get("screen") as Screen) ?? "new-report");
  const state = useConsoleState(persona);

  const variant = VARIANTS[current];
  const screens = variant.screens(state.me.role);
  const shownScreen = screens.some((s) => s.key === screen) ? screen : "new-report";

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

  const { Component, key, name } = variant;
  const showShell = state.me.status === "active";

  return (
    <>
      <Component state={state} screen={shownScreen} setScreen={setScreen} />

      {process.env.NODE_ENV !== "production" && (
        <div className="fixed bottom-4 left-1/2 z-[100] flex -translate-x-1/2 flex-col items-center gap-1.5 font-sans">
          <div className="max-w-[60vw] truncate rounded-full bg-white/90 px-3 py-1 text-[11px] text-slate-700 shadow">
            最新：{state.latest?.version ?? "无"} · 待审批：{state.pendingCount} · 下载记录：{state.downloads.length} · 当前身份：
            {state.me.displayName}（{state.me.role === "admin" ? "管理员" : "普通用户"} / {state.me.status}）· 最近操作：{state.lastEvent}
          </div>
          <div className="flex items-center gap-2 rounded-full bg-white px-2 py-1.5 text-xs text-slate-900 shadow-xl ring-1 ring-black/10">
            <button type="button" onClick={() => go(-1)} className="rounded-full p-1 hover:bg-slate-200 cursor-pointer" aria-label="上一个方案">
              <ChevronLeft className="h-4 w-4" />
            </button>
            <span className="min-w-32 text-center font-semibold">
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
                    <option key={s.key} value={s.key}>
                      {s.label}
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
