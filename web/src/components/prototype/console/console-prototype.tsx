// PROTOTYPE — throwaway. Switcher for the console layout variants.
// Round 1 (A sidebar, C master-detail) lost to B; they remain on this
// branch's first commit. Round 2 refined B into B2/B3; dark won but the
// layout felt unchanged. Round 3 (D1-D3) drops that layout for three dark
// directions borrowed from getdesign.md (Raycast, Vercel, ClickHouse).
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
  type Role,
  type Screen,
} from "./console-state";
import { VariantB } from "./variant-b";
import { B2_SCREENS, VariantB2 } from "./variant-b2";
import { VariantD1 } from "./variant-d1-palette";
import { VariantD2 } from "./variant-d2-pipeline";
import { VariantD3 } from "./variant-d3-voltage";

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
  { key: "D1", name: "指令台 · Raycast", Component: VariantD1, screens: B2_SCREENS },
  { key: "D2", name: "流水线 · Vercel", Component: VariantD2, screens: B2_SCREENS },
  { key: "D3", name: "电压 · ClickHouse", Component: VariantD3, screens: B2_SCREENS },
  { key: "B3", name: "第二轮深色（对照）", Component: (p) => <VariantB2 {...p} tone="dark" />, screens: B2_SCREENS },
  {
    key: "B",
    name: "第一轮 B（对照）",
    Component: VariantB,
    screens: (r) => screensFor(r).map((k) => ({ key: k, label: SCREEN_LABEL[k] })),
  },
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
  const current = Math.max(0, VARIANTS.findIndex((v) => v.key === (params.get("variant") ?? "D1")));
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
