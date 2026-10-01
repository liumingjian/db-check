// PROTOTYPE — throwaway. D3 "电压": one long page with editorial type and a
// single electric yellow (after ClickHouse's DESIGN.md: #0a0a0a canvas,
// #faff69 voltage on CTAs, stats and full-bleed bands, Inter 700 with negative
// tracking, JetBrains Mono code). The hero IS the drop zone; the four
// platforms are equal tiles.
// Round 4: D3 won. Admin is its own view in the same language instead of the
// shared kit page, and the sticky top bar is under question, so `head`
// switches between three treatments: "none" (marks scroll away with the hero,
// a quiet section index on the right edge), "rail" (a slim left rail with
// vertical CJK labels) and "bar" (the round-3 top bar, for comparison).
"use client";

import { useContext, useEffect, useRef, useState } from "react";
import { ArrowDown, ArrowLeft, ArrowRight, Check, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import { formatSize, type ConsoleState, type DbType, type Release, type ReportTask, type Screen, type User } from "./console-state";
import { DEMO_FILES, stageOf, useReportFlow } from "./flow";
import { CopyText, DB_LABEL, EASE_OUT, Menu, PRESS, USAGE, Ui, UiHost, releaseMenu, when } from "./kit";

export type Head = "none" | "rail" | "bar";

const TOKENS = {
  "--background": "#0a0a0a",
  "--foreground": "#ffffff",
  "--card": "#1a1a1a",
  "--popover": "#1a1a1a",
  "--muted": "#242424",
  "--muted-foreground": "#888888",
  "--border": "#2a2a2a",
  "--primary": "#faff69",
  "--primary-foreground": "#0a0a0a",
  "--destructive": "#ef4444",
  "--warning": "#f59e0b",
  "--success": "#22c55e",
  "--ring": "#faff69",
} as const;

const SANS = 'var(--font-inter), "PingFang SC", -apple-system, sans-serif';
const MONO = "font-[family-name:var(--font-jetbrains)]";
const caption = "text-[12px] font-semibold uppercase tracking-[1.5px] text-[#888]";
const Y = "#faff69";
const RAIL_W = "pl-[72px]";

const SECTIONS: { key: Screen; label: string }[] = [
  { key: "new-report", label: "生成报告" },
  { key: "collectors", label: "采集器" },
  { key: "reports", label: "我的报告" },
];

export function VariantD3({ state, screen, setScreen, head = "none" }: VariantProps & { head?: Head }) {
  const { me } = state;
  const isAdmin = me.role === "admin";
  const adminView = isAdmin && screen === "users";
  const refs = useRef<Partial<Record<Screen, HTMLElement | null>>>({});
  const [active, setActive] = useState<Screen>("new-report");

  // First paint and view switches jump; in-page moves glide.
  const mounted = useRef(false);
  const wasAdmin = useRef(adminView);
  useEffect(() => {
    const glide = mounted.current && !wasAdmin.current && !adminView;
    if (adminView) window.scrollTo({ top: 0 });
    else refs.current[screen]?.scrollIntoView({ behavior: glide ? "smooth" : "instant", block: "start" });
    mounted.current = true;
    wasAdmin.current = adminView;
  }, [screen, adminView]);

  useEffect(() => {
    if (adminView) return;
    const io = new IntersectionObserver(
      (entries) => {
        const hit = entries.find((e) => e.isIntersecting);
        if (hit) setActive((hit.target as HTMLElement).dataset.key as Screen);
      },
      { rootMargin: "-45% 0px -50% 0px" },
    );
    Object.values(refs.current).forEach((el) => el && io.observe(el));
    return () => io.disconnect();
  }, [adminView]);

  function go(k: Screen) {
    setScreen(k);
    if (k !== "users") refs.current[k]?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  const current = adminView ? "users" : active;
  const nav = { state, current, go };
  const heroH = head === "bar" ? "min-h-[min(calc(100vh-64px),860px)]" : "min-h-[min(100vh,860px)]";
  const scrollMt = head === "bar" ? "scroll-mt-16" : "scroll-mt-0";

  const section = (k: Screen, node: React.ReactNode, border = true) => (
    <section
      data-key={k}
      ref={(el) => {
        refs.current[k] = el;
      }}
      className={cn(scrollMt, border && "border-t border-[#2a2a2a]")}
    >
      {node}
    </section>
  );

  return (
    <div style={{ ...TOKENS, fontFamily: SANS } as React.CSSProperties} className="min-h-screen bg-[#0a0a0a] text-white antialiased">
      <UiHost>
        {me.status !== "active" ? (
          <Gate state={state} />
        ) : (
          <>
            {head === "bar" && <TopBar {...nav} />}
            {head === "rail" && <Rail {...nav} />}
            {head === "none" && !adminView && <SectionIndex {...nav} />}

            <div className={cn(head === "rail" && RAIL_W)}>
              {adminView ? (
                <AdminView
                  state={state}
                  top={head === "none" ? <TopMarks state={state} onBack={() => go("new-report")} /> : null}
                />
              ) : (
                <>
                  {section(
                    "new-report",
                    <Hero state={state} heightClass={heroH} top={head === "none" ? <TopMarks state={state} onAdmin={() => go("users")} /> : null} />,
                    false,
                  )}
                  {section("collectors", <Collectors state={state} />)}
                  {section("reports", <Reports state={state} />)}
                </>
              )}
              <footer className="border-t border-[#2a2a2a] px-8 py-10 pb-32 text-center text-xs text-[#5a5a5a]">DB-Check · 数据库巡检平台</footer>
            </div>
          </>
        )}
      </UiHost>
    </div>
  );
}

/* ── Chrome: the three head treatments ── */

interface NavProps {
  state: ConsoleState;
  current: Screen;
  go: (k: Screen) => void;
}

function Bars({ size = 20 }: { size?: number }) {
  const s = size / 20;
  return (
    <span className="flex items-end gap-[3px]" style={{ height: size }}>
      {[12, 20, 16].map((h, i) => (
        <span key={i} className="rounded-[1px]" style={{ width: 4 * s, height: h * s, background: Y }} />
      ))}
    </span>
  );
}

function Brand() {
  return (
    <span className="flex items-center gap-2.5 text-[15px] font-bold tracking-[-0.3px]">
      <Bars />
      DB-Check
    </span>
  );
}

function navItems(state: ConsoleState) {
  return state.me.role === "admin" ? [...SECTIONS, { key: "users" as Screen, label: "管理" }] : SECTIONS;
}

/** Account circle with a popover; admins get 管理 here, with the pending count. */
function AccountMenu({ state, onAdmin, onBack, up }: { state: ConsoleState; onAdmin?: () => void; onBack?: () => void; up?: boolean }) {
  const { toast } = useContext(Ui);
  const [open, setOpen] = useState(false);
  const { me } = state;
  const isAdmin = me.role === "admin";
  const dot = isAdmin && state.pendingCount > 0;
  const item = "flex w-full items-center justify-between rounded-lg px-3 py-2 text-left text-sm cursor-pointer hover:bg-[#242424]";

  return (
    <div className="relative">
      <button
        type="button"
        aria-label="账号"
        onClick={() => setOpen(!open)}
        className={cn("relative flex h-9 w-9 items-center justify-center rounded-full bg-[#242424] text-sm font-semibold cursor-pointer hover:bg-[#2f2f2f]", PRESS)}
      >
        {me.displayName.slice(0, 1)}
        {dot && <span className="absolute -top-0.5 -right-0.5 h-2.5 w-2.5 rounded-full ring-2 ring-[#0a0a0a]" style={{ background: Y }} />}
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div
            className={cn(
              "absolute z-50 w-60 rounded-xl bg-[#1a1a1a] p-1 shadow-2xl ring-1 ring-[#2a2a2a]",
              "transition-[opacity,transform] duration-150 starting:scale-95 starting:opacity-0",
              EASE_OUT,
              up ? "bottom-0 left-full ml-3 origin-bottom-left" : "right-0 top-full mt-2 origin-top-right",
            )}
          >
            <div className="px-3 pt-2 pb-3">
              <p className="text-sm font-semibold">{me.displayName}</p>
              <p className="mt-0.5 text-xs text-[#888]">
                {isAdmin ? "管理员" : "工程师"} · {me.team}
              </p>
            </div>
            <div className="h-px bg-[#2a2a2a]" />
            <div className="pt-1">
              {isAdmin && onAdmin && (
                <button type="button" className={item} onClick={() => { setOpen(false); onAdmin(); }}>
                  管理
                  {state.pendingCount > 0 && (
                    <span className="rounded-sm px-1.5 text-[11px] font-bold text-black tabular-nums" style={{ background: Y }}>
                      {state.pendingCount} 待审批
                    </span>
                  )}
                </button>
              )}
              {onBack && (
                <button type="button" className={item} onClick={() => { setOpen(false); onBack(); }}>
                  回到工作台
                </button>
              )}
              <button type="button" className={cn(item, "text-[#888]")} onClick={() => { setOpen(false); toast("原型：不会真的退出"); }}>
                退出登录
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/** "none": brand and account sit inside the first screen and scroll away with it. */
function TopMarks({ state, onAdmin, onBack }: { state: ConsoleState; onAdmin?: () => void; onBack?: () => void }) {
  return (
    <div className="mx-auto flex h-20 max-w-[1240px] items-center justify-between px-8">
      {onBack ? (
        <button type="button" onClick={onBack} className="flex items-center gap-2 text-sm font-semibold text-[#888] hover:text-white cursor-pointer">
          <ArrowLeft className="h-4 w-4" /> 回到工作台
        </button>
      ) : (
        <Brand />
      )}
      <AccountMenu state={state} onAdmin={onAdmin} />
    </div>
  );
}

/** "none": a quiet index pinned to the right edge; labels appear on hover. */
function SectionIndex({ state, current, go }: NavProps) {
  const items = navItems(state);
  return (
    <nav className="group fixed top-1/2 right-6 z-40 flex -translate-y-1/2 flex-col items-end gap-3">
      {items.map((a, i) => {
        const on = current === a.key;
        const badge = a.key === "users" && state.pendingCount > 0;
        return (
          <button key={a.key} type="button" onClick={() => go(a.key)} className="flex h-6 items-center gap-3 cursor-pointer">
            <span className={cn("text-xs font-semibold opacity-0 transition-opacity duration-150 group-hover:opacity-100", on ? "text-white" : "text-[#888]")}>
              {a.label}
            </span>
            <span className="w-5 text-right text-[11px] font-semibold tabular-nums" style={{ color: on || badge ? Y : "#5a5a5a" }}>
              {badge ? state.pendingCount : String(i + 1).padStart(2, "0")}
            </span>
            <span
              className={cn("h-[2px] w-6 origin-right transition-transform duration-200", EASE_OUT)}
              style={{ background: on ? Y : "#3a3a3a", transform: `scaleX(${on ? 1 : 0.4})` }}
            />
          </button>
        );
      })}
    </nav>
  );
}

/** "rail": slim left rail, labels set vertically like a book spine. */
function Rail({ state, current, go }: NavProps) {
  const items = navItems(state);
  return (
    <aside className="fixed inset-y-0 left-0 z-40 flex w-[72px] flex-col items-center justify-between border-r border-[#1c1c1c] bg-[#0a0a0a] py-7">
      <button type="button" aria-label="DB-Check" onClick={() => go("new-report")} className="cursor-pointer">
        <Bars size={22} />
      </button>
      <div className="flex flex-col items-center gap-9">
        {items.map((a) => {
          const on = current === a.key;
          return (
            <button
              key={a.key}
              type="button"
              onClick={() => go(a.key)}
              className={cn("relative flex flex-col items-center gap-2 cursor-pointer", on ? "text-white" : "text-[#5a5a5a] hover:text-[#bbb]")}
            >
              <span className="text-[14px] font-semibold tracking-[0.35em]" style={{ writingMode: "vertical-rl" }}>
                {a.label}
              </span>
              {a.key === "users" && state.pendingCount > 0 && (
                <span className="rounded-sm px-1 text-[11px] font-bold text-black tabular-nums" style={{ background: Y }}>
                  {state.pendingCount}
                </span>
              )}
              <span
                className={cn("absolute top-0 -left-[22px] h-full w-[2px] origin-top transition-transform duration-200", EASE_OUT)}
                style={{ background: Y, transform: `scaleY(${on ? 1 : 0})` }}
              />
            </button>
          );
        })}
      </div>
      <AccountMenu state={state} up />
    </aside>
  );
}

/** "bar": the round-3 sticky top bar, kept for comparison. */
function TopBar({ state, current, go }: NavProps) {
  const { me } = state;
  return (
    <nav className="sticky top-0 z-40 border-b border-[#2a2a2a] bg-[#0a0a0a]/85 backdrop-blur-md">
      <div className="mx-auto flex h-16 max-w-[1240px] items-center gap-10 px-8">
        <Brand />
        <div className="flex gap-7 text-sm font-medium">
          {navItems(state).map((a) => (
            <button
              key={a.key}
              type="button"
              onClick={() => go(a.key)}
              className={cn("cursor-pointer", current === a.key ? "text-white" : "text-[#888] hover:text-white")}
            >
              {a.label}
              {a.key === "users" && state.pendingCount > 0 && (
                <span className="ml-1.5 rounded-sm px-1 text-[11px] font-bold text-black" style={{ background: Y }}>
                  {state.pendingCount}
                </span>
              )}
            </button>
          ))}
        </div>
        <span className="ml-auto text-sm text-[#888]">
          {me.displayName} · {me.role === "admin" ? "管理员" : "工程师"}
        </span>
      </div>
    </nav>
  );
}

/* ── Hero: 生成报告 ── */

function YellowBtn({ children, onClick, disabled, className }: { children: React.ReactNode; onClick: () => void; disabled?: boolean; className?: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn("inline-flex h-12 items-center justify-center gap-2 rounded-lg px-6 text-[15px] font-semibold text-[#0a0a0a] hover:bg-[#e6eb52] cursor-pointer disabled:opacity-40", PRESS, className)}
      style={{ background: Y }}
    >
      {children}
    </button>
  );
}

function Hero({ state, heightClass, top }: { state: ConsoleState; heightClass: string; top: React.ReactNode }) {
  const { toast } = useContext(Ui);
  const flow = useReportFlow(state);
  const [drag, setDrag] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const overall = flow.progress ? Math.round(flow.progress.reduce((a, b) => a + b, 0) / flow.progress.length) : 0;

  const dropProps = {
    onDragOver: (e: React.DragEvent) => {
      e.preventDefault();
      setDrag(true);
    },
    onDragLeave: (e: React.DragEvent) => e.currentTarget === e.target && setDrag(false),
    onDrop: (e: React.DragEvent) => {
      e.preventDefault();
      setDrag(false);
      flow.add([...e.dataTransfer.files].map((f) => ({ name: f.name, size: f.size })));
    },
  };

  if (flow.doneId) {
    return (
      <div className={cn("relative flex flex-col text-[#0a0a0a]", heightClass)} style={{ background: Y }}>
        <div className="mx-auto flex w-full max-w-[1240px] flex-1 flex-col justify-center px-8 py-16">
          <p className="text-[12px] font-semibold uppercase tracking-[1.5px] text-[#0a0a0a]/60">{flow.doneId}</p>
          <h1 className="mt-4 text-[96px] leading-[1] font-bold tracking-[-3.5px]">报告好了。</h1>
          <p className="mt-6 text-lg text-[#0a0a0a]/70">
            {flow.okCount} 份报告{flow.failedCount > 0 && `，${flow.failedCount} 份失败`} · 用时 {flow.elapsed} 秒 · 30 天内可在「我的报告」重新下载
          </p>
          <div className="mt-12 flex items-center gap-6">
            <button
              type="button"
              disabled={flow.okCount === 0}
              onClick={() => toast(`正在下载 reports-${flow.doneId}.zip`)}
              className={cn("inline-flex h-16 items-center gap-3 rounded-xl bg-[#0a0a0a] px-8 text-lg font-semibold cursor-pointer disabled:opacity-40", PRESS)}
              style={{ color: Y }}
            >
              下载报告 <ArrowDown className="h-5 w-5" />
            </button>
            <button type="button" onClick={flow.reset} className="text-base font-semibold underline underline-offset-4 cursor-pointer">
              再来一份
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      {...dropProps}
      className={cn("flex flex-col transition-[background-color,color] duration-200", heightClass, drag && "text-[#0a0a0a]")}
      style={drag ? { background: Y } : undefined}
    >
      <input
        ref={input}
        type="file"
        multiple
        accept=".zip"
        className="hidden"
        onChange={(e) => flow.add([...(e.target.files ?? [])].map((f) => ({ name: f.name, size: f.size })))}
      />
      {top && <div className={cn(drag && "invisible")}>{top}</div>}
      <div className="mx-auto grid w-full max-w-[1240px] flex-1 grid-cols-[1.3fr_1fr] items-center gap-16 px-8 py-16">
        <div>
          {drag ? (
            <h1 className="text-[120px] leading-[1] font-bold tracking-[-4px]">松手。</h1>
          ) : flow.progress ? (
            <>
              <p className={caption}>正在生成</p>
              <p className="mt-4 text-[160px] leading-[0.9] font-bold tracking-[-6px] tabular-nums" style={{ color: Y }}>
                {overall}
                <span className="text-[64px] tracking-[-2px]">%</span>
              </p>
              <p className="mt-6 text-lg text-[#ccc]">{flow.files.length} 个采集包 · {flow.elapsed} 秒</p>
            </>
          ) : (
            <>
              <h1 className="text-[88px] leading-[1.02] font-bold tracking-[-3px]">
                拖进 ZIP，
                <br />
                <span style={{ color: Y }}>拿走报告。</span>
              </h1>
              <p className="mt-8 max-w-md text-lg leading-relaxed text-[#ccc]">
                把采集器生成的 ZIP 拖到这一屏任意位置。数据库类型自动识别，一次可以放多台主机。
              </p>
              {!flow.files.length && (
                <div className="mt-10 flex items-center gap-6">
                  <YellowBtn onClick={() => input.current?.click()}>
                    选择采集包 <ArrowRight className="h-4 w-4" />
                  </YellowBtn>
                  <button type="button" onClick={() => flow.add(DEMO_FILES)} className="text-sm text-[#888] hover:text-white cursor-pointer">
                    原型：填入示例文件
                  </button>
                </div>
              )}
            </>
          )}
        </div>

        <div>
          {flow.files.length > 0 && !drag ? (
            <div className="rounded-2xl bg-[#1a1a1a] p-2">
              {flow.files.map((f, i) => {
                const p = flow.progress?.[i];
                const n = flow.notice(f.version);
                return (
                  <div key={f.name} className="rounded-xl px-4 py-3.5 hover:bg-[#242424]">
                    <div className="flex items-center gap-3">
                      <span className="min-w-0 flex-1">
                        <span className={`${MONO} block truncate text-sm`}>{f.name}</span>
                        <span className="text-xs text-[#888] tabular-nums">
                          {DB_LABEL[f.db]} · {formatSize(f.size)} · v{f.version}
                          {f.aux && ` · ${f.aux}`}
                        </span>
                      </span>
                      {p !== undefined ? (
                        p >= 100 ? <Check className="h-4 w-4" style={{ color: Y }} /> : <span className="text-xs text-[#888]">{stageOf(p)}</span>
                      ) : (
                        <>
                          {f.db !== "mysql" && !f.aux && (
                            <button type="button" onClick={() => flow.attach(f.name)} className="text-xs text-[#888] hover:text-white cursor-pointer">
                              + {f.db === "oracle" ? "AWR" : "WDR"}
                            </button>
                          )}
                          <button type="button" aria-label="移除" onClick={() => flow.remove(f.name)} className="text-[#5a5a5a] hover:text-white cursor-pointer">
                            <X className="h-4 w-4" />
                          </button>
                        </>
                      )}
                    </div>
                    {p !== undefined && (
                      <div className="mt-2.5 h-[3px] overflow-hidden rounded-full bg-[#2a2a2a]">
                        <div className="h-full origin-left transition-transform duration-150 ease-linear" style={{ background: Y, transform: `scaleX(${p / 100})` }} />
                      </div>
                    )}
                    {n && <p className={cn("mt-1.5 text-xs", n.tone === "danger" ? "text-[#ef4444]" : "text-[#f59e0b]")}>{n.text}</p>}
                  </div>
                );
              })}
              {!flow.progress && (
                <YellowBtn onClick={flow.start} className="mt-2 h-14 w-full text-base">
                  生成 {flow.files.length} 份报告 <ArrowRight className="h-4 w-4" />
                </YellowBtn>
              )}
            </div>
          ) : (
            !drag && (
              <div className="grid grid-cols-2 gap-x-10 gap-y-12">
                {[
                  ["3", "种数据库", "MySQL · Oracle · GaussDB"],
                  ["4", "个平台", "Linux / Windows · x86 / ARM"],
                  ["30", "天保留", "随时重新下载"],
                  [state.latest ? state.latest.version : "—", "采集器", state.latest ? "最新版本" : "暂无推荐版本"],
                ].map(([n, unit, sub]) => (
                  <div key={unit}>
                    <p className="text-[56px] leading-none font-bold tracking-[-1.5px] tabular-nums" style={{ color: Y }}>{n}</p>
                    <p className="mt-2 text-base font-semibold">{unit}</p>
                    <p className="mt-0.5 text-sm text-[#888]">{sub}</p>
                  </div>
                ))}
              </div>
            )
          )}
        </div>
      </div>
    </div>
  );
}

/* ── 采集器 ── */

function Collectors({ state }: { state: ConsoleState }) {
  const { toast, ask } = useContext(Ui);
  const [db, setDb] = useState<DbType>("oracle");
  const [showOld, setShowOld] = useState(false);
  const latest = state.latest;
  const isAdmin = state.me.role === "admin";
  const others = state.releases.filter((r) => r.status !== "latest" && (isAdmin || r.status === "deprecated"));

  function dl(r: Release, p: Release["packages"][number]) {
    state.download(r.version, p.platform);
    toast(`正在下载 ${p.fileName}`);
  }

  return (
    <div className="mx-auto max-w-[1240px] px-8 py-24">
      <p className={caption}>Collector</p>
      <div className="mt-4 flex items-end justify-between">
        <h2 className="text-[56px] leading-[1.1] font-bold tracking-[-2px]">
          下载采集器{" "}
          {latest ? <span style={{ color: Y }}>v{latest.version}</span> : <span className="text-[#888]">· 暂无推荐版本</span>}
        </h2>
        {latest && <Menu items={releaseMenu(state, ask, latest)} />}
      </div>
      <p className="mt-4 text-lg text-[#ccc]">按客户主机的系统和架构选择，四个包功能完全一致。</p>

      {latest && (
        <div className="mt-12 grid grid-cols-4 gap-4">
          {latest.packages.map((p) => (
            <button
              key={p.platform}
              type="button"
              onClick={() => dl(latest, p)}
              className={cn(
                "group flex h-60 flex-col justify-between rounded-2xl bg-[#1a1a1a] p-6 text-left cursor-pointer",
                "transition-[background-color,color,transform] duration-150 ease-[cubic-bezier(0.23,1,0.32,1)] active:scale-[0.98] hover:bg-[#faff69] hover:text-[#0a0a0a]",
              )}
            >
              <span className="text-sm font-semibold text-[#888] group-hover:text-[#0a0a0a]/60">{p.osLabel}</span>
              <span className="text-[40px] leading-none font-bold tracking-[-1.5px]">{p.archLabel}</span>
              <span className="flex items-center justify-between text-sm">
                <span className="tabular-nums text-[#888] group-hover:text-[#0a0a0a]/60">{formatSize(p.size)}</span>
                <span className="flex items-center gap-1 font-semibold">下载 <ArrowDown className="h-4 w-4" /></span>
              </span>
            </button>
          ))}
        </div>
      )}
      {latest && (
        <div className="mt-4 flex flex-wrap gap-x-8 gap-y-1 text-xs text-[#5a5a5a]">
          {latest.packages.map((p) => (
            <CopyText key={p.platform} text={p.sha256} label={`${p.osLabel} ${p.archLabel} sha256 ${p.sha256.slice(0, 12)}`} />
          ))}
        </div>
      )}

      <div className="mt-20 grid grid-cols-[1fr_1.6fr] gap-16">
        <div>
          <h3 className="text-2xl font-bold tracking-[-0.3px]">三步用起来</h3>
          <ol className="mt-6 space-y-5">
            {["上传到客户数据库主机并解压", "运行右侧的采集命令", "把 ZIP 拖回本页顶部"].map((s, i) => (
              <li key={s} className="flex items-baseline gap-4">
                <span className="text-[32px] leading-none font-bold tracking-[-1px]" style={{ color: Y }}>{i + 1}</span>
                <span className="text-base text-[#ccc]">{s}</span>
              </li>
            ))}
          </ol>
        </div>
        <div className="overflow-hidden rounded-2xl bg-[#1a1a1a]">
          <div className="flex gap-2 px-4 pt-4">
            {(Object.keys(USAGE) as DbType[]).map((k) => (
              <Chip key={k} on={db === k} onClick={() => setDb(k)}>
                {DB_LABEL[k]}
              </Chip>
            ))}
          </div>
          <div className="flex items-start gap-4 p-5">
            <code className={`${MONO} flex-1 text-sm leading-7 break-all text-[#e6e6e6]`}>{USAGE[db]}</code>
            <CopyText text={USAGE[db]} label="" />
          </div>
        </div>
      </div>

      {others.length > 0 && (
        <div className="mt-16">
          <button type="button" onClick={() => setShowOld(!showOld)} className="text-sm font-semibold text-[#888] hover:text-white cursor-pointer">
            历史版本 {showOld ? "↑" : "↓"}
          </button>
          {showOld && (
            <div className="mt-4 divide-y divide-[#2a2a2a] border-y border-[#2a2a2a]">
              {others.map((r) => (
                <div key={r.version} className="flex items-center gap-6 py-3 text-sm">
                  <span className="w-28 font-semibold tabular-nums">v{r.version}</span>
                  <span className={cn("w-56 truncate text-xs", r.status === "revoked" ? "text-[#ef4444]" : r.status === "deprecated" ? "text-[#f59e0b]" : "text-[#888]")}>
                    {r.status === "revoked" ? `已撤回 · ${r.revokeReason}` : r.status === "deprecated" ? "已弃用" : "预发布 · 仅管理员可见"}
                  </span>
                  <span className="flex flex-1 gap-4 text-xs">
                    {r.packages.map((p) => (
                      <button key={p.platform} type="button" onClick={() => dl(r, p)} className="text-[#888] hover:text-white cursor-pointer">
                        {p.osLabel} {p.archLabel}
                      </button>
                    ))}
                  </span>
                  <Menu items={releaseMenu(state, ask, r)} />
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Chip({ on, onClick, children }: { on: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn("rounded-full px-3 py-1 text-xs font-semibold cursor-pointer", on ? "text-[#0a0a0a]" : "text-[#888] hover:text-white")}
      style={on ? { background: Y } : undefined}
    >
      {children}
    </button>
  );
}

/* ── 我的报告 ── */

function ReportLine({ state, task, submitter }: { state: ConsoleState; task: ReportTask; submitter?: boolean }) {
  const { toast } = useContext(Ui);
  const failed = task.items.filter((i) => i.outcome === "failed").length;
  const revoked = task.items.map((i) => state.versionNotice(i.collectorVersion)).find((n) => n?.tone === "danger");
  const ok = !task.filesExpired && task.status !== "processing" && failed < task.items.length;
  return (
    <div className="flex items-center gap-6 py-4">
      <span className="w-28 shrink-0 text-sm text-[#888] tabular-nums">{when(task.createdAt)}</span>
      {submitter && <span className="w-20 shrink-0 text-sm font-semibold">{state.userById(task.submitterId)?.displayName}</span>}
      <span className="min-w-0 flex-1">
        <span className="block truncate text-base">
          {task.items[0].name}
          {task.items.length > 1 && <span className="text-[#888]"> 等 {task.items.length} 份</span>}
        </span>
        {(failed > 0 || revoked) && (
          <span className="text-xs">
            {failed > 0 && <span className="text-[#888]">{failed} 份失败 </span>}
            {revoked && <span className="text-[#ef4444]">{revoked.text}</span>}
          </span>
        )}
      </span>
      {ok ? (
        <button type="button" onClick={() => toast(`正在下载 reports-${task.id}.zip`)} className="flex items-center gap-1 text-sm font-semibold hover:underline cursor-pointer" style={{ color: Y }}>
          下载 <ArrowDown className="h-4 w-4" />
        </button>
      ) : (
        <span className="text-sm text-[#5a5a5a]">{task.status === "processing" ? "生成中" : task.filesExpired ? "已过期" : "失败"}</span>
      )}
    </div>
  );
}

function Reports({ state }: { state: ConsoleState }) {
  const mine = state.allTasks.filter((t) => t.submitterId === state.me.id);
  return (
    <div className="mx-auto max-w-[1240px] px-8 py-24">
      <div className="grid grid-cols-[1fr_1.6fr] gap-16">
        <div>
          <p className={caption}>My reports</p>
          <h2 className="mt-4 text-[40px] leading-[1.15] font-bold tracking-[-1.5px]">忘了下载？<br />都在这里。</h2>
          <p className="mt-4 text-base text-[#888]">报告保留 30 天。</p>
        </div>
        <div className="divide-y divide-[#2a2a2a] border-y border-[#2a2a2a]">
          {mine.length === 0 && <p className="py-10 text-sm text-[#888]">还没有生成过报告</p>}
          {mine.map((t) => (
            <ReportLine key={t.id} state={state} task={t} />
          ))}
        </div>
      </div>
    </div>
  );
}

/* ── 管理 ── */

type AdminTab = "users" | "downloads" | "reports";

function platformLabel(state: ConsoleState, platform: string) {
  const p = state.releases[0]?.packages.find((x) => x.platform === platform);
  return p ? `${p.osLabel} ${p.archLabel}` : platform;
}

function AdminView({ state, top }: { state: ConsoleState; top: React.ReactNode }) {
  const { toast, ask } = useContext(Ui);
  const [tab, setTab] = useState<AdminTab>("users");
  const pending = state.users.filter((u) => u.status === "pending");
  const tabs: { key: AdminTab; label: string; count: number }[] = [
    { key: "users", label: "成员", count: state.users.length - pending.length },
    { key: "downloads", label: "下载记录", count: state.downloads.length },
    { key: "reports", label: "全部报告", count: state.allTasks.length },
  ];

  return (
    <div className="min-h-screen">
      {top}
      <div className="mx-auto max-w-[1240px] px-8 pt-16 pb-24">
        <p className={caption}>Admin</p>
        <h1 className="mt-4 text-[72px] leading-[1.05] font-bold tracking-[-2.5px]">
          {pending.length > 0 ? (
            <>
              <span style={{ color: Y }}>{pending.length} 人</span>在等你批准。
            </>
          ) : (
            "没有待审批的申请。"
          )}
        </h1>
        {pending.length === 0 && <p className="mt-4 text-lg text-[#888]">新的注册申请会出现在这里。</p>}

        {pending.length > 0 && (
          <div className="mt-12 grid grid-cols-2 gap-4">
            {pending.map((u) => (
              <div key={u.id} className="flex flex-col rounded-2xl bg-[#1a1a1a] p-7">
                <p className="text-sm text-[#888] tabular-nums">{when(u.registeredAt)} 申请</p>
                <p className="mt-4 text-[32px] leading-none font-bold tracking-[-1px]">{u.displayName}</p>
                <p className="mt-2 text-sm text-[#888]">
                  @{u.username} · {u.team} · {u.email}
                </p>
                {u.note && <p className="mt-5 text-base text-[#ccc]">“{u.note}”</p>}
                <div className="mt-8 flex items-center gap-6">
                  <YellowBtn
                    onClick={() => {
                      state.userActions.approve(u.id);
                      toast(`已批准 ${u.displayName}`);
                    }}
                  >
                    批准 <Check className="h-4 w-4" />
                  </YellowBtn>
                  <button
                    type="button"
                    onClick={() =>
                      ask({
                        title: `拒绝 ${u.displayName} 的申请`,
                        body: "对方登录后会看到原因，可以修改后重新申请。",
                        input: "拒绝原因",
                        confirm: "拒绝",
                        danger: true,
                        onConfirm: (r) => state.userActions.reject(u.id, r),
                      })
                    }
                    className="text-sm font-semibold text-[#888] hover:text-white cursor-pointer"
                  >
                    拒绝…
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}

        <div className="mt-24 flex items-baseline gap-10 border-b border-[#2a2a2a] pb-5">
          {tabs.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setTab(t.key)}
              className={cn("text-[32px] font-bold tracking-[-1px] cursor-pointer", tab === t.key ? "text-white" : "text-[#3a3a3a] hover:text-[#888]")}
            >
              {t.label}
              <sup className="ml-1 text-sm font-semibold tracking-normal tabular-nums" style={{ color: tab === t.key ? Y : undefined }}>
                {t.count}
              </sup>
            </button>
          ))}
          <p className="ml-auto text-sm text-[#5a5a5a]">版本状态在「采集器」的 ··· 菜单里调整</p>
        </div>

        {tab === "users" && <Members state={state} />}
        {tab === "downloads" && <Downloads state={state} />}
        {tab === "reports" && <AllReports state={state} />}
      </div>
    </div>
  );
}

function Members({ state }: { state: ConsoleState }) {
  const { ask } = useContext(Ui);
  const a = state.userActions;
  const order = { active: 0, disabled: 1, rejected: 2, pending: 3 } as const;
  const list = state.users.filter((u) => u.status !== "pending").sort((x, y) => order[x.status] - order[y.status]);

  function menu(u: User) {
    const items: { label: string; onSelect: () => void; danger?: boolean }[] = [];
    if (u.status === "active") {
      items.push({ label: u.role === "admin" ? "降为普通用户" : "设为管理员", onSelect: () => a.toggleRole(u.id) });
      items.push({
        label: "重置密码",
        onSelect: () => {
          const temp = a.resetPassword(u.id, true);
          ask({
            title: `${u.displayName} 的临时密码`,
            body: (
              <div className="space-y-3">
                <p>请线下交给对方，下次登录时必须修改。</p>
                <div className="rounded-lg bg-[#242424] px-3 py-2"><CopyText text={temp} /></div>
              </div>
            ),
            confirm: "完成",
          });
        },
      });
      if (u.id !== state.me.id)
        items.push({
          label: "禁用账号",
          danger: true,
          onSelect: () =>
            ask({
              title: `禁用 ${u.displayName}`,
              body: "禁用后无法登录，已提交的报告会保留。",
              input: "禁用原因",
              confirm: "禁用",
              danger: true,
              onConfirm: (r) => a.disable(u.id, r),
            }),
        });
    }
    if (u.status === "disabled") items.push({ label: "启用账号", onSelect: () => a.enable(u.id) });
    return items;
  }

  return (
    <div className="divide-y divide-[#2a2a2a] border-b border-[#2a2a2a]">
      {list.map((u) => (
        <div key={u.id} className={cn("flex items-center gap-6 py-4", u.status !== "active" && "text-[#5a5a5a]")}>
          <span className="w-36 shrink-0 text-base font-semibold">
            {u.displayName}
            {u.id === state.me.id && <span className="ml-1.5 text-xs font-normal text-[#888]">你</span>}
          </span>
          <span className="w-24 shrink-0 text-sm font-semibold" style={{ color: u.role === "admin" && u.status === "active" ? Y : undefined }}>
            {u.role === "admin" ? "管理员" : "工程师"}
          </span>
          <span className="min-w-0 flex-1 truncate text-sm text-[#888]">
            @{u.username} · {u.team}
          </span>
          {u.status !== "active" && (
            <span className={cn("max-w-[40%] truncate text-sm", u.status === "rejected" ? "text-[#ef4444]" : "text-[#f59e0b]")}>
              {u.status === "rejected" ? "已拒绝" : "已禁用"} · {u.reason}
            </span>
          )}
          <Menu items={menu(u)} />
        </div>
      ))}
    </div>
  );
}

function PeopleFilter({ ids, state, value, onChange }: { ids: string[]; state: ConsoleState; value: string; onChange: (v: string) => void }) {
  return (
    <div className="flex gap-2 py-5">
      <Chip on={value === "all"} onClick={() => onChange("all")}>全部</Chip>
      {[...new Set(ids)].map((id) => (
        <Chip key={id} on={value === id} onClick={() => onChange(id)}>
          {state.userById(id)?.displayName}
        </Chip>
      ))}
    </div>
  );
}

function Downloads({ state }: { state: ConsoleState }) {
  const [who, setWho] = useState("all");
  const list = state.downloads.filter((d) => who === "all" || d.userId === who);
  return (
    <>
      <PeopleFilter ids={state.downloads.map((d) => d.userId)} state={state} value={who} onChange={setWho} />
      <div className="divide-y divide-[#2a2a2a] border-y border-[#2a2a2a]">
        {list.map((d) => {
          const n = state.versionNotice(d.version);
          return (
            <div key={d.id} className="flex items-center gap-6 py-4">
              <span className="w-28 shrink-0 text-sm text-[#888] tabular-nums">{when(d.at)}</span>
              <span className="w-20 shrink-0 text-sm font-semibold">{state.userById(d.userId)?.displayName}</span>
              <span className="w-28 shrink-0 text-base font-semibold tabular-nums">v{d.version}</span>
              <span className="flex-1 text-sm text-[#ccc]">{platformLabel(state, d.platform)}</span>
              {n && <span className={cn("text-xs", n.tone === "danger" ? "text-[#ef4444]" : "text-[#f59e0b]")}>{n.tone === "danger" ? "已撤回" : "已弃用"}</span>}
            </div>
          );
        })}
      </div>
    </>
  );
}

function AllReports({ state }: { state: ConsoleState }) {
  const [who, setWho] = useState("all");
  const list = state.allTasks.filter((t) => who === "all" || t.submitterId === who);
  return (
    <>
      <PeopleFilter ids={state.allTasks.map((t) => t.submitterId)} state={state} value={who} onChange={setWho} />
      <div className="divide-y divide-[#2a2a2a] border-y border-[#2a2a2a]">
        {list.map((t) => (
          <ReportLine key={t.id} state={state} task={t} submitter />
        ))}
      </div>
    </>
  );
}

/* ── 待审批 / 已拒绝 ── */

function Gate({ state }: { state: ConsoleState }) {
  const { me } = state;
  const field = "h-12 w-full rounded-lg bg-[#1a1a1a] px-4 text-base outline-none ring-1 ring-transparent focus:ring-[#faff69]";
  return (
    <div className="flex min-h-screen flex-col">
      <div className="mx-auto flex h-20 w-full max-w-[1240px] items-center px-8">
        <Brand />
      </div>
      <div className="mx-auto grid w-full max-w-[1240px] flex-1 grid-cols-[1.3fr_1fr] items-center gap-16 px-8 pb-32">
        {me.status === "pending" ? (
          <div>
            <p className={caption}>Pending</p>
            <h1 className="mt-4 text-[88px] leading-[1.02] font-bold tracking-[-3px]">
              申请已提交，
              <br />
              <span style={{ color: Y }}>等管理员批准。</span>
            </h1>
            <p className="mt-8 max-w-md text-lg leading-relaxed text-[#ccc]">
              {me.displayName}，你在 {when(me.registeredAt)} 提交了申请。批准后刷新本页就能开始用。
            </p>
          </div>
        ) : (
          <>
            <div>
              <p className={caption}>Rejected</p>
              <h1 className="mt-4 text-[88px] leading-[1.02] font-bold tracking-[-3px]">这次没通过。</h1>
              <p className="mt-8 max-w-md border-l-2 border-[#ef4444] pl-4 text-lg leading-relaxed text-[#ccc]">{me.reason}</p>
            </div>
            <div className="space-y-3">
              <p className="mb-5 text-base text-[#888]">改一下再申请，用户名和邮箱不变。</p>
              <input className={field} defaultValue={me.displayName} placeholder="显示名称" />
              <input className={field} defaultValue={me.team} placeholder="团队" />
              <textarea className={cn(field, "h-auto resize-none py-3")} rows={3} defaultValue={me.note} placeholder="申请说明" />
              <YellowBtn onClick={() => state.userActions.resubmit(me.id)} className="h-14 w-full text-base">
                重新提交申请 <ArrowRight className="h-4 w-4" />
              </YellowBtn>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
