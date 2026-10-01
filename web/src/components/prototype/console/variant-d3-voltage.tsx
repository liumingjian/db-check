// PROTOTYPE — throwaway. Round 3, D3 "电压": one long page with editorial
// type and a single electric yellow (after ClickHouse's DESIGN.md: #0a0a0a
// canvas, #faff69 voltage on CTAs, stats and full-bleed bands, Inter 700 with
// negative tracking, JetBrains Mono code). The hero IS the drop zone and turns
// yellow while a file hovers; the four platforms are equal tiles.
"use client";

import { useContext, useEffect, useRef, useState } from "react";
import { ArrowDown, ArrowRight, Check, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import { formatSize, type ConsoleState, type DbType, type Release, type Screen } from "./console-state";
import { DEMO_FILES, stageOf, useReportFlow } from "./flow";
import { AccountGate, Admin, CopyText, DB_LABEL, Menu, PRESS, USAGE, Ui, UiHost, releaseMenu, when } from "./kit";

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

const ANCHORS: { key: Screen; label: string }[] = [
  { key: "new-report", label: "生成报告" },
  { key: "collectors", label: "采集器" },
  { key: "reports", label: "我的报告" },
];

export function VariantD3({ state, screen, setScreen }: VariantProps) {
  const { me } = state;
  const refs = useRef<Partial<Record<Screen, HTMLElement | null>>>({});
  const anchors = me.role === "admin" ? [...ANCHORS, { key: "users" as Screen, label: "管理" }] : ANCHORS;

  const mounted = useRef(false);
  useEffect(() => {
    refs.current[screen]?.scrollIntoView({ behavior: mounted.current ? "smooth" : "instant", block: "start" });
    mounted.current = true;
  }, [screen]);

  return (
    <div style={{ ...TOKENS, fontFamily: SANS } as React.CSSProperties} className="min-h-screen bg-[#0a0a0a] text-white antialiased">
      <UiHost>
        {me.status !== "active" ? (
          <AccountGate state={state} />
        ) : (
          <>
            <nav className="sticky top-0 z-40 border-b border-[#2a2a2a] bg-[#0a0a0a]/85 backdrop-blur-md">
              <div className="mx-auto flex h-16 max-w-[1240px] items-center gap-10 px-8">
                <span className="flex items-center gap-2.5 text-[15px] font-bold tracking-[-0.3px]">
                  <span className="flex h-5 items-end gap-[3px]">
                    {[12, 20, 16].map((h, i) => (
                      <span key={i} className="w-[4px] rounded-[1px]" style={{ height: h, background: Y }} />
                    ))}
                  </span>
                  DB-Check
                </span>
                <div className="flex gap-7 text-sm font-medium">
                  {anchors.map((a) => (
                    <button
                      key={a.key}
                      type="button"
                      onClick={() => {
                        setScreen(a.key);
                        refs.current[a.key]?.scrollIntoView({ behavior: "smooth", block: "start" });
                      }}
                      className={cn("cursor-pointer", screen === a.key ? "text-white" : "text-[#888] hover:text-white")}
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

            <section ref={(el) => { refs.current["new-report"] = el; }} className="scroll-mt-16">
              <Hero state={state} />
            </section>
            <section ref={(el) => { refs.current.collectors = el; }} className="scroll-mt-16 border-t border-[#2a2a2a]">
              <Collectors state={state} />
            </section>
            <section ref={(el) => { refs.current.reports = el; }} className="scroll-mt-16 border-t border-[#2a2a2a]">
              <Reports state={state} />
            </section>
            {me.role === "admin" && (
              <section ref={(el) => { refs.current.users = el; }} className="scroll-mt-16 border-t border-[#2a2a2a]">
                <div className="mx-auto max-w-3xl px-8 py-24">
                  <Admin state={state} />
                </div>
              </section>
            )}
            <footer className="border-t border-[#2a2a2a] px-8 py-10 pb-32 text-center text-xs text-[#5a5a5a]">DB-Check · 数据库巡检平台</footer>
          </>
        )}
      </UiHost>
    </div>
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

function Hero({ state }: { state: ConsoleState }) {
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
      <div className="min-h-[min(calc(100vh-64px),860px)] text-[#0a0a0a]" style={{ background: Y }}>
        <div className="mx-auto flex min-h-[min(calc(100vh-64px),860px)] max-w-[1240px] flex-col justify-center px-8">
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
      className={cn("min-h-[min(calc(100vh-64px),860px)] transition-[background-color,color] duration-200", drag ? "text-[#0a0a0a]" : "")}
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
      <div className="mx-auto grid min-h-[min(calc(100vh-64px),860px)] max-w-[1240px] grid-cols-[1.3fr_1fr] items-center gap-16 px-8 py-16">
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
              <button
                key={k}
                type="button"
                onClick={() => setDb(k)}
                className={cn("rounded-full px-3 py-1 text-xs font-semibold cursor-pointer", db === k ? "text-[#0a0a0a]" : "text-[#888] hover:text-white")}
                style={db === k ? { background: Y } : undefined}
              >
                {DB_LABEL[k]}
              </button>
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

/* ── 我的报告 ── */

function Reports({ state }: { state: ConsoleState }) {
  const { toast } = useContext(Ui);
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
          {mine.map((t) => {
            const failed = t.items.filter((i) => i.outcome === "failed").length;
            const revoked = t.items.map((i) => state.versionNotice(i.collectorVersion)).find((n) => n?.tone === "danger");
            const ok = !t.filesExpired && t.status !== "processing" && failed < t.items.length;
            return (
              <div key={t.id} className="flex items-center gap-6 py-4">
                <span className="w-28 text-sm text-[#888] tabular-nums">{when(t.createdAt)}</span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-base">{t.items[0].name}{t.items.length > 1 && <span className="text-[#888]"> 等 {t.items.length} 份</span>}</span>
                  {(failed > 0 || revoked) && (
                    <span className="text-xs">
                      {failed > 0 && <span className="text-[#888]">{failed} 份失败 </span>}
                      {revoked && <span className="text-[#ef4444]">{revoked.text}</span>}
                    </span>
                  )}
                </span>
                {ok ? (
                  <button type="button" onClick={() => toast(`正在下载 reports-${t.id}.zip`)} className="flex items-center gap-1 text-sm font-semibold hover:underline cursor-pointer" style={{ color: Y }}>
                    下载 <ArrowDown className="h-4 w-4" />
                  </button>
                ) : (
                  <span className="text-sm text-[#5a5a5a]">{t.status === "processing" ? "生成中" : t.filesExpired ? "已过期" : "失败"}</span>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
