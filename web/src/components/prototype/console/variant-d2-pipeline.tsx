// PROTOTYPE — throwaway. Round 3, D2 "流水线": generating a report reads like
// a deployment (after Vercel's Geist DESIGN.md, dark: black canvas, hairline
// grids, Geist with tight display tracking, Geist Mono uppercase eyebrows,
// one mesh-gradient moment, blue = ready). The four platforms sit in an equal
// 2x2 hairline grid.
"use client";

import { useContext, useEffect, useRef, useState } from "react";
import { Check, Download, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import { formatSize, type ConsoleState, type DbType, type Release, type Screen } from "./console-state";
import { DEMO_FILES, stageOf, useReportFlow, type ReportFlow } from "./flow";
import { AccountGate, Admin, CopyText, DB_LABEL, Menu, PRESS, USAGE, Ui, UiHost, releaseMenu, when } from "./kit";

const TOKENS = {
  "--background": "#000000",
  "--foreground": "#ededed",
  "--card": "#0a0a0a",
  "--popover": "#111111",
  "--muted": "#1a1a1a",
  "--muted-foreground": "#a1a1a1",
  "--border": "#262626",
  "--primary": "#ededed",
  "--primary-foreground": "#000000",
  "--destructive": "#ff6166",
  "--warning": "#f5a623",
  "--success": "#3291ff",
  "--ring": "rgba(255,255,255,0.3)",
} as const;

const SANS = 'var(--font-geist), "PingFang SC", -apple-system, sans-serif';
const MONO = "font-[family-name:var(--font-geist-mono)]";
const eyebrow = `${MONO} text-[12px] font-medium uppercase tracking-normal text-[#8f8f8f]`;

function Btn({ children, onClick, kind = "primary", disabled }: { children: React.ReactNode; onClick: () => void; kind?: "primary" | "secondary"; disabled?: boolean }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "inline-flex h-9 items-center gap-2 rounded-md px-3.5 text-sm font-medium cursor-pointer disabled:opacity-40",
        PRESS,
        kind === "primary" ? "bg-[#ededed] text-black hover:bg-white" : "bg-black text-[#ededed] ring-1 ring-[#333] hover:bg-[#111]",
      )}
    >
      {children}
    </button>
  );
}

function Dot({ tone }: { tone: "ready" | "building" | "error" | "idle" }) {
  return (
    <span
      className={cn(
        "inline-block h-2.5 w-2.5 rounded-full",
        tone === "ready" && "bg-[#3291ff]",
        tone === "building" && "animate-pulse bg-[#f5a623]",
        tone === "error" && "bg-[#ff6166]",
        tone === "idle" && "bg-[#444]",
      )}
    />
  );
}

const TABS: { key: Screen; label: string }[] = [
  { key: "new-report", label: "生成" },
  { key: "collectors", label: "采集器" },
  { key: "reports", label: "报告" },
];

export function VariantD2({ state, screen, setScreen }: VariantProps) {
  const { me } = state;
  const tabs = me.role === "admin" ? [...TABS, { key: "users" as Screen, label: "管理" }] : TABS;
  return (
    <div style={{ ...TOKENS, fontFamily: SANS } as React.CSSProperties} className="min-h-screen bg-black text-[#ededed] antialiased">
      <UiHost>
        {me.status !== "active" ? (
          <AccountGate state={state} />
        ) : (
          <>
            <header className="border-b border-[#262626]">
              <div className="mx-auto flex h-14 max-w-[1100px] items-center gap-3 px-6 text-sm">
                <svg viewBox="0 0 24 24" className="h-5 w-5 fill-white" aria-hidden><path d="M4 4h7v7H4zM13 13h7v7h-7zM13 4h7v7h-7z" opacity=".35" /><path d="M4 13h7v7H4z" /></svg>
                <span className="text-[#444]">/</span>
                <span className="font-medium">DB-Check</span>
                <span className="text-[#444]">/</span>
                <span className="flex items-center gap-2 text-[#a1a1a1]">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-gradient-to-br from-[#007cf0] to-[#00dfd8] text-[10px] font-semibold text-black">
                    {me.displayName.slice(0, 1)}
                  </span>
                  {me.displayName}
                  <span className={`${MONO} rounded-full px-1.5 py-px text-[10px] uppercase ring-1 ring-[#333]`}>{me.role === "admin" ? "admin" : "engineer"}</span>
                </span>
              </div>
              <nav className="mx-auto flex max-w-[1100px] gap-1 px-4">
                {tabs.map((t) => (
                  <button
                    key={t.key}
                    type="button"
                    onClick={() => setScreen(t.key)}
                    className={cn(
                      "relative px-3 pt-2 pb-3 text-sm cursor-pointer",
                      screen === t.key ? "text-white" : "text-[#8f8f8f] hover:text-white",
                    )}
                  >
                    {t.label}
                    {t.key === "users" && state.pendingCount > 0 && (
                      <span className={`${MONO} ml-1.5 rounded-full bg-[#3291ff] px-1.5 text-[10px] text-white`}>{state.pendingCount}</span>
                    )}
                    {screen === t.key && <span className="absolute inset-x-3 bottom-0 h-px bg-white" />}
                  </button>
                ))}
              </nav>
            </header>
            <main className="mx-auto max-w-[1100px] px-6 pt-12 pb-40">
              {screen === "new-report" && <Generate state={state} />}
              {screen === "collectors" && <Collectors state={state} />}
              {screen === "reports" && <Reports state={state} />}
              {screen === "users" && (
                <div className="max-w-3xl">
                  <Admin state={state} />
                </div>
              )}
            </main>
          </>
        )}
      </UiHost>
    </div>
  );
}

/* ── 生成 ── */

function Generate({ state }: { state: ConsoleState }) {
  const flow = useReportFlow(state);
  const [drag, setDrag] = useState(false);

  if (flow.progress) return <Deployment flow={flow} />;

  return (
    <div className="relative">
      <div aria-hidden className="pointer-events-none absolute -top-24 left-1/2 h-[380px] w-[760px] -translate-x-1/2 opacity-40 blur-3xl">
        <div className="absolute left-[8%] top-[20%] h-56 w-56 rounded-full bg-[#007cf0]" />
        <div className="absolute left-[34%] top-[6%] h-64 w-64 rounded-full bg-[#7928ca]" />
        <div className="absolute left-[58%] top-[24%] h-56 w-56 rounded-full bg-[#ff0080]" />
        <div className="absolute left-[74%] top-[2%] h-44 w-44 rounded-full bg-[#f9cb28]" />
      </div>

      <div className="relative">
        <p className={eyebrow}>New report</p>
        <h1 className="mt-3 text-[48px] leading-[48px] font-semibold tracking-[-2.4px]">上传采集包，拿到巡检报告</h1>
        <p className="mt-4 text-base text-[#a1a1a1]">数据库类型从 manifest 自动识别。支持 MySQL、Oracle、GaussDB。</p>

        <label
          onDragOver={(e) => {
            e.preventDefault();
            setDrag(true);
          }}
          onDragLeave={() => setDrag(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDrag(false);
            flow.add([...e.dataTransfer.files].map((f) => ({ name: f.name, size: f.size })));
          }}
          className={cn(
            "mt-10 block cursor-pointer overflow-hidden rounded-xl bg-[#0a0a0a] ring-1 transition-[box-shadow] duration-150",
            drag ? "ring-[#3291ff]" : "ring-[#262626] hover:ring-[#444]",
          )}
        >
          <input
            type="file"
            multiple
            accept=".zip"
            className="hidden"
            onChange={(e) => flow.add([...(e.target.files ?? [])].map((f) => ({ name: f.name, size: f.size })))}
          />
          <div className={`${MONO} flex items-center justify-between border-b border-[#262626] px-4 py-2.5 text-xs text-[#8f8f8f]`}>
            <span>~/reports/new</span>
            <span>{flow.files.length} files</span>
          </div>
          {flow.files.length === 0 ? (
            <div className={`${MONO} px-6 py-14 text-sm`}>
              <span className="text-[#8f8f8f]">$</span> 拖入 *.zip，或点击选择文件<span className="ml-0.5 inline-block h-4 w-2 translate-y-0.5 animate-pulse bg-[#ededed]" />
            </div>
          ) : (
            <div className="divide-y divide-[#1a1a1a]">
              {flow.files.map((f) => {
                const n = flow.notice(f.version);
                return (
                  <div key={f.name} className="flex items-center gap-4 px-4 py-3 text-sm" onClick={(e) => e.preventDefault()}>
                    <span className={`${MONO} min-w-0 flex-1 truncate`}>{f.name}</span>
                    <span className="w-20 text-[#a1a1a1]">{DB_LABEL[f.db]}</span>
                    <span className={cn(`${MONO} w-44 text-xs`, n?.tone === "danger" ? "text-[#ff6166]" : n ? "text-[#f5a623]" : "text-[#8f8f8f]")}>
                      collector v{f.version}{n?.tone === "danger" ? " · revoked" : n ? " · deprecated" : ""}
                    </span>
                    <span className="w-20 text-right text-xs">
                      {f.aux ? (
                        <span className="text-[#8f8f8f]">+ {f.aux.slice(0, 3)}</span>
                      ) : f.db !== "mysql" ? (
                        <button type="button" onClick={() => flow.attach(f.name)} className="text-[#a1a1a1] hover:text-white cursor-pointer">
                          + {f.db === "oracle" ? "AWR" : "WDR"}
                        </button>
                      ) : null}
                    </span>
                    <button type="button" aria-label="移除" onClick={() => flow.remove(f.name)} className="text-[#666] hover:text-white cursor-pointer">
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                );
              })}
              <p className={`${MONO} px-4 py-3 text-xs text-[#666]`}>$ 继续拖入以追加</p>
            </div>
          )}
        </label>

        <div className="mt-5 flex items-center justify-between">
          {flow.files.length === 0 ? (
            <button type="button" onClick={() => flow.add(DEMO_FILES)} className={`${MONO} text-xs text-[#666] hover:text-white cursor-pointer`}>
              prototype: use sample files
            </button>
          ) : (
            <span className="text-xs text-[#8f8f8f]">{flow.files.length} 个采集包 · 预计 {flow.files.length * 3} 秒</span>
          )}
          <Btn onClick={flow.start} disabled={!flow.files.length}>生成报告</Btn>
        </div>
      </div>
    </div>
  );
}

function Deployment({ flow }: { flow: ReportFlow }) {
  const { toast } = useContext(Ui);
  const logRef = useRef<HTMLDivElement>(null);
  const ready = !!flow.doneId;

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [flow.logs.length]);

  return (
    <div>
      <p className={eyebrow}>{ready ? flow.doneId : "Generating"}</p>
      <div className="mt-3 flex items-end justify-between">
        <h1 className="text-[32px] leading-10 font-semibold tracking-[-1.28px]">{ready ? "报告已就绪" : "正在生成报告"}</h1>
        {ready && (
          <div className="flex gap-2">
            <Btn kind="secondary" onClick={flow.reset}>再生成一份</Btn>
            <Btn onClick={() => toast(`正在下载 reports-${flow.doneId}.zip`)} disabled={flow.okCount === 0}>
              <Download className="h-4 w-4" />下载报告
            </Btn>
          </div>
        )}
      </div>

      <div className="mt-8 grid grid-cols-4 gap-px overflow-hidden rounded-xl bg-[#262626] ring-1 ring-[#262626]">
        {[
          { k: "Status", v: <span className="flex items-center gap-2"><Dot tone={ready ? "ready" : "building"} />{ready ? "Ready" : "Building"}</span> },
          { k: "Duration", v: <span className="tabular-nums">{flow.elapsed}s</span> },
          { k: "Reports", v: <span className="tabular-nums">{ready ? `${flow.okCount} / ${flow.files.length}` : flow.files.length}</span> },
          { k: "Kept", v: "30 天" },
        ].map((c) => (
          <div key={c.k} className="bg-[#0a0a0a] px-5 py-4">
            <p className={eyebrow}>{c.k}</p>
            <div className="mt-2 text-sm font-medium">{c.v}</div>
          </div>
        ))}
      </div>

      <div className="mt-6 space-y-px overflow-hidden rounded-xl ring-1 ring-[#262626]">
        {flow.files.map((f, i) => {
          const p = flow.progress![i];
          return (
            <div key={f.name} className="relative flex items-center gap-4 bg-[#0a0a0a] px-5 py-3 text-sm">
              <span className="absolute inset-x-0 bottom-0 h-px origin-left bg-[#3291ff] transition-transform duration-150 ease-linear" style={{ transform: `scaleX(${p / 100})` }} />
              <Dot tone={p >= 100 ? "ready" : "building"} />
              <span className={`${MONO} flex-1 truncate`}>{f.name}</span>
              <span className={`${MONO} w-16 text-right text-xs text-[#8f8f8f]`}>{stageOf(p)}</span>
            </div>
          );
        })}
      </div>

      <div className="mt-6 overflow-hidden rounded-xl bg-[#0a0a0a] ring-1 ring-[#262626]">
        <div className="flex items-center justify-between border-b border-[#262626] px-5 py-3">
          <p className={eyebrow}>Build logs</p>
          <p className={`${MONO} text-xs text-[#666]`}>{flow.logs.length} lines</p>
        </div>
        <div ref={logRef} className={`${MONO} max-h-72 overflow-y-auto px-5 py-3 text-[13px] leading-6`}>
          {flow.logs.map((l, i) => (
            <div key={i} className="flex gap-4">
              <span className="w-20 shrink-0 text-[#555] tabular-nums">{l.t}</span>
              <span className={cn(l.tone === "danger" && "text-[#ff6166]", l.tone === "warn" && "text-[#f5a623]", l.tone === "ok" && "text-[#3291ff]", !l.tone && "text-[#a1a1a1]")}>
                {l.tone === "ok" && <Check className="mr-1 inline h-3.5 w-3.5" />}
                {l.text}
              </span>
            </div>
          ))}
          {ready && <div className="flex gap-4"><span className="w-20 text-[#555]" /><span className="text-white">Ready · {flow.doneId}</span></div>}
        </div>
      </div>
    </div>
  );
}

/* ── 采集器 ── */

function Collectors({ state }: { state: ConsoleState }) {
  const { toast, ask } = useContext(Ui);
  const [db, setDb] = useState<DbType>("oracle");
  const latest = state.latest;
  const isAdmin = state.me.role === "admin";
  const releases = state.releases.filter((r) => isAdmin || r.status === "latest" || r.status === "deprecated");

  function dl(r: Release, p: Release["packages"][number]) {
    state.download(r.version, p.platform);
    toast(`正在下载 ${p.fileName}`);
  }

  return (
    <div>
      <div className="flex items-end justify-between">
        <div>
          <p className={eyebrow}>{latest ? "Collector · latest" : "Collector"}</p>
          <h1 className="mt-3 text-[48px] leading-[48px] font-semibold tracking-[-2.4px]">{latest ? `v${latest.version}` : "暂无推荐版本"}</h1>
          {latest && (
            <p className={`${MONO} mt-3 text-xs text-[#8f8f8f]`}>
              {latest.tag} · {latest.commit} · {latest.publishedAt.slice(0, 10)} · {latest.dbTypes.join(" / ")}
            </p>
          )}
        </div>
        {latest && <Menu items={releaseMenu(state, ask, latest)} />}
      </div>

      {latest ? (
        <div className="mt-10 grid grid-cols-2 gap-px overflow-hidden rounded-xl bg-[#262626] ring-1 ring-[#262626]">
          {latest.packages.map((p) => (
            <div key={p.platform} className="group bg-black p-7 transition-colors duration-150 hover:bg-[#0a0a0a]">
              <p className={eyebrow}>{p.osLabel}</p>
              <p className="mt-2 text-[32px] leading-10 font-semibold tracking-[-1.28px]">{p.archLabel}</p>
              <p className={`${MONO} mt-4 truncate text-xs text-[#8f8f8f]`}>{p.fileName}</p>
              <div className="mt-1 flex items-center gap-3 text-xs text-[#8f8f8f]">
                <span className="tabular-nums">{formatSize(p.size)}</span>
                <CopyText text={p.sha256} label={`sha256 ${p.sha256.slice(0, 8)}`} />
              </div>
              <div className="mt-6">
                <Btn kind="secondary" onClick={() => dl(latest, p)}>
                  <Download className="h-4 w-4" />下载
                </Btn>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="mt-6 text-sm text-[#a1a1a1]">{isAdmin ? "在下方版本列表中把一个版本设为最新。" : "请联系管理员，或使用下方的历史版本。"}</p>
      )}

      <section className="mt-16">
        <p className={eyebrow}>Usage</p>
        <div className="mt-4 overflow-hidden rounded-xl bg-[#0a0a0a] ring-1 ring-[#262626]">
          <div className="flex gap-4 border-b border-[#262626] px-5">
            {(Object.keys(USAGE) as DbType[]).map((k) => (
              <button
                key={k}
                type="button"
                onClick={() => setDb(k)}
                className={cn("relative py-3 text-sm cursor-pointer", db === k ? "text-white" : "text-[#8f8f8f] hover:text-white")}
              >
                {DB_LABEL[k]}
                {db === k && <span className="absolute inset-x-0 bottom-0 h-px bg-white" />}
              </button>
            ))}
          </div>
          <div className="flex items-start gap-4 px-5 py-4">
            <code className={`${MONO} flex-1 text-[13px] leading-6 break-all`}>
              <span className="text-[#555]">$ </span>{USAGE[db]}
            </code>
            <CopyText text={USAGE[db]} label="" />
          </div>
          <p className="border-t border-[#262626] px-5 py-3 text-xs text-[#8f8f8f]">在客户数据库主机上解压后运行，生成的 ZIP 拿回来在「生成」上传。</p>
        </div>
      </section>

      <section className="mt-16">
        <p className={eyebrow}>Releases</p>
        <div className="mt-4 divide-y divide-[#1a1a1a] overflow-hidden rounded-xl ring-1 ring-[#262626]">
          {releases.map((r) => (
            <div key={r.version} className="flex items-center gap-5 bg-[#0a0a0a] px-5 py-3 text-sm">
              <span className={`${MONO} w-28`}>v{r.version}</span>
              <span className="flex w-24 items-center gap-2 text-xs text-[#a1a1a1]">
                <Dot tone={r.status === "latest" ? "ready" : r.status === "revoked" ? "error" : r.status === "deprecated" ? "building" : "idle"} />
                {{ latest: "Latest", deprecated: "Deprecated", revoked: "Revoked", "pre-release": "Pre-release" }[r.status]}
              </span>
              <span className="flex-1 truncate text-xs text-[#8f8f8f]">{r.revokeReason ?? r.notes.split("\n")[0].replace(/^- /, "")}</span>
              <span className={`${MONO} text-xs text-[#666]`}>{r.publishedAt.slice(0, 10)}</span>
              <Menu items={releaseMenu(state, ask, r)} />
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

/* ── 报告 ── */

function Reports({ state }: { state: ConsoleState }) {
  const { toast } = useContext(Ui);
  const mine = state.allTasks.filter((t) => t.submitterId === state.me.id);
  return (
    <div>
      <p className={eyebrow}>Reports · kept 30 days</p>
      <h1 className="mt-3 text-[32px] leading-10 font-semibold tracking-[-1.28px]">我的报告</h1>
      <div className="mt-8 divide-y divide-[#1a1a1a] overflow-hidden rounded-xl ring-1 ring-[#262626]">
        {mine.map((t) => {
          const failed = t.items.filter((i) => i.outcome === "failed").length;
          const revoked = t.items.map((i) => state.versionNotice(i.collectorVersion)).find((n) => n?.tone === "danger");
          const tone = t.status === "processing" ? "building" : t.filesExpired ? "idle" : failed === t.items.length ? "error" : "ready";
          return (
            <div key={t.id} className={cn("flex items-center gap-5 bg-[#0a0a0a] px-5 py-4", t.filesExpired && "opacity-60")}>
              <span className="flex w-24 items-center gap-2 text-xs text-[#a1a1a1]">
                <Dot tone={tone} />
                {t.status === "processing" ? "Building" : t.filesExpired ? "Expired" : failed === t.items.length ? "Error" : "Ready"}
              </span>
              <div className="min-w-0 flex-1">
                <p className={`${MONO} truncate text-sm`}>{t.items.map((i) => i.name).join(", ")}</p>
                <p className="mt-0.5 text-xs text-[#8f8f8f]">
                  {t.id}
                  {failed > 0 && failed < t.items.length && ` · ${failed} 份失败`}
                  {revoked && <span className="text-[#ff6166]"> · {revoked.text}</span>}
                </p>
              </div>
              <span className="text-xs text-[#8f8f8f] tabular-nums">{when(t.createdAt)}</span>
              {tone === "ready" ? (
                <Btn kind="secondary" onClick={() => toast(`正在下载 reports-${t.id}.zip`)}>
                  <Download className="h-4 w-4" />
                </Btn>
              ) : (
                <span className="w-[46px]" />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
