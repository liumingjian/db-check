// PROTOTYPE — throwaway. Round 2: B refined after feedback.
// Hierarchy follows usage: 生成报告 (most frequent) > 采集器 > 我的报告 (rare,
// re-download only). Admin pages live behind one 管理 entry so engineers'
// screens carry no admin noise. Motion follows Emil Kowalski's rules: no
// animation on frequent navigation, 150-200ms ease-out for popovers and
// dialogs (scale from 0.95, never 0), 0.97 press feedback, transition-based
// toasts, tabular numbers.
"use client";

import { useContext, useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, Download, FileArchive, Loader2, Plus, Upload, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import {
  detectPlatform,
  formatSize,
  type ConsoleState,
  type DbType,
  type Release,
  type ReportItem,
  type Role,
  type Screen,
} from "./console-state";
import {
  AccountGate,
  Admin,
  Button,
  CopyText,
  DB_LABEL,
  EASE_OUT,
  Menu,
  PageHead,
  PRESS,
  ReportRow,
  USAGE,
  Ui,
  UiHost,
  releaseMenu,
  when,
} from "./kit";

export function B2_SCREENS(role: Role): { key: Screen; label: string }[] {
  const base: { key: Screen; label: string }[] = [
    { key: "new-report", label: "生成报告" },
    { key: "collectors", label: "采集器" },
    { key: "reports", label: "我的报告" },
  ];
  return role === "admin" ? [...base, { key: "users", label: "管理" }] : base;
}

/* ── Design tokens, scoped to this variant ── */

const TONES = {
  light: {
    "--background": "#fafafa",
    "--foreground": "#171717",
    "--card": "#ffffff",
    "--popover": "#ffffff",
    "--primary": "#171717",
    "--primary-foreground": "#fafafa",
    "--muted": "#f2f2f2",
    "--muted-foreground": "#6f6f6f",
    "--border": "rgba(0,0,0,0.08)",
    "--destructive": "#dc2626",
    "--warning": "#b45309",
    "--success": "#15803d",
    "--ring": "rgba(0,0,0,0.25)",
  },
  dark: {
    "--background": "#0a0a0a",
    "--foreground": "#ededed",
    "--card": "#111111",
    "--popover": "#171717",
    "--primary": "#ededed",
    "--primary-foreground": "#0a0a0a",
    "--muted": "#1c1c1c",
    "--muted-foreground": "#a1a1a1",
    "--border": "rgba(255,255,255,0.09)",
    "--destructive": "#f87171",
    "--warning": "#fbbf24",
    "--success": "#4ade80",
    "--ring": "rgba(255,255,255,0.3)",
  },
} as const;

const FONT = '-apple-system, BlinkMacSystemFont, "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif';

/* ── Shell ── */

export function VariantB2({ state, screen, setScreen, tone }: VariantProps & { tone: "light" | "dark" }) {
  const { me } = state;
  return (
    <div
      style={{ ...TONES[tone], fontFamily: FONT } as React.CSSProperties}
      className="min-h-screen bg-background text-foreground antialiased"
    >
      <UiHost>
        {me.status === "active" ? (
          <>
            <TopBar state={state} screen={screen} setScreen={setScreen} />
            <main className="mx-auto max-w-2xl px-6 pt-28 pb-40">
              {screen === "new-report" && <NewReport state={state} goCollectors={() => setScreen("collectors")} goReports={() => setScreen("reports")} />}
              {screen === "collectors" && <Collectors state={state} />}
              {screen === "reports" && <MyReports state={state} />}
              {screen === "users" && <Admin state={state} />}
            </main>
          </>
        ) : (
          <AccountGate state={state} />
        )}
      </UiHost>
    </div>
  );
}

function TopBar({ state, screen, setScreen }: Omit<VariantProps, "state"> & { state: ConsoleState }) {
  const { me } = state;
  const [open, setOpen] = useState(false);
  return (
    <nav className="fixed inset-x-0 top-0 z-40 bg-background/80 backdrop-blur-xl">
      <div className="mx-auto flex h-16 max-w-5xl items-center gap-8 px-6">
        <span className="text-[15px] font-semibold tracking-tight">DB-Check</span>
        <div className="flex items-center gap-1">
          {B2_SCREENS(me.role).map((s) => (
            <button
              key={s.key}
              type="button"
              onClick={() => setScreen(s.key)}
              className={cn(
                "relative rounded-full px-3 py-1.5 text-sm cursor-pointer",
                screen === s.key ? "bg-muted font-medium text-foreground" : "text-muted-foreground hover:text-foreground",
              )}
            >
              {s.label}
              {s.key === "users" && state.pendingCount > 0 && (
                <span className="ml-1.5 inline-flex h-4 min-w-4 items-center justify-center rounded-full bg-foreground px-1 text-[10px] font-semibold text-background tabular-nums">
                  {state.pendingCount}
                </span>
              )}
            </button>
          ))}
        </div>
        <div className="relative ml-auto">
          <button
            type="button"
            onClick={() => setOpen(!open)}
            className={cn("flex h-8 w-8 items-center justify-center rounded-full bg-muted text-xs font-semibold cursor-pointer", PRESS)}
          >
            {me.displayName.slice(0, 1)}
          </button>
          {open && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
              <div
                className={cn(
                  "absolute right-0 top-full z-50 mt-2 w-52 origin-top-right rounded-xl bg-popover p-1 shadow-lg ring-1 ring-border",
                  "transition-[opacity,transform] duration-150 starting:scale-95 starting:opacity-0",
                  EASE_OUT,
                )}
              >
                <div className="px-3 py-2">
                  <p className="text-sm font-medium">{me.displayName}</p>
                  <p className="text-xs text-muted-foreground">{me.role === "admin" ? "管理员" : "普通用户"} · {me.team}</p>
                </div>
                <div className="my-1 h-px bg-border" />
                <button type="button" className="block w-full rounded-lg px-3 py-1.5 text-left text-sm hover:bg-muted cursor-pointer">修改密码</button>
                <button type="button" className="block w-full rounded-lg px-3 py-1.5 text-left text-sm hover:bg-muted cursor-pointer">退出登录</button>
              </div>
            </>
          )}
        </div>
      </div>
    </nav>
  );
}

/* ── 生成报告 ── */

interface Picked {
  name: string;
  size: number;
  db: DbType;
  version: string;
  aux?: string;
}

function guessDb(name: string): DbType {
  if (/ora|awr/i.test(name)) return "oracle";
  if (/gauss|wdr/i.test(name)) return "gaussdb";
  return "mysql";
}

function NewReport({ state, goCollectors, goReports }: { state: ConsoleState; goCollectors: () => void; goReports: () => void }) {
  const { toast } = useContext(Ui);
  const [files, setFiles] = useState<Picked[]>([]);
  const [drag, setDrag] = useState(false);
  const [progress, setProgress] = useState<number[] | null>(null);
  const [doneId, setDoneId] = useState<string | null>(null);
  const input = useRef<HTMLInputElement>(null);
  const latest = state.latest?.version ?? "1.2.0";

  function add(list: { name: string; size: number }[]) {
    setFiles((fs) => [
      ...fs,
      ...list
        .filter((f) => !fs.some((x) => x.name === f.name))
        .map((f) => ({ ...f, db: guessDb(f.name), version: /legacy|old/i.test(f.name) ? "1.0.0" : latest })),
    ]);
  }

  useEffect(() => {
    if (!progress || doneId) return;
    if (progress.every((p) => p >= 100)) {
      const items: ReportItem[] = files.map((f) => ({
        name: f.name,
        dbType: f.db,
        collectorVersion: f.version,
        outcome: /broken/i.test(f.name) ? "failed" : "success",
      }));
      const t = setTimeout(() => setDoneId(state.addTask(items)), 250);
      return () => clearTimeout(t);
    }
    const t = setTimeout(() => setProgress((ps) => ps!.map((p, i) => Math.min(100, p + 6 + ((i * 7 + p) % 11)))), 120);
    return () => clearTimeout(t);
  }, [progress, doneId, files, state]);

  function reset() {
    setFiles([]);
    setProgress(null);
    setDoneId(null);
  }

  if (doneId) {
    const failed = files.filter((f) => /broken/i.test(f.name)).length;
    const ok = files.length - failed;
    return (
      <div className="flex flex-col items-center pt-10 text-center">
        <div className={cn("flex h-14 w-14 items-center justify-center rounded-full bg-success/15 text-success transition-[opacity,transform] duration-300 starting:scale-90 starting:opacity-0", EASE_OUT)}>
          <Check className="h-7 w-7" strokeWidth={2.5} />
        </div>
        <h1 className="mt-5 text-2xl font-semibold tracking-tight">报告已生成</h1>
        <p className="mt-1.5 text-sm text-muted-foreground">
          {ok} 份报告{failed > 0 && `，${failed} 份失败`}。已保存到
          <button type="button" onClick={goReports} className="mx-0.5 text-foreground underline underline-offset-4 cursor-pointer">我的报告</button>
          ，保留 30 天。
        </p>
        <Button kind="primary" size="lg" className="mt-8 w-64" disabled={ok === 0} onClick={() => toast(`正在下载 reports-${doneId}.zip`)}>
          <Download className="h-4 w-4" />
          下载报告
        </Button>
        <Button kind="ghost" className="mt-2" onClick={reset}>继续生成</Button>
      </div>
    );
  }

  const running = progress !== null;

  return (
    <div>
      <PageHead title="生成巡检报告" sub="上传采集器生成的 ZIP，平台会自动识别数据库类型。" />

      {!running && (
        <label
          onDragOver={(e) => {
            e.preventDefault();
            setDrag(true);
          }}
          onDragLeave={() => setDrag(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDrag(false);
            add([...e.dataTransfer.files].map((f) => ({ name: f.name, size: f.size })));
          }}
          className={cn(
            "flex cursor-pointer flex-col items-center justify-center rounded-2xl border border-dashed px-6 text-center",
            "transition-[background-color,border-color] duration-150",
            files.length ? "py-8" : "py-16",
            drag ? "border-foreground/40 bg-muted" : "border-foreground/15 hover:border-foreground/30 hover:bg-card",
          )}
        >
          <input
            ref={input}
            type="file"
            multiple
            accept=".zip"
            className="hidden"
            onChange={(e) => add([...(e.target.files ?? [])].map((f) => ({ name: f.name, size: f.size })))}
          />
          <Upload className="h-5 w-5 text-muted-foreground" />
          <p className="mt-3 text-sm font-medium">{files.length ? "继续添加" : "拖入 ZIP，或点击选择"}</p>
          {!files.length && <p className="mt-1 text-xs text-muted-foreground">可一次上传多台主机的采集包</p>}
        </label>
      )}

      {files.length > 0 && (
        <ul className="mt-4 divide-y divide-border rounded-2xl bg-card ring-1 ring-border">
          {files.map((f, i) => {
            const notice = state.versionNotice(f.version);
            const auxKind = f.db === "oracle" ? "AWR" : f.db === "gaussdb" ? "WDR" : null;
            return (
              <li key={f.name} className="relative overflow-hidden px-4 py-3">
                {running && (
                  <span
                    className="absolute inset-0 origin-left bg-muted transition-transform duration-150 ease-linear"
                    style={{ transform: `scaleX(${progress[i] / 100})` }}
                  />
                )}
                <div className="relative flex items-center gap-3">
                  <FileArchive className="h-4 w-4 shrink-0 text-muted-foreground" />
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm">{f.name}</p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {DB_LABEL[f.db]} · {formatSize(f.size)} · 采集器 v{f.version}
                      {f.aux && ` · 已附 ${f.aux}`}
                    </p>
                    {notice && (
                      <p className={cn("mt-1 text-xs", notice.tone === "danger" ? "text-destructive" : "text-warning")}>{notice.text}</p>
                    )}
                  </div>
                  {running ? (
                    <span className="text-xs text-muted-foreground tabular-nums">{progress[i] >= 100 ? "完成" : `${progress[i]}%`}</span>
                  ) : (
                    <>
                      {auxKind && !f.aux && (
                        <Button
                          kind="ghost"
                          size="sm"
                          onClick={() => setFiles((fs) => fs.map((x) => (x.name === f.name ? { ...x, aux: `${auxKind} 报告` } : x)))}
                        >
                          <Plus className="h-3 w-3" />
                          {auxKind}
                        </Button>
                      )}
                      <button
                        type="button"
                        aria-label="移除"
                        onClick={() => setFiles((fs) => fs.filter((x) => x.name !== f.name))}
                        className="rounded-md p-1 text-muted-foreground hover:bg-muted hover:text-foreground cursor-pointer"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </>
                  )}
                </div>
              </li>
            );
          })}
        </ul>
      )}

      {files.length > 0 && (
        <Button kind="primary" size="lg" className="mt-6 w-full" disabled={running} onClick={() => setProgress(files.map(() => 0))}>
          {running ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          {running ? "正在生成…" : `生成 ${files.length} 份报告`}
        </Button>
      )}

      {!running && (
        <div className="mt-8 flex items-center justify-between text-sm text-muted-foreground">
          <button type="button" onClick={goCollectors} className="hover:text-foreground cursor-pointer">
            还没有采集包？先下载采集器 →
          </button>
          {files.length === 0 && (
            <button
              type="button"
              onClick={() =>
                add([
                  { name: "bank-ora-01.zip", size: 2_400_000 },
                  { name: "mall-mysql.zip", size: 1_100_000 },
                  { name: "legacy-ora.zip", size: 1_900_000 },
                ])
              }
              className="text-xs hover:text-foreground cursor-pointer"
            >
              原型：填入示例文件
            </button>
          )}
        </div>
      )}
    </div>
  );
}

function Collectors({ state }: { state: ConsoleState }) {
  const { toast, ask } = useContext(Ui);
  const [db, setDb] = useState<DbType>("oracle");
  const [showOld, setShowOld] = useState(false);
  const [showNotes, setShowNotes] = useState(false);
  const isAdmin = state.me.role === "admin";
  const latest = state.latest;
  const platform = detectPlatform();
  const mine = latest?.packages.find((p) => p.platform === platform) ?? latest?.packages[0];
  const others = state.releases.filter((r) => r.status !== "latest" && (isAdmin || r.status === "deprecated"));

  function download(r: Release, p: Release["packages"][number]) {
    state.download(r.version, p.platform);
    toast(`正在下载 ${p.fileName}`);
  }

  return (
    <div>
      <PageHead title="采集器" sub="在客户数据库主机上运行，生成用于报告的 ZIP。" />

      {latest && mine ? (
        <section className="rounded-2xl bg-card p-6 ring-1 ring-border">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-3xl font-semibold tracking-tight tabular-nums">v{latest.version}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                最新版本 · {when(latest.publishedAt).split(" ")[0]}发布 · 支持 {latest.dbTypes.map((d) => DB_LABEL[d]).join("、")}
              </p>
            </div>
            <Menu items={releaseMenu(state, ask, latest)} />
          </div>

          <Button kind="primary" size="lg" className="mt-6 w-full" onClick={() => download(latest, mine)}>
            <Download className="h-4 w-4" />
            下载 {mine.osLabel} {mine.archLabel} 版
            <span className="font-normal opacity-60 tabular-nums">{formatSize(mine.size)}</span>
          </Button>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
            <span>
              其他平台：
              {latest.packages
                .filter((p) => p !== mine)
                .map((p, i) => (
                  <span key={p.platform}>
                    {i > 0 && " · "}
                    <button type="button" onClick={() => download(latest, p)} className="hover:text-foreground cursor-pointer">
                      {p.osLabel} {p.archLabel}
                    </button>
                  </span>
                ))}
            </span>
            <CopyText text={mine.sha256} label={`SHA256 ${mine.sha256.slice(0, 10)}…`} />
          </div>
        </section>
      ) : (
        <section className="rounded-2xl bg-card p-8 text-center ring-1 ring-border">
          <AlertTriangle className="mx-auto h-5 w-5 text-warning" />
          <p className="mt-3 font-medium">暂无推荐版本</p>
          <p className="mt-1 text-sm text-muted-foreground">
            {isAdmin ? "在下方历史版本中选择一个「设为最新」。" : "请联系管理员；如有急需，可使用下方历史版本。"}
          </p>
        </section>
      )}

      <section className="mt-12">
        <h2 className="text-sm font-medium">使用方法</h2>
        <ol className="mt-4 space-y-5 text-sm">
          <li className="flex gap-3">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-muted text-[11px] tabular-nums">1</span>
            <span className="text-muted-foreground">把采集器上传到数据库主机并解压。</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-muted text-[11px] tabular-nums">2</span>
            <div className="min-w-0 flex-1">
              <p className="text-muted-foreground">运行采集命令：</p>
              <div className="mt-2 overflow-hidden rounded-xl bg-muted">
                <div className="flex gap-1 px-2 pt-2">
                  {(Object.keys(USAGE) as DbType[]).map((k) => (
                    <button
                      key={k}
                      type="button"
                      onClick={() => setDb(k)}
                      className={cn("rounded-md px-2 py-0.5 text-xs cursor-pointer", db === k ? "bg-card text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground")}
                    >
                      {DB_LABEL[k]}
                    </button>
                  ))}
                </div>
                <div className="flex items-start gap-3 px-3 py-3">
                  <code className="flex-1 font-mono text-xs leading-relaxed break-all">{USAGE[db]}</code>
                  <CopyText text={USAGE[db]} label="" />
                </div>
              </div>
            </div>
          </li>
          <li className="flex gap-3">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-muted text-[11px] tabular-nums">3</span>
            <span className="text-muted-foreground">把生成的 ZIP 带回来，在「生成报告」上传。</span>
          </li>
        </ol>
      </section>

      <section className="mt-12 space-y-3 border-t border-border pt-6 text-sm">
        {latest && (
          <div>
            <button type="button" onClick={() => setShowNotes(!showNotes)} className="text-muted-foreground hover:text-foreground cursor-pointer">
              v{latest.version} 更新内容 {showNotes ? "↑" : "↓"}
            </button>
            {showNotes && <pre className="mt-2 font-sans text-sm whitespace-pre-wrap text-muted-foreground">{latest.notes}</pre>}
          </div>
        )}
        {others.length > 0 && (
          <div>
            <button type="button" onClick={() => setShowOld(!showOld)} className="text-muted-foreground hover:text-foreground cursor-pointer">
              历史版本 {showOld ? "↑" : "↓"}
            </button>
            {showOld && (
              <ul className="mt-2 divide-y divide-border">
                {others.map((r) => (
                  <li key={r.version} className="flex items-center gap-3 py-2.5">
                    <span className="w-24 font-medium tabular-nums">v{r.version}</span>
                    <span
                      className={cn(
                        "text-xs",
                        r.status === "deprecated" && "text-warning",
                        r.status === "revoked" && "text-destructive",
                        r.status === "pre-release" && "text-muted-foreground",
                      )}
                      title={r.revokeReason}
                    >
                      {r.status === "deprecated" ? "已弃用" : r.status === "revoked" ? `已撤回：${r.revokeReason}` : "预发布 · 仅管理员可见"}
                    </span>
                    <span className="ml-auto flex items-center gap-1">
                      {r.status !== "revoked" || isAdmin ? (
                        <Button kind="ghost" size="sm" onClick={() => download(r, r.packages.find((p) => p.platform === platform) ?? r.packages[0])}>
                          下载
                        </Button>
                      ) : null}
                      <Menu items={releaseMenu(state, ask, r)} />
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </section>
    </div>
  );
}

/* ── 我的报告 ── */

function MyReports({ state }: { state: ConsoleState }) {
  const mine = state.allTasks.filter((t) => t.submitterId === state.me.id);
  return (
    <div>
      <PageHead title="我的报告" sub="生成过的报告保留 30 天，忘了下载或弄丢了可以在这里重新下载。" />
      {mine.length ? (
        <ul className="divide-y divide-border rounded-2xl bg-card ring-1 ring-border">
          {mine.map((t) => <ReportRow key={t.id} state={state} task={t} />)}
        </ul>
      ) : (
        <p className="rounded-2xl py-16 text-center text-sm text-muted-foreground ring-1 ring-border">还没有生成过报告</p>
      )}
    </div>
  );
}

