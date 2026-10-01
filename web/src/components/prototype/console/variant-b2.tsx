// PROTOTYPE — throwaway. Round 2: B refined after feedback.
// Hierarchy follows usage: 生成报告 (most frequent) > 采集器 > 我的报告 (rare,
// re-download only). Admin pages live behind one 管理 entry so engineers'
// screens carry no admin noise. Motion follows Emil Kowalski's rules: no
// animation on frequent navigation, 150-200ms ease-out for popovers and
// dialogs (scale from 0.95, never 0), 0.97 press feedback, transition-based
// toasts, tabular numbers.
"use client";

import { createContext, useContext, useEffect, useRef, useState } from "react";
import {
  AlertTriangle,
  Check,
  Copy,
  Download,
  FileArchive,
  Hourglass,
  Loader2,
  MoreHorizontal,
  Plus,
  Upload,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import {
  RELEASE_ACTION_LABEL,
  detectPlatform,
  formatSize,
  releaseActionsFor,
  type ConsoleState,
  type DbType,
  type Release,
  type ReportItem,
  type ReportTask,
  type Role,
  type Screen,
  type User,
} from "./console-state";

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
const EASE_OUT = "ease-[cubic-bezier(0.23,1,0.32,1)]";
const PRESS = `transition-[transform,background-color,color,opacity] duration-150 ${EASE_OUT} active:scale-[0.97]`;
const TODAY = "2026-10-01";
const DB_LABEL: Record<DbType, string> = { mysql: "MySQL", oracle: "Oracle", gaussdb: "GaussDB" };

/* ── Primitives ── */

function Button({
  kind = "secondary",
  size = "md",
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { kind?: "primary" | "secondary" | "ghost" | "danger"; size?: "sm" | "md" | "lg" }) {
  return (
    <button
      type="button"
      {...props}
      className={cn(
        "inline-flex items-center justify-center gap-1.5 rounded-lg font-medium whitespace-nowrap cursor-pointer select-none disabled:pointer-events-none disabled:opacity-40",
        PRESS,
        size === "sm" && "h-7 px-2.5 text-xs",
        size === "md" && "h-9 px-3.5 text-sm",
        size === "lg" && "h-12 px-5 text-[15px]",
        kind === "primary" && "bg-primary text-primary-foreground hover:opacity-90",
        kind === "secondary" && "bg-card text-foreground shadow-[0_0_0_1px_var(--border),0_1px_2px_rgba(0,0,0,0.04)] hover:bg-muted",
        kind === "ghost" && "text-muted-foreground hover:bg-muted hover:text-foreground",
        kind === "danger" && "bg-destructive text-white hover:opacity-90",
        className,
      )}
    />
  );
}

interface AskOpts {
  title: string;
  body?: React.ReactNode;
  input?: string;
  confirm: string;
  danger?: boolean;
  onConfirm?: (value: string) => void;
}

const Ui = createContext<{ toast: (msg: string) => void; ask: (o: AskOpts) => void }>({
  toast: () => {},
  ask: () => {},
});

function Dialog({ opts, onClose }: { opts: AskOpts; onClose: () => void }) {
  const [value, setValue] = useState("");
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="fixed inset-0 z-[90] flex items-center justify-center p-4">
      <div onClick={onClose} className="absolute inset-0 bg-black/30 transition-opacity duration-200 starting:opacity-0" />
      <div
        className={cn(
          "relative w-full max-w-sm rounded-2xl bg-popover p-5 shadow-2xl ring-1 ring-border",
          "transition-[opacity,transform] duration-200 starting:scale-95 starting:opacity-0",
          EASE_OUT,
        )}
      >
        <h2 className="text-[15px] font-semibold">{opts.title}</h2>
        {opts.body && <div className="mt-1.5 text-sm text-muted-foreground">{opts.body}</div>}
        {opts.input !== undefined && (
          <textarea
            autoFocus
            rows={3}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder={opts.input}
            className="mt-4 w-full resize-none rounded-lg bg-muted px-3 py-2 text-sm outline-none ring-1 ring-transparent focus:ring-ring"
          />
        )}
        <div className="mt-5 flex justify-end gap-2">
          {opts.onConfirm && <Button kind="ghost" onClick={onClose}>取消</Button>}
          <Button
            kind={opts.danger ? "danger" : "primary"}
            disabled={opts.input !== undefined && !value.trim()}
            onClick={() => {
              opts.onConfirm?.(value.trim());
              onClose();
            }}
          >
            {opts.confirm}
          </Button>
        </div>
      </div>
    </div>
  );
}

function Menu({ items }: { items: { label: string; onSelect: () => void; danger?: boolean }[] }) {
  const [open, setOpen] = useState(false);
  if (items.length === 0) return null;
  return (
    <div className="relative">
      <button
        type="button"
        aria-label="更多操作"
        onClick={() => setOpen(!open)}
        className={cn("flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground cursor-pointer", PRESS)}
      >
        <MoreHorizontal className="h-4 w-4" />
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div
            className={cn(
              "absolute right-0 top-full z-50 mt-1 min-w-36 origin-top-right rounded-xl bg-popover p-1 shadow-lg ring-1 ring-border",
              "transition-[opacity,transform] duration-150 starting:scale-95 starting:opacity-0",
              EASE_OUT,
            )}
          >
            {items.map((it) => (
              <button
                key={it.label}
                type="button"
                onClick={() => {
                  setOpen(false);
                  it.onSelect();
                }}
                className={cn(
                  "block w-full rounded-lg px-3 py-1.5 text-left text-sm cursor-pointer hover:bg-muted",
                  it.danger && "text-destructive",
                )}
              >
                {it.label}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function CopyText({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      title={text}
      onClick={() => {
        void navigator.clipboard?.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
      className="inline-flex items-center gap-1.5 font-mono text-xs text-muted-foreground hover:text-foreground cursor-pointer"
    >
      {label ?? text}
      {copied ? <Check className="h-3 w-3 text-success" /> : <Copy className="h-3 w-3" />}
    </button>
  );
}

function PageHead({ title, sub }: { title: string; sub: string }) {
  return (
    <header className="mb-8">
      <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
      <p className="mt-1 text-sm text-muted-foreground">{sub}</p>
    </header>
  );
}

function when(iso: string): string {
  const day = iso.slice(0, 10);
  const time = iso.slice(11, 16);
  if (day === TODAY) return `今天 ${time}`;
  if (day === "2026-09-30") return `昨天 ${time}`;
  return `${Number(iso.slice(5, 7))}月${Number(iso.slice(8, 10))}日 ${time}`;
}

/* ── Shell ── */

export function VariantB2({ state, screen, setScreen, tone }: VariantProps & { tone: "light" | "dark" }) {
  const [toasts, setToasts] = useState<{ id: number; msg: string }[]>([]);
  const [dialog, setDialog] = useState<AskOpts | null>(null);
  const seq = useRef(0);

  function toast(msg: string) {
    const id = ++seq.current;
    setToasts((t) => [...t.slice(-2), { id, msg }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4000);
  }

  const { me } = state;

  return (
    <Ui.Provider value={{ toast, ask: setDialog }}>
      <div
        style={{ ...TONES[tone], fontFamily: FONT } as React.CSSProperties}
        className="min-h-screen bg-background text-foreground antialiased"
      >
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

        <div className="fixed right-5 bottom-5 z-[95] flex w-80 flex-col gap-2">
          {toasts.map((t) => (
            <div
              key={t.id}
              className="flex items-center gap-2 rounded-xl bg-popover px-4 py-3 text-sm shadow-lg ring-1 ring-border transition-[opacity,transform] duration-400 ease-out starting:translate-y-full starting:opacity-0"
            >
              <Check className="h-4 w-4 shrink-0 text-success" />
              {t.msg}
            </div>
          ))}
        </div>
        {dialog && <Dialog opts={dialog} onClose={() => setDialog(null)} />}
      </div>
    </Ui.Provider>
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

/* ── 采集器 ── */

const USAGE: Record<DbType, string> = {
  mysql: "./db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password '***' --dbname dbcheck",
  oracle: "./db-collector --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password '***' --dbname ORCL",
  gaussdb: "./db-collector --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password '***' --dbname postgres",
};

function releaseMenu(state: ConsoleState, ask: (o: AskOpts) => void, r: Release) {
  if (state.me.role !== "admin") return [];
  return releaseActionsFor(r.status).map((a) => ({
    label: RELEASE_ACTION_LABEL[a],
    danger: a === "revoke",
    onSelect: () =>
      a === "revoke"
        ? ask({
            title: `撤回 v${r.version}`,
            body: "撤回后普通用户将看不到此版本，用它采集的数据生成报告时会显示警告。",
            input: "撤回原因（会展示给用户）",
            confirm: "撤回",
            danger: true,
            onConfirm: (reason) => state.releaseActions.revoke(r.version, reason),
          })
        : state.releaseActions[a](r.version),
  }));
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

function ReportRow({ state, task, showSubmitter }: { state: ConsoleState; task: ReportTask; showSubmitter?: boolean }) {
  const { toast } = useContext(Ui);
  const first = task.items[0];
  const failed = task.items.filter((i) => i.outcome === "failed").length;
  const revoked = task.items.map((i) => state.versionNotice(i.collectorVersion)).find((n) => n?.tone === "danger");

  return (
    <li className="flex items-center gap-4 px-4 py-3.5">
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm">
          {first.name}
          {task.items.length > 1 && <span className="text-muted-foreground"> 等 {task.items.length} 份</span>}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground tabular-nums">
          {when(task.createdAt)}
          {showSubmitter && ` · ${state.userById(task.submitterId)?.displayName}`}
          {failed > 0 && ` · ${failed} 份失败`}
        </p>
        {revoked && <p className="mt-1 text-xs text-destructive">{revoked.text}</p>}
      </div>
      {task.status === "processing" ? (
        <span className="flex items-center gap-1.5 text-xs text-muted-foreground"><Loader2 className="h-3 w-3 animate-spin" />生成中</span>
      ) : task.filesExpired ? (
        <span className="text-xs text-muted-foreground">已过期</span>
      ) : failed === task.items.length ? (
        <span className="text-xs text-muted-foreground">生成失败</span>
      ) : (
        <Button size="sm" onClick={() => toast(`正在下载 reports-${task.id}.zip`)}>
          <Download className="h-3.5 w-3.5" />
          下载
        </Button>
      )}
    </li>
  );
}

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

/* ── 管理 ── */

function Admin({ state }: { state: ConsoleState }) {
  const [tab, setTab] = useState<"users" | "downloads" | "reports">("users");
  const tabs = [
    { key: "users", label: "用户", count: state.pendingCount },
    { key: "downloads", label: "下载记录", count: 0 },
    { key: "reports", label: "全部报告", count: 0 },
  ] as const;

  return (
    <div>
      <PageHead title="管理" sub="审批账号、查看下载与全部报告。版本状态在「采集器」页的 ··· 菜单中调整。" />
      <div className="mb-6 inline-flex rounded-lg bg-muted p-0.5">
        {tabs.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => setTab(t.key)}
            className={cn("rounded-md px-3 py-1 text-sm cursor-pointer", tab === t.key ? "bg-card font-medium shadow-sm" : "text-muted-foreground hover:text-foreground")}
          >
            {t.label}
            {t.count > 0 && <span className="ml-1.5 text-xs tabular-nums text-muted-foreground">{t.count}</span>}
          </button>
        ))}
      </div>
      {tab === "users" && <UsersAdmin state={state} />}
      {tab === "downloads" && <DownloadsAdmin state={state} />}
      {tab === "reports" && <ReportsAdmin state={state} />}
    </div>
  );
}

function UsersAdmin({ state }: { state: ConsoleState }) {
  const { toast, ask } = useContext(Ui);
  const a = state.userActions;
  const pending = state.users.filter((u) => u.status === "pending");
  const rest = state.users.filter((u) => u.status !== "pending");

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
                <div className="rounded-lg bg-muted px-3 py-2"><CopyText text={temp} /></div>
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
    <div className="space-y-10">
      {pending.length > 0 && (
        <section>
          <h2 className="mb-3 text-sm font-medium">待审批</h2>
          <ul className="divide-y divide-border rounded-2xl bg-card ring-1 ring-border">
            {pending.map((u) => (
              <li key={u.id} className="flex items-center gap-4 px-4 py-4">
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium">{u.displayName} <span className="font-normal text-muted-foreground">@{u.username} · {u.team}</span></p>
                  {u.note && <p className="mt-1 text-sm text-muted-foreground">{u.note}</p>}
                  <p className="mt-1 text-xs text-muted-foreground tabular-nums">{u.email} · {when(u.registeredAt)} 申请</p>
                </div>
                <Button
                  kind="ghost"
                  size="sm"
                  onClick={() =>
                    ask({
                      title: `拒绝 ${u.displayName} 的申请`,
                      body: "对方登录后会看到原因，可以修改后重新申请。",
                      input: "拒绝原因",
                      confirm: "拒绝",
                      danger: true,
                      onConfirm: (r) => a.reject(u.id, r),
                    })
                  }
                >
                  拒绝
                </Button>
                <Button
                  kind="primary"
                  size="sm"
                  onClick={() => {
                    a.approve(u.id);
                    toast(`已批准 ${u.displayName}`);
                  }}
                >
                  批准
                </Button>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section>
        <h2 className="mb-3 text-sm font-medium">全部用户</h2>
        <ul className="divide-y divide-border rounded-2xl bg-card ring-1 ring-border">
          {rest.map((u) => (
            <li key={u.id} className="flex items-center gap-3 px-4 py-3">
              <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-semibold">{u.displayName.slice(0, 1)}</span>
              <div className="min-w-0 flex-1">
                <p className="text-sm">
                  {u.displayName}
                  {u.role === "admin" && <span className="ml-2 text-xs text-muted-foreground">管理员</span>}
                  {u.id === state.me.id && <span className="ml-2 text-xs text-muted-foreground">（你）</span>}
                </p>
                <p className="text-xs text-muted-foreground">
                  @{u.username} · {u.team}
                  {u.status !== "active" && (
                    <span className={u.status === "rejected" ? "text-destructive" : "text-warning"}>
                      {" "}· {u.status === "rejected" ? "已拒绝" : "已禁用"}：{u.reason}
                    </span>
                  )}
                </p>
              </div>
              <Menu items={menu(u)} />
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

function DownloadsAdmin({ state }: { state: ConsoleState }) {
  const [user, setUser] = useState("all");
  const list = state.downloads.filter((d) => user === "all" || d.userId === user);
  return (
    <div>
      <select value={user} onChange={(e) => setUser(e.target.value)} className="mb-3 rounded-lg bg-card px-2.5 py-1.5 text-sm ring-1 ring-border outline-none">
        <option value="all">全部用户</option>
        {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
      </select>
      <ul className="divide-y divide-border rounded-2xl bg-card text-sm ring-1 ring-border">
        {list.map((d) => (
          <li key={d.id} className="flex items-center gap-4 px-4 py-2.5">
            <span className="w-20">{state.userById(d.userId)?.displayName}</span>
            <span className="w-24 tabular-nums">v{d.version}</span>
            <span className="flex-1 text-xs text-muted-foreground">{d.platform}</span>
            <span className="text-xs text-muted-foreground tabular-nums">{when(d.at)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ReportsAdmin({ state }: { state: ConsoleState }) {
  const [user, setUser] = useState("all");
  const list = state.allTasks.filter((t) => user === "all" || t.submitterId === user);
  return (
    <div>
      <select value={user} onChange={(e) => setUser(e.target.value)} className="mb-3 rounded-lg bg-card px-2.5 py-1.5 text-sm ring-1 ring-border outline-none">
        <option value="all">全部提交人</option>
        {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
      </select>
      <ul className="divide-y divide-border rounded-2xl bg-card ring-1 ring-border">
        {list.map((t) => <ReportRow key={t.id} state={state} task={t} showSubmitter />)}
      </ul>
    </div>
  );
}

/* ── 待审批 / 已拒绝 ── */

function AccountGate({ state }: { state: ConsoleState }) {
  const { me } = state;
  const field = "w-full rounded-lg bg-card px-3 py-2 text-sm ring-1 ring-border outline-none focus:ring-ring";
  return (
    <div className="flex min-h-screen items-center justify-center px-6 pb-24">
      <div className="w-full max-w-sm">
        <p className="mb-10 text-[15px] font-semibold tracking-tight">DB-Check</p>
        {me.status === "pending" ? (
          <>
            <Hourglass className="h-5 w-5 text-muted-foreground" />
            <h1 className="mt-4 text-2xl font-semibold tracking-tight">等待审批</h1>
            <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
              {me.displayName}，你的申请已在 {when(me.registeredAt)} 提交。管理员批准后，刷新本页即可开始使用。
            </p>
          </>
        ) : (
          <>
            <h1 className="text-2xl font-semibold tracking-tight">申请未通过</h1>
            <p className="mt-3 rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">{me.reason}</p>
            <p className="mt-6 mb-3 text-sm text-muted-foreground">修改后可重新申请，用户名和邮箱保持不变。</p>
            <div className="space-y-2">
              <input className={field} defaultValue={me.displayName} placeholder="显示名称" />
              <input className={field} defaultValue={me.team} placeholder="团队" />
              <textarea className={cn(field, "resize-none")} rows={3} defaultValue={me.note} placeholder="申请说明" />
            </div>
            <Button kind="primary" size="lg" className="mt-4 w-full" onClick={() => state.userActions.resubmit(me.id)}>
              重新提交申请
            </Button>
          </>
        )}
      </div>
    </div>
  );
}
