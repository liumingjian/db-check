// PROTOTYPE — throwaway. Round 3, D1 "指令台": the whole app is one command
// palette (after Raycast's DESIGN.md: #07080a canvas, hairline #242728,
// surface ladder instead of shadows, white CTA, Inter ss03, keycaps, one red
// stripe band). Every action is a searchable row: each of the four platforms
// is its own download command, recent reports are commands, and dropping a
// ZIP anywhere starts a report.
"use client";

import { useContext, useRef, useState } from "react";
import {
  AppWindow,
  BookOpen,
  Check,
  ClipboardList,
  Download,
  FileArchive,
  FileUp,
  History,
  Search,
  Server,
  Users,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import { formatSize, type ConsoleState, type DbType, type Release, type ReportTask, type Screen } from "./console-state";
import { DEMO_FILES, stageOf, useReportFlow, type ReportFlow } from "./flow";
import {
  AccountGate,
  CopyText,
  DB_LABEL,
  DownloadsAdmin,
  EASE_OUT,
  Menu,
  PRESS,
  ReportsAdmin,
  USAGE,
  Ui,
  UiHost,
  UsersAdmin,
  releaseMenu,
  when,
} from "./kit";

const TOKENS = {
  "--background": "#07080a",
  "--foreground": "#f4f4f6",
  "--card": "#0d0d0d",
  "--popover": "#121212",
  "--muted": "#101111",
  "--muted-foreground": "#9c9c9d",
  "--border": "#242728",
  "--primary": "#ffffff",
  "--primary-foreground": "#000000",
  "--destructive": "#ff6161",
  "--warning": "#ffc533",
  "--success": "#59d499",
  "--ring": "rgba(255,255,255,0.16)",
} as const;

const FONT = 'var(--font-inter), "PingFang SC", -apple-system, sans-serif';

function Key({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="inline-flex h-5 min-w-5 items-center justify-center rounded-[4px] bg-gradient-to-b from-[#121212] to-[#0d0d0d] px-1.5 font-sans text-[11px] text-[#cdcdcd] ring-1 ring-[#242728]">
      {children}
    </kbd>
  );
}

function Cta({ children, onClick, disabled }: { children: React.ReactNode; onClick: () => void; disabled?: boolean }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn("inline-flex h-9 items-center gap-2 rounded-lg bg-white px-4 text-sm font-medium text-black hover:bg-[#e8e8e8] cursor-pointer disabled:opacity-40", PRESS)}
    >
      {children}
    </button>
  );
}

interface Item {
  id: string;
  group: string;
  label: string;
  sub?: string;
  icon: React.ReactNode;
  screen: Screen;
  keywords?: string;
  badge?: number;
  enter?: () => void;
  detail: () => React.ReactNode;
}

export function VariantD1({ state, screen, setScreen }: VariantProps) {
  return (
    <div
      style={{ ...TOKENS, fontFamily: FONT, fontFeatureSettings: '"calt", "kern", "liga", "ss03"' } as React.CSSProperties}
      className="relative min-h-screen overflow-hidden bg-background text-foreground antialiased"
    >
      <div className="pointer-events-none absolute inset-x-0 top-0 h-[420px] overflow-hidden [mask-image:linear-gradient(to_bottom,black,transparent)]">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            className="absolute -top-48 h-[640px] w-44 -skew-x-[28deg] blur-3xl"
            style={{ left: `${34 + i * 13}%`, background: "linear-gradient(180deg,#ff5757,#a1131a)", opacity: 0.32 - i * 0.08 }}
          />
        ))}
      </div>
      <UiHost>
        {state.me.status === "active" ? <Palette state={state} screen={screen} setScreen={setScreen} /> : <AccountGate state={state} />}
      </UiHost>
    </div>
  );
}

const PLATFORM_ICON = (p: string) => (p.startsWith("linux") ? <Server className="h-3.5 w-3.5" /> : <AppWindow className="h-3.5 w-3.5" />);

function Tile({ children, tone }: { children: React.ReactNode; tone?: string }) {
  return (
    <span className={cn("flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-[#121212] ring-1 ring-[#242728]", tone ?? "text-[#cdcdcd]")}>
      {children}
    </span>
  );
}

function Palette({ state, screen, setScreen }: VariantProps) {
  const { toast, ask } = useContext(Ui);
  const flow = useReportFlow(state);
  const [query, setQuery] = useState("");
  const [local, setLocal] = useState<{ screen: Screen; id: string }>({ screen, id: "" });
  const [drag, setDrag] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const { me } = state;
  const latest = state.latest;
  const mine = state.allTasks.filter((t) => t.submitterId === me.id);

  function downloadPkg(r: Release, p: Release["packages"][number]) {
    state.download(r.version, p.platform);
    toast(`正在下载 ${p.fileName}`);
  }
  function downloadReport(t: ReportTask) {
    toast(`正在下载 reports-${t.id}.zip`);
  }

  const items: Item[] = [
    {
      id: "gen",
      group: "报告",
      label: "生成报告",
      sub: flow.doneId ? "已生成，按 ↵ 下载" : flow.files.length ? `${flow.files.length} 个采集包待生成` : "拖入 ZIP 到任意位置",
      icon: <Tile tone="text-white"><FileUp className="h-3.5 w-3.5" /></Tile>,
      screen: "new-report",
      keywords: "生成 上传 zip report",
      enter: () => {
        if (flow.doneId) toast(`正在下载 reports-${flow.doneId}.zip`);
        else if (flow.files.length && !flow.running) flow.start();
      },
      detail: () => <GenDetail flow={flow} />,
    },
    ...mine.slice(0, 3).map<Item>((t) => ({
      id: t.id,
      group: "报告",
      label: `${t.items[0].name}${t.items.length > 1 ? ` 等 ${t.items.length} 份` : ""}`,
      sub: `${when(t.createdAt)}${t.filesExpired ? " · 已过期" : ""}`,
      icon: <Tile><FileArchive className="h-3.5 w-3.5" /></Tile>,
      screen: "reports",
      keywords: `${t.id} 报告 下载`,
      enter: () => !t.filesExpired && t.status !== "processing" && downloadReport(t),
      detail: () => <ReportDetail state={state} task={t} onDownload={() => downloadReport(t)} />,
    })),
    {
      id: "mine",
      group: "报告",
      label: "我的全部报告",
      sub: `${mine.length} 个`,
      icon: <Tile><ClipboardList className="h-3.5 w-3.5" /></Tile>,
      screen: "reports",
      keywords: "我的报告 历史 重新下载",
      detail: () => (
        <div className="space-y-1">
          <p className="mb-3 text-xs text-[#6a6b6c]">报告保留 30 天</p>
          {mine.map((t) => (
            <div key={t.id} className="flex items-center gap-3 rounded-md px-2 py-2 hover:bg-[#121212]">
              <span className="min-w-0 flex-1 truncate text-sm">{t.items[0].name}{t.items.length > 1 && <span className="text-[#9c9c9d]"> 等 {t.items.length} 份</span>}</span>
              <span className="text-xs text-[#6a6b6c] tabular-nums">{when(t.createdAt)}</span>
              {t.filesExpired ? <span className="w-12 text-right text-xs text-[#6a6b6c]">已过期</span> : (
                <button type="button" onClick={() => downloadReport(t)} className="w-12 text-right text-xs text-white hover:underline cursor-pointer">下载</button>
              )}
            </div>
          ))}
        </div>
      ),
    },
    ...(latest
      ? latest.packages.map<Item>((p) => ({
          id: `pkg-${p.platform}`,
          group: `采集器 v${latest.version}`,
          label: `下载 ${p.osLabel} ${p.archLabel}`,
          sub: formatSize(p.size),
          icon: <Tile>{PLATFORM_ICON(p.platform)}</Tile>,
          screen: "collectors",
          keywords: `采集器 collector ${p.platform} ${p.osLabel} ${p.archLabel} x86 amd64 arm aarch64`,
          enter: () => downloadPkg(latest, p),
          detail: () => <PkgDetail state={state} release={latest} pkg={p} onDownload={() => downloadPkg(latest, p)} />,
        }))
      : []),
    {
      id: "usage",
      group: latest ? `采集器 v${latest.version}` : "采集器",
      label: "怎么使用采集器",
      icon: <Tile><BookOpen className="h-3.5 w-3.5" /></Tile>,
      screen: "collectors",
      keywords: "使用 命令 帮助 help",
      detail: () => <Usage />,
    },
    {
      id: "history",
      group: latest ? `采集器 v${latest.version}` : "采集器",
      label: latest ? "历史版本" : "暂无推荐版本 · 历史版本",
      icon: <Tile tone={latest ? undefined : "text-[#ffc533]"}><History className="h-3.5 w-3.5" /></Tile>,
      screen: "collectors",
      keywords: "历史 旧版本 弃用 撤回 预发布",
      detail: () => <HistoryDetail state={state} onDownload={downloadPkg} ask={ask} />,
    },
    ...(me.role === "admin"
      ? [
          { id: "adm-users", label: "审批与用户", icon: <Users className="h-3.5 w-3.5" />, badge: state.pendingCount, detail: () => <UsersAdmin state={state} /> },
          { id: "adm-dl", label: "下载记录", icon: <Download className="h-3.5 w-3.5" />, detail: () => <DownloadsAdmin state={state} /> },
          { id: "adm-rep", label: "全部报告", icon: <ClipboardList className="h-3.5 w-3.5" />, detail: () => <ReportsAdmin state={state} /> },
        ].map<Item>((x) => ({ ...x, group: "管理", screen: "users", keywords: "管理 admin 用户 审批", icon: <Tile>{x.icon}</Tile> }))
      : []),
  ];

  const q = query.trim().toLowerCase();
  const shown = q ? items.filter((it) => `${it.label} ${it.keywords ?? ""} ${it.group}`.toLowerCase().includes(q)) : items;
  const preferred = local.screen === screen && local.id ? local.id : items.find((it) => it.screen === screen)?.id;
  const current = shown.find((it) => it.id === preferred) ?? shown[0];

  function select(it: Item) {
    setLocal({ screen: it.screen, id: it.id });
    if (it.screen !== screen) setScreen(it.screen);
  }

  function onKey(e: React.KeyboardEvent) {
    if (!shown.length) return;
    const i = Math.max(0, shown.indexOf(current!));
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      select(shown[(i + (e.key === "ArrowDown" ? 1 : -1) + shown.length) % shown.length]);
    } else if (e.key === "Enter") {
      e.preventDefault();
      current?.enter?.();
    } else if (e.key === "Escape") {
      setQuery("");
    }
  }

  const groups = [...new Set(shown.map((it) => it.group))];

  return (
    <div
      className="relative flex min-h-screen flex-col items-center px-6 pt-[9vh] pb-32"
      onDragOver={(e) => {
        e.preventDefault();
        setDrag(true);
      }}
      onDragLeave={(e) => e.currentTarget === e.target && setDrag(false)}
      onDrop={(e) => {
        e.preventDefault();
        setDrag(false);
        flow.add([...e.dataTransfer.files].map((f) => ({ name: f.name, size: f.size })));
        select(items[0]);
      }}
    >
      <div className="mb-6 flex w-full max-w-[920px] items-center justify-between text-sm">
        <span className="font-semibold tracking-[0.2px]">DB-Check</span>
        <span className="flex items-center gap-2 text-[#9c9c9d]">
          {me.displayName}
          <span className="rounded-[4px] bg-[#101111] px-1.5 py-0.5 text-[11px] text-white/70">{me.role === "admin" ? "管理员" : "工程师"}</span>
        </span>
      </div>

      <div
        className={cn(
          "flex h-[600px] w-full max-w-[920px] flex-col overflow-hidden rounded-2xl bg-[#0d0d0d]/95 ring-1 backdrop-blur-xl transition-[box-shadow] duration-200",
          drag ? "ring-white/60" : "ring-[#242728]",
        )}
      >
        <div className="flex h-14 shrink-0 items-center gap-3 border-b border-[#242728] px-5">
          <Search className="h-4 w-4 text-[#6a6b6c]" />
          <input
            ref={inputRef}
            autoFocus
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKey}
            placeholder="搜索命令，例如 linux arm、报告、审批…"
            className="flex-1 bg-transparent text-[15px] text-white outline-none placeholder:text-[#6a6b6c]"
          />
          {drag && <span className="text-xs text-white">松手添加采集包</span>}
        </div>

        <div className="flex min-h-0 flex-1">
          <div className="w-[320px] shrink-0 overflow-y-auto border-r border-[#242728] p-2">
            {groups.map((g) => (
              <div key={g} className="mb-2">
                <p className="px-2.5 pt-2 pb-1 text-[12px] tracking-[0.4px] text-[#6a6b6c]">{g}</p>
                {shown
                  .filter((it) => it.group === g)
                  .map((it) => (
                    <button
                      key={it.id}
                      type="button"
                      onClick={() => {
                        select(it);
                        inputRef.current?.focus();
                      }}
                      onDoubleClick={() => it.enter?.()}
                      className={cn(
                        "flex w-full items-center gap-3 rounded-md px-2.5 py-2 text-left cursor-pointer",
                        current?.id === it.id ? "bg-[#1b1c1e]" : "hover:bg-[#121212]",
                      )}
                    >
                      {it.icon}
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-sm text-[#f4f4f6]">{it.label}</span>
                      </span>
                      {it.badge ? (
                        <span className="rounded-full bg-white px-1.5 text-[10px] font-semibold text-black tabular-nums">{it.badge}</span>
                      ) : (
                        it.sub && <span className="max-w-24 truncate text-[12px] text-[#6a6b6c] tabular-nums">{it.sub}</span>
                      )}
                    </button>
                  ))}
              </div>
            ))}
            {!shown.length && <p className="px-3 py-6 text-sm text-[#6a6b6c]">没有匹配的命令</p>}
          </div>

          <div className="min-w-0 flex-1 overflow-y-auto p-6">
            {current && (
              <div key={current.id} className={cn("h-full transition-[opacity,transform] duration-200 starting:translate-y-1 starting:opacity-0", EASE_OUT)}>
                {current.detail()}
              </div>
            )}
          </div>
        </div>

        <div className="flex h-11 shrink-0 items-center gap-4 border-t border-[#242728] bg-[#0a0b0c] px-4 text-[12px] text-[#9c9c9d]">
          <span className="flex items-center gap-2 text-[#cdcdcd]">
            <span className="h-2 w-2 rounded-full bg-[#59d499]" />
            {latest ? `采集器 v${latest.version} 可用` : "暂无推荐采集器版本"}
          </span>
          <span className="ml-auto flex items-center gap-1.5">{current?.enter ? "执行" : "查看"} <Key>↵</Key></span>
          <span className="flex items-center gap-1.5">选择 <Key>↑</Key><Key>↓</Key></span>
          <span className="flex items-center gap-1.5">清空 <Key>Esc</Key></span>
        </div>
      </div>
    </div>
  );
}

function GenDetail({ flow }: { flow: ReportFlow }) {
  const { toast } = useContext(Ui);
  if (flow.doneId) {
    return (
      <div className="flex h-full flex-col items-start pt-6">
        <span className={cn("flex h-10 w-10 items-center justify-center rounded-full bg-[#59d499]/15 text-[#59d499] transition-[opacity,transform] duration-300 starting:scale-90 starting:opacity-0", EASE_OUT)}>
          <Check className="h-5 w-5" strokeWidth={2.5} />
        </span>
        <h2 className="mt-5 text-2xl font-medium tracking-[0.2px]">报告已生成</h2>
        <p className="mt-1 text-sm text-[#9c9c9d]">
          {flow.okCount} 份成功{flow.failedCount > 0 && `，${flow.failedCount} 份失败`} · 用时 {flow.elapsed}s · 已存入我的报告
        </p>
        <div className="mt-8 flex items-center gap-3">
          <Cta onClick={() => toast(`正在下载 reports-${flow.doneId}.zip`)} disabled={flow.okCount === 0}>
            <Download className="h-4 w-4" />下载报告 <Key>↵</Key>
          </Cta>
          <button type="button" onClick={flow.reset} className="text-sm text-[#9c9c9d] hover:text-white cursor-pointer">再来一份</button>
        </div>
      </div>
    );
  }

  if (!flow.files.length) {
    return (
      <div className="flex h-full flex-col items-center justify-center rounded-xl border border-dashed border-[#242728] text-center">
        <FileUp className="h-6 w-6 text-[#6a6b6c]" />
        <p className="mt-4 text-[15px]">把采集器生成的 ZIP 拖进窗口</p>
        <p className="mt-1 text-sm text-[#6a6b6c]">数据库类型会自动识别，可一次放多台主机</p>
        <button type="button" onClick={() => flow.add(DEMO_FILES)} className="mt-6 text-xs text-[#9c9c9d] underline-offset-4 hover:underline cursor-pointer">
          原型：填入示例文件
        </button>
      </div>
    );
  }

  return (
    <div>
      <p className="mb-3 text-[12px] tracking-[0.4px] text-[#6a6b6c]">{flow.running ? "正在生成" : "待生成"}</p>
      <div className="space-y-1">
        {flow.files.map((f, i) => (
          <FileRow key={f.name} flow={flow} i={i} />
        ))}
      </div>
      {!flow.running && (
        <div className="mt-6 flex items-center gap-3">
          <Cta onClick={flow.start}>
            生成 {flow.files.length} 份报告 <Key>↵</Key>
          </Cta>
          <span className="text-xs text-[#6a6b6c]">继续拖入可追加</span>
        </div>
      )}
    </div>
  );
}

function FileRow({ flow, i }: { flow: ReportFlow; i: number }) {
  const f = flow.files[i];
  const p = flow.progress?.[i];
  const notice = flow.notice(f.version);
  return (
    <div className="relative overflow-hidden rounded-md bg-[#101111] px-3 py-2.5">
      {p !== undefined && (
        <span className="absolute inset-0 origin-left bg-white/[0.06] transition-transform duration-150 ease-linear" style={{ transform: `scaleX(${p / 100})` }} />
      )}
      <div className="relative flex items-center gap-3 text-sm">
        <FileArchive className="h-4 w-4 text-[#6a6b6c]" />
        <span className="min-w-0 flex-1 truncate">{f.name}</span>
        <span className="text-xs text-[#9c9c9d] tabular-nums">{DB_LABEL[f.db]} · v{f.version}</span>
        {p !== undefined ? (
          <span className="w-10 text-right text-xs text-[#cdcdcd] tabular-nums">{p >= 100 ? <Check className="ml-auto h-3.5 w-3.5 text-[#59d499]" /> : stageOf(p)}</span>
        ) : (
          <>
            {f.db !== "mysql" && !f.aux && (
              <button type="button" onClick={() => flow.attach(f.name)} className="text-xs text-[#9c9c9d] hover:text-white cursor-pointer">
                + {f.db === "oracle" ? "AWR" : "WDR"}
              </button>
            )}
            {f.aux && <span className="text-xs text-[#9c9c9d]">已附 {f.aux}</span>}
            <button type="button" aria-label="移除" onClick={() => flow.remove(f.name)} className="text-[#6a6b6c] hover:text-white cursor-pointer">
              <X className="h-3.5 w-3.5" />
            </button>
          </>
        )}
      </div>
      {notice && <p className={cn("relative mt-1 pl-7 text-xs", notice.tone === "danger" ? "text-[#ff6161]" : "text-[#ffc533]")}>{notice.text}</p>}
    </div>
  );
}

function PkgDetail({
  state,
  release,
  pkg,
  onDownload,
}: {
  state: ConsoleState;
  release: Release;
  pkg: Release["packages"][number];
  onDownload: () => void;
}) {
  const { ask } = useContext(Ui);
  return (
    <div>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-[#9c9c9d]">{pkg.osLabel}</p>
          <p className="text-[40px] leading-tight font-semibold tracking-tight">{pkg.archLabel}</p>
        </div>
        <Menu items={releaseMenu(state, ask, release)} />
      </div>
      <dl className="mt-6 grid grid-cols-[80px_1fr] gap-y-2.5 text-sm">
        <dt className="text-[#6a6b6c]">版本</dt>
        <dd>v{release.version} <span className="text-[#6a6b6c]">· {when(release.publishedAt).split(" ")[0]}</span></dd>
        <dt className="text-[#6a6b6c]">文件</dt>
        <dd className="truncate font-[family-name:var(--font-jetbrains)] text-[13px]">{pkg.fileName}</dd>
        <dt className="text-[#6a6b6c]">大小</dt>
        <dd className="tabular-nums">{formatSize(pkg.size)}</dd>
        <dt className="text-[#6a6b6c]">SHA256</dt>
        <dd><CopyText text={pkg.sha256} label={`${pkg.sha256.slice(0, 24)}…`} /></dd>
      </dl>
      <div className="mt-7">
        <Cta onClick={onDownload}>
          <Download className="h-4 w-4" />下载 <Key>↵</Key>
        </Cta>
      </div>
      <p className="mt-8 text-xs text-[#6a6b6c]">四个平台的发布包内容相同，按客户主机的系统与架构选择。</p>
    </div>
  );
}

function Usage() {
  const [db, setDb] = useState<DbType>("oracle");
  return (
    <div className="space-y-5 text-sm">
      <p className="text-[#cdcdcd]">1　把采集器上传到客户数据库主机并解压</p>
      <div>
        <p className="text-[#cdcdcd]">2　运行采集命令</p>
        <div className="mt-3 rounded-lg bg-[#101111] ring-1 ring-[#242728]">
          <div className="flex gap-1 border-b border-[#242728] px-2 py-1.5">
            {(Object.keys(USAGE) as DbType[]).map((k) => (
              <button
                key={k}
                type="button"
                onClick={() => setDb(k)}
                className={cn("rounded-full px-2.5 py-0.5 text-xs cursor-pointer", db === k ? "bg-[#1b1c1e] text-white" : "text-[#9c9c9d]")}
              >
                {DB_LABEL[k]}
              </button>
            ))}
          </div>
          <div className="flex items-start gap-3 p-3">
            <code className="flex-1 font-[family-name:var(--font-jetbrains)] text-xs leading-relaxed break-all text-[#cdcdcd]">{USAGE[db]}</code>
            <CopyText text={USAGE[db]} label="" />
          </div>
        </div>
      </div>
      <p className="text-[#cdcdcd]">3　把生成的 ZIP 拖回这个窗口</p>
    </div>
  );
}

function HistoryDetail({
  state,
  onDownload,
  ask,
}: {
  state: ConsoleState;
  onDownload: (r: Release, p: Release["packages"][number]) => void;
  ask: Parameters<typeof releaseMenu>[1];
}) {
  const isAdmin = state.me.role === "admin";
  const list = state.releases.filter((r) => r.status !== "latest" && (isAdmin || r.status === "deprecated"));
  return (
    <div className="space-y-4">
      {!state.latest && (
        <p className="rounded-md bg-[#ffc533]/10 px-3 py-2 text-sm text-[#ffc533]">
          当前没有推荐版本。{isAdmin ? "在下方版本的 ··· 中「设为最新」。" : "请联系管理员，或暂用下方版本。"}
        </p>
      )}
      {list.map((r) => (
        <div key={r.version} className="rounded-lg bg-[#101111] p-4 ring-1 ring-[#242728]">
          <div className="flex items-center gap-3">
            <span className="font-medium tabular-nums">v{r.version}</span>
            <span className={cn("text-xs", r.status === "revoked" ? "text-[#ff6161]" : r.status === "deprecated" ? "text-[#ffc533]" : "text-[#57c1ff]")}>
              {r.status === "revoked" ? "已撤回" : r.status === "deprecated" ? "已弃用" : "预发布"}
            </span>
            <span className="ml-auto"><Menu items={releaseMenu(state, ask, r)} /></span>
          </div>
          {r.revokeReason && <p className="mt-1 text-xs text-[#ff6161]">{r.revokeReason}</p>}
          <div className="mt-3 flex flex-wrap gap-1.5">
            {r.packages.map((p) => (
              <button
                key={p.platform}
                type="button"
                onClick={() => onDownload(r, p)}
                className="rounded-md px-2 py-1 text-xs text-[#cdcdcd] ring-1 ring-white/16 hover:bg-[#121212] cursor-pointer"
              >
                {p.osLabel} {p.archLabel}
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ReportDetail({ state, task, onDownload }: { state: ConsoleState; task: ReportTask; onDownload: () => void }) {
  return (
    <div>
      <p className="text-sm text-[#9c9c9d] tabular-nums">{when(task.createdAt)}</p>
      <h2 className="mt-1 font-[family-name:var(--font-jetbrains)] text-lg">{task.id}</h2>
      <div className="mt-5 space-y-1">
        {task.items.map((i) => {
          const n = state.versionNotice(i.collectorVersion);
          return (
            <div key={i.name} className="rounded-md bg-[#101111] px-3 py-2 text-sm">
              <div className="flex items-center gap-3">
                <span className="flex-1 truncate">{i.name}</span>
                <span className="text-xs text-[#9c9c9d]">{DB_LABEL[i.dbType]} · v{i.collectorVersion}</span>
                {i.outcome === "failed" && <span className="text-xs text-[#ff6161]">失败</span>}
              </div>
              {n?.tone === "danger" && <p className="mt-1 text-xs text-[#ff6161]">{n.text}</p>}
            </div>
          );
        })}
      </div>
      <div className="mt-7">
        {task.filesExpired ? (
          <p className="text-sm text-[#6a6b6c]">文件已超过 30 天，已清理</p>
        ) : task.status === "processing" ? (
          <p className="text-sm text-[#9c9c9d]">生成中…</p>
        ) : (
          <Cta onClick={onDownload}>
            <Download className="h-4 w-4" />重新下载 <Key>↵</Key>
          </Cta>
        )}
      </div>
    </div>
  );
}
