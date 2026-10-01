// PROTOTYPE — throwaway. Variant B: top nav (like the current shell), the
// latest release as a hero download card, admin actions inline on the page.
"use client";

import { useState } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ClipboardList,
  Clock,
  Database,
  Download,
  FileBarChart,
  Hourglass,
  Package,
  ShieldAlert,
  User as UserIcon,
  Users,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { VariantProps } from "./console-prototype";
import {
  RELEASE_ACTION_LABEL,
  SCREEN_LABEL,
  detectPlatform,
  formatSize,
  formatTime,
  releaseActionsFor,
  screensFor,
  type ConsoleState,
  type Release,
  type Screen,
  type User,
} from "./console-state";
import {
  AccountPill,
  CopySha,
  NewReportPlaceholder,
  QUICKSTART,
  ReleasePill,
  TaskPill,
  VersionNotice,
} from "./shared";

const ICONS: Record<Screen, React.ReactNode> = {
  "new-report": <FileBarChart className="h-4 w-4" />,
  reports: <ClipboardList className="h-4 w-4" />,
  collectors: <Package className="h-4 w-4" />,
  users: <Users className="h-4 w-4" />,
  downloads: <Download className="h-4 w-4" />,
};

const chip = "inline-flex items-center gap-1 rounded-lg px-3 py-1.5 text-xs font-medium cursor-pointer transition-colors";

export function VariantB({ state, screen, setScreen }: VariantProps) {
  const { me } = state;

  return (
    <>
      <nav className="fixed top-0 left-0 right-0 z-50 border-b border-border bg-card/80 backdrop-blur-md">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
          <div className="flex items-center gap-2.5">
            <Database className="h-5 w-5 text-primary" />
            <span className="text-sm font-bold tracking-tight">DB-Check</span>
          </div>
          {me.status === "active" && (
            <div className="flex items-center gap-1">
              {screensFor(me.role).map((s) => (
                <button
                  key={s}
                  type="button"
                  onClick={() => setScreen(s)}
                  className={cn(
                    "relative flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium cursor-pointer",
                    screen === s ? "text-primary" : "text-muted-foreground hover:text-foreground hover:bg-muted/50",
                  )}
                >
                  {ICONS[s]}
                  {SCREEN_LABEL[s]}
                  {s === "users" && state.pendingCount > 0 && (
                    <span className="absolute -top-0.5 right-0 flex h-4 min-w-4 items-center justify-center rounded-full bg-warning px-1 text-[10px] font-bold text-background">
                      {state.pendingCount}
                    </span>
                  )}
                  {screen === s && <span className="absolute -bottom-[calc(0.5rem+1px)] left-2 right-2 h-0.5 rounded-full bg-primary" />}
                </button>
              ))}
            </div>
          )}
          <div className="flex items-center gap-3">
            <span className="rounded-md bg-warning/10 px-2 py-0.5 text-[10px] font-semibold text-warning">Mock</span>
            <span className="flex items-center gap-2 text-sm">
              <UserIcon className="h-4 w-4 text-muted-foreground" />
              {me.displayName}
            </span>
          </div>
        </div>
      </nav>

      <main className="mx-auto max-w-4xl px-4 pt-20 pb-28">
        {me.status !== "active" ? (
          <AccountHero state={state} />
        ) : (
          <>
            {screen === "new-report" && <NewReportPlaceholder />}
            {screen === "collectors" && <Collectors state={state} />}
            {screen === "users" && <UsersPage state={state} />}
            {screen === "reports" && <Tasks state={state} />}
            {screen === "downloads" && <Downloads state={state} />}
          </>
        )}
      </main>
    </>
  );
}

function AdminStrip({ state, release }: { state: ConsoleState; release: Release }) {
  if (state.me.role !== "admin") return null;
  const n = state.downloads.filter((d) => d.version === release.version).length;
  return (
    <div className="flex flex-wrap items-center gap-2 border-t border-dashed border-border pt-3 text-xs">
      <ShieldAlert className="h-3.5 w-3.5 text-accent" />
      <span className="text-muted-foreground">管理：</span>
      {releaseActionsFor(release.status).map((a) => (
        <button
          key={a}
          type="button"
          onClick={() => state.releaseActions[a](release.version)}
          className={cn(chip, "border border-border px-2 py-1", a === "revoke" ? "text-destructive hover:bg-destructive/10" : "hover:bg-muted")}
        >
          {RELEASE_ACTION_LABEL[a]}
        </button>
      ))}
      <span className="ml-auto text-muted-foreground">已下载 {n} 次</span>
    </div>
  );
}

function Collectors({ state }: { state: ConsoleState }) {
  const isAdmin = state.me.role === "admin";
  const platform = detectPlatform();
  const latest = state.latest;
  const preReleases = state.releases.filter((r) => r.status === "pre-release");
  const others = state.releases.filter((r) => r.status === "deprecated" || (isAdmin && r.status === "revoked"));
  const mine = latest?.packages.find((p) => p.platform === platform);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">采集器</h1>
        <p className="text-sm text-muted-foreground">下载最新采集器，在客户数据库主机上运行生成诊断 ZIP</p>
      </div>

      {latest ? (
        <section className="rounded-2xl border border-primary/30 bg-gradient-to-br from-primary/10 to-card p-6 space-y-5 ring-1 ring-primary/10">
          <div className="flex items-start justify-between gap-6">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <span className="text-3xl font-bold tracking-tight">v{latest.version}</span>
                <ReleasePill status="latest" />
              </div>
              <p className="text-xs text-muted-foreground">
                {formatTime(latest.publishedAt)} 发布 · {latest.tag}@{latest.commit} · 支持 {latest.dbTypes.join(" / ")}
              </p>
            </div>
            {mine && (
              <button
                type="button"
                onClick={() => state.download(latest.version, mine.platform)}
                className="flex flex-col items-center rounded-xl bg-primary px-6 py-3 text-primary-foreground hover:bg-primary/90 cursor-pointer"
              >
                <span className="flex items-center gap-2 text-base font-semibold"><Download className="h-5 w-5" />下载 {mine.osLabel} {mine.archLabel}</span>
                <span className="text-[11px] opacity-80">{mine.fileName} · {formatSize(mine.size)}</span>
              </button>
            )}
          </div>
          {mine && <p className="text-xs">SHA256 <CopySha sha={mine.sha256} short={false} /></p>}
          <div className="flex flex-wrap gap-2">
            <span className="text-xs text-muted-foreground self-center">其他平台：</span>
            {latest.packages.filter((p) => p !== mine).map((p) => (
              <button key={p.platform} type="button" onClick={() => state.download(latest.version, p.platform)} title={p.sha256} className={cn(chip, "border border-border bg-card hover:bg-muted")}>
                <Download className="h-3 w-3" />
                {p.osLabel} {p.archLabel} · {formatSize(p.size)}
              </button>
            ))}
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="mb-1 text-xs font-semibold">更新内容</p>
              <pre className="whitespace-pre-wrap text-xs text-muted-foreground">{latest.notes}</pre>
            </div>
            <div>
              <p className="mb-1 text-xs font-semibold">快速上手</p>
              <ol className="list-decimal space-y-0.5 pl-4 text-xs text-muted-foreground">
                {QUICKSTART.map((s) => <li key={s}>{s}</li>)}
              </ol>
            </div>
          </div>
          <AdminStrip state={state} release={latest} />
        </section>
      ) : (
        <section className="rounded-2xl border border-dashed border-warning/40 bg-warning/5 p-8 text-center">
          <AlertTriangle className="mx-auto h-8 w-8 text-warning" />
          <p className="mt-2 font-semibold">当前没有推荐版本</p>
          <p className="text-sm text-muted-foreground">{isAdmin ? "请在下方选择一个版本「设为最新」。" : "请联系管理员，或暂用下方历史版本。"}</p>
        </section>
      )}

      {isAdmin && preReleases.length > 0 && (
        <section className="space-y-3">
          <h2 className="text-sm font-semibold">预发布（仅管理员可见）</h2>
          {preReleases.map((r) => <ReleaseRow key={r.version} state={state} release={r} />)}
        </section>
      )}

      <Older state={state} releases={others} />
    </div>
  );
}

function Older({ state, releases }: { state: ConsoleState; releases: Release[] }) {
  const [open, setOpen] = useState(false);
  if (releases.length === 0) return null;
  return (
    <section className="space-y-3">
      <button type="button" onClick={() => setOpen(!open)} className="flex items-center gap-1 text-sm font-semibold cursor-pointer">
        <ChevronDown className={cn("h-4 w-4 transition-transform", !open && "-rotate-90")} />
        历史版本（{releases.length}）
      </button>
      {open && releases.map((r) => <ReleaseRow key={r.version} state={state} release={r} />)}
    </section>
  );
}

function ReleaseRow({ state, release: r }: { state: ConsoleState; release: Release }) {
  return (
    <div className={cn("rounded-xl border bg-card p-4 space-y-3", r.status === "revoked" ? "border-destructive/30" : "border-border")}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold">v{r.version}</span>
        <ReleasePill status={r.status} />
        <span className="text-xs text-muted-foreground">{formatTime(r.publishedAt)}</span>
        {r.status === "deprecated" && <span className="text-xs text-warning">已弃用，仅在必要时使用</span>}
        {r.revokeReason && <span className="text-xs text-destructive">撤回原因：{r.revokeReason}</span>}
      </div>
      <div className="flex flex-wrap gap-2">
        {r.packages.map((p) => (
          <button key={p.platform} type="button" onClick={() => state.download(r.version, p.platform)} title={p.sha256} className={cn(chip, "border border-border hover:bg-muted")}>
            <Download className="h-3 w-3" />
            {p.osLabel} {p.archLabel}
          </button>
        ))}
      </div>
      <AdminStrip state={state} release={r} />
    </div>
  );
}

function UsersPage({ state }: { state: ConsoleState }) {
  const pending = state.users.filter((u) => u.status === "pending");
  const rest = state.users.filter((u) => u.status !== "pending");
  const a = state.userActions;

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">用户管理</h1>
        <p className="text-sm text-muted-foreground">审批注册申请，管理账号状态与角色</p>
      </div>

      <section className="space-y-3">
        <h2 className="flex items-center gap-2 text-sm font-semibold">
          <Hourglass className="h-4 w-4 text-warning" />
          待审批（{pending.length}）
        </h2>
        {pending.length === 0 && <p className="text-sm text-muted-foreground">没有待审批的申请 🎉</p>}
        {pending.map((u) => (
          <div key={u.id} className="flex items-center gap-4 rounded-xl border border-warning/30 bg-warning/5 p-4">
            <div className="flex-1 space-y-1">
              <p className="font-semibold">{u.displayName} <span className="font-mono text-xs text-muted-foreground">@{u.username}</span></p>
              <p className="text-xs text-muted-foreground">{u.team} · {u.email} · {formatTime(u.registeredAt)} 申请</p>
              <p className="text-sm">“{u.note || "（无申请说明）"}”</p>
            </div>
            <button type="button" onClick={() => a.approve(u.id)} className={cn(chip, "bg-primary text-primary-foreground hover:bg-primary/90")}>
              <CheckCircle2 className="h-4 w-4" />批准
            </button>
            <button type="button" onClick={() => a.reject(u.id)} className={cn(chip, "border border-destructive/40 text-destructive hover:bg-destructive/10")}>
              拒绝
            </button>
          </div>
        ))}
      </section>

      <section className="space-y-2">
        <h2 className="text-sm font-semibold">全部用户</h2>
        {rest.map((u) => <UserRow key={u.id} state={state} user={u} />)}
      </section>
    </div>
  );
}

function UserRow({ state, user: u }: { state: ConsoleState; user: User }) {
  const [open, setOpen] = useState(false);
  const a = state.userActions;
  return (
    <div className="rounded-xl border border-border bg-card">
      <button type="button" onClick={() => setOpen(!open)} className="flex w-full items-center gap-3 px-4 py-3 text-left cursor-pointer">
        <span className="font-medium">{u.displayName}</span>
        <span className="font-mono text-xs text-muted-foreground">@{u.username}</span>
        {u.role === "admin" && <span className="text-[10px] font-semibold text-accent">管理员</span>}
        {u.id === state.me.id && <span className="text-[10px] text-primary">（我）</span>}
        <span className="text-xs text-muted-foreground">{u.team}</span>
        <span className="ml-auto"><AccountPill status={u.status} /></span>
        <ChevronDown className={cn("h-4 w-4 text-muted-foreground transition-transform", !open && "-rotate-90")} />
      </button>
      {open && (
        <div className="space-y-3 border-t border-border px-4 py-3 text-xs">
          <p className="text-muted-foreground">{u.email} · 注册于 {formatTime(u.registeredAt)}</p>
          {u.reason && <p className="text-destructive">原因：{u.reason}</p>}
          {u.lastAction && <p className="text-muted-foreground">最近操作：{u.lastAction.action}（{u.lastAction.by}，{formatTime(u.lastAction.at)}）</p>}
          <div className="flex flex-wrap gap-2">
            {u.status === "active" && (
              <>
                <button type="button" onClick={() => a.toggleRole(u.id)} className={cn(chip, "border border-border hover:bg-muted")}>{u.role === "admin" ? "降为普通用户" : "升为管理员"}</button>
                <button type="button" onClick={() => a.resetPassword(u.id)} className={cn(chip, "border border-border hover:bg-muted")}>重置密码</button>
                <button type="button" onClick={() => a.disable(u.id)} className={cn(chip, "border border-destructive/40 text-destructive hover:bg-destructive/10")}>禁用</button>
              </>
            )}
            {u.status === "disabled" && <button type="button" onClick={() => a.enable(u.id)} className={cn(chip, "border border-border hover:bg-muted")}>启用</button>}
            {u.status === "rejected" && <span className="text-muted-foreground">等待申请人重新提交</span>}
          </div>
        </div>
      )}
    </div>
  );
}

function Tasks({ state }: { state: ConsoleState }) {
  const isAdmin = state.me.role === "admin";
  const [status, setStatus] = useState("all");
  const [submitter, setSubmitter] = useState("all");
  const list = state.tasks.filter((t) => (status === "all" || t.status === status) && (submitter === "all" || t.submitterId === submitter));
  const statuses = [["all", "全部"], ["success", "已完成"], ["partial", "部分成功"], ["failed", "失败"], ["processing", "处理中"]];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">报告任务</h1>
        <p className="text-sm text-muted-foreground">{isAdmin ? "全部提交人的报告任务" : "我提交的报告任务"}；上传文件与报告保留 30 天</p>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {statuses.map(([k, label]) => (
          <button key={k} type="button" onClick={() => setStatus(k)} className={cn(chip, status === k ? "bg-primary/15 text-primary" : "text-muted-foreground hover:bg-muted")}>{label}</button>
        ))}
        {isAdmin && (
          <select value={submitter} onChange={(e) => setSubmitter(e.target.value)} className="ml-auto rounded-lg border border-border bg-card px-2 py-1.5 text-xs">
            <option value="all">全部提交人</option>
            {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
          </select>
        )}
      </div>
      <div className="space-y-3">
        {list.map((t) => (
          <div key={t.id} className="rounded-xl border border-border bg-card p-4 space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-mono text-sm font-semibold">{t.id}</span>
              <TaskPill status={t.status} />
              <span className="text-xs text-muted-foreground">{formatTime(t.createdAt)}</span>
              {isAdmin && <span className="text-xs text-muted-foreground">· 提交人 {state.userById(t.submitterId)?.displayName}</span>}
              <span className="ml-auto">
                {t.filesExpired ? (
                  <span className="inline-flex items-center gap-1 text-xs text-muted-foreground"><Clock className="h-3 w-3" />文件已过期</span>
                ) : t.status !== "processing" && (
                  <button type="button" className={cn(chip, "border border-border hover:bg-muted")}><Download className="h-3.5 w-3.5 text-primary" />下载报告</button>
                )}
              </span>
            </div>
            <div className="space-y-1.5">
              {t.items.map((i) => {
                const notice = state.versionNotice(i.collectorVersion);
                return (
                  <div key={i.name} className={cn("rounded-lg px-3 py-2 text-xs", notice?.tone === "danger" ? "bg-destructive/10" : "bg-muted/40")}>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-mono">{i.name}</span>
                      <span className="text-muted-foreground">{i.dbType}</span>
                      <span className="text-muted-foreground">采集器 v{i.collectorVersion}</span>
                      {i.outcome === "failed" && <span className="text-destructive">生成失败</span>}
                    </div>
                    {notice && <div className="mt-1"><VersionNotice notice={notice} /></div>}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function Downloads({ state }: { state: ConsoleState }) {
  const byDay = new Map<string, typeof state.downloads>();
  for (const d of state.downloads) {
    const day = d.at.slice(0, 10);
    byDay.set(day, [...(byDay.get(day) ?? []), d]);
  }
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">下载记录</h1>
        <p className="text-sm text-muted-foreground">每次下载发布包都会留下记录</p>
      </div>
      {[...byDay.entries()].sort(([a], [b]) => b.localeCompare(a)).map(([day, ds]) => (
        <section key={day} className="space-y-1.5">
          <h2 className="text-xs font-semibold text-muted-foreground">{day}</h2>
          {ds.map((d) => (
            <div key={d.id} className="flex items-center gap-3 rounded-lg border border-border bg-card px-3 py-2 text-sm">
              <span className="w-12 text-xs text-muted-foreground">{d.at.slice(11, 16)}</span>
              <span className="font-medium">{state.userById(d.userId)?.displayName}</span>
              <span className="text-muted-foreground">下载了</span>
              <span className="font-mono">v{d.version}</span>
              <span className="text-xs text-muted-foreground">{d.platform}</span>
            </div>
          ))}
        </section>
      ))}
    </div>
  );
}

function AccountHero({ state }: { state: ConsoleState }) {
  const { me } = state;
  const input = "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm";
  return (
    <div className="space-y-6 pt-6">
      <div className={cn("rounded-2xl p-8", me.status === "pending" ? "bg-warning/10" : "bg-destructive/10")}>
        {me.status === "pending" ? (
          <>
            <Hourglass className="h-10 w-10 text-warning" />
            <h1 className="mt-3 text-2xl font-bold">你好，{me.displayName}，申请正在审批中</h1>
            <p className="mt-1 text-sm text-muted-foreground">提交于 {formatTime(me.registeredAt)}。管理员批准后刷新页面即可使用报告生成与采集器下载。</p>
          </>
        ) : (
          <>
            <ShieldAlert className="h-10 w-10 text-destructive" />
            <h1 className="mt-3 text-2xl font-bold">申请未通过</h1>
            <p className="mt-1 text-sm">原因：{me.reason}</p>
          </>
        )}
      </div>
      {me.status === "rejected" && (
        <div className="grid gap-3 sm:grid-cols-2">
          <input className={input} value={me.username} disabled />
          <input className={input} value={me.email} disabled />
          <input className={input} defaultValue={me.displayName} placeholder="显示名称" />
          <input className={input} defaultValue={me.team} placeholder="团队" />
          <textarea className={cn(input, "sm:col-span-2")} defaultValue={me.note} rows={3} placeholder="申请说明" />
          <button type="button" onClick={() => state.userActions.resubmit(me.id)} className="sm:col-span-2 rounded-lg bg-primary py-2.5 text-sm font-semibold text-primary-foreground cursor-pointer">
            重新提交申请
          </button>
        </div>
      )}
    </div>
  );
}
