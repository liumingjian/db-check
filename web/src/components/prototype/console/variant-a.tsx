// PROTOTYPE — throwaway. Variant A: left sidebar admin console, dense tables.
"use client";

import { Fragment, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  ClipboardList,
  Clock,
  Database,
  Download,
  FileBarChart,
  LogOut,
  Package,
  Users,
  XCircle,
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
  type AccountStatus,
  type ConsoleState,
  type Screen,
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

const th = "px-3 py-2 text-left text-[11px] font-medium text-muted-foreground";
const td = "px-3 py-2 align-top";
const btn = "rounded-md border border-border px-2 py-0.5 text-[11px] hover:bg-muted cursor-pointer";

export function VariantA({ state, screen, setScreen }: VariantProps) {
  const { me } = state;
  if (me.status !== "active") return <AccountGate state={state} />;

  return (
    <div className="flex min-h-screen">
      <aside className="fixed inset-y-0 left-0 flex w-56 flex-col border-r border-border bg-card">
        <div className="flex h-14 items-center gap-2.5 border-b border-border px-4">
          <Database className="h-5 w-5 text-primary" />
          <span className="text-sm font-bold tracking-tight">DB-Check</span>
          <span className="ml-auto rounded-md bg-warning/10 px-2 py-0.5 text-[10px] font-semibold text-warning">Mock</span>
        </div>
        <nav className="flex-1 space-y-0.5 p-2">
          {screensFor(me.role).map((s, i) => (
            <Fragment key={s}>
              {s === "users" && <p className="px-3 pt-4 pb-1 text-[10px] font-semibold tracking-wider text-muted-foreground">管理</p>}
              {i === 0 && <p className="px-3 pt-2 pb-1 text-[10px] font-semibold tracking-wider text-muted-foreground">工作台</p>}
              <button
                type="button"
                onClick={() => setScreen(s)}
                className={cn(
                  "flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm cursor-pointer",
                  screen === s ? "bg-primary/10 text-primary font-medium" : "text-muted-foreground hover:bg-muted hover:text-foreground",
                )}
              >
                {ICONS[s]}
                {SCREEN_LABEL[s]}
                {s === "users" && state.pendingCount > 0 && (
                  <span className="ml-auto rounded-full bg-warning px-1.5 text-[10px] font-bold text-background">{state.pendingCount}</span>
                )}
              </button>
            </Fragment>
          ))}
        </nav>
        <div className="flex items-center gap-2 border-t border-border p-3 text-sm">
          <div className="min-w-0 flex-1">
            <p className="truncate font-medium">{me.displayName}</p>
            <p className="text-[11px] text-muted-foreground">{me.role === "admin" ? "管理员" : "普通用户"} · {me.team}</p>
          </div>
          <LogOut className="h-4 w-4 text-muted-foreground" />
        </div>
      </aside>

      <main className="ml-56 flex-1 px-8 pt-6 pb-28">
        <h1 className="mb-4 text-lg font-semibold">{SCREEN_LABEL[screen]}</h1>
        {screen === "new-report" && <NewReportPlaceholder />}
        {screen === "collectors" && (me.role === "admin" ? <AdminReleases state={state} /> : <EngineerCollectors state={state} />)}
        {screen === "users" && <UsersTable state={state} />}
        {screen === "reports" && <TasksTable state={state} />}
        {screen === "downloads" && <DownloadsTable state={state} />}
      </main>
    </div>
  );
}

function Table({ head, children }: { head: string[]; children: React.ReactNode }) {
  return (
    <div className="overflow-hidden rounded-lg border border-border bg-card">
      <table className="w-full text-xs">
        <thead className="border-b border-border bg-muted/40">
          <tr>{head.map((h) => <th key={h} className={th}>{h}</th>)}</tr>
        </thead>
        <tbody className="divide-y divide-border">{children}</tbody>
      </table>
    </div>
  );
}

function EngineerCollectors({ state }: { state: ConsoleState }) {
  const platform = detectPlatform();
  const latest = state.latest;
  const older = state.releases.filter((r) => r.status === "deprecated");
  const [showOld, setShowOld] = useState(false);

  return (
    <div className="space-y-6">
      {latest ? (
        <section className="space-y-2">
          <div className="flex items-baseline gap-2 text-sm">
            <span className="font-semibold">最新版本 {latest.version}</span>
            <ReleasePill status="latest" />
            <span className="text-xs text-muted-foreground">{formatTime(latest.publishedAt)} · {latest.tag}@{latest.commit} · 支持 {latest.dbTypes.join(" / ")}</span>
          </div>
          <Table head={["平台", "文件", "大小", "SHA256", ""]}>
            {latest.packages.map((p) => (
              <tr key={p.platform} className={cn(p.platform === platform && "bg-primary/5")}>
                <td className={td}>
                  {p.osLabel} {p.archLabel}
                  {p.platform === platform && <span className="ml-2 text-[10px] text-primary">当前平台</span>}
                </td>
                <td className={cn(td, "font-mono")}>{p.fileName}</td>
                <td className={td}>{formatSize(p.size)}</td>
                <td className={td}><CopySha sha={p.sha256} /></td>
                <td className={cn(td, "text-right")}>
                  <button type="button" onClick={() => state.download(latest.version, p.platform)} className={cn(btn, "border-primary/40 text-primary")}>下载</button>
                </td>
              </tr>
            ))}
          </Table>
          <pre className="whitespace-pre-wrap text-xs text-muted-foreground">{latest.notes}</pre>
        </section>
      ) : (
        <p className="rounded-lg border border-dashed border-border p-6 text-sm text-muted-foreground">当前没有推荐版本，请联系管理员。</p>
      )}

      <section>
        <h2 className="mb-2 text-sm font-semibold">使用说明</h2>
        <ol className="list-decimal space-y-1 pl-5 text-xs text-muted-foreground">
          {QUICKSTART.map((s) => <li key={s}>{s}</li>)}
        </ol>
      </section>

      <section>
        <button type="button" onClick={() => setShowOld(!showOld)} className="flex items-center gap-1 text-sm font-semibold cursor-pointer">
          {showOld ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          历史版本（{older.length}）
        </button>
        {showOld && (
          <div className="mt-2">
            <Table head={["版本", "状态", "发布时间", "下载"]}>
              {older.map((r) => (
                <tr key={r.version}>
                  <td className={cn(td, "font-mono")}>{r.version}</td>
                  <td className={td}><ReleasePill status={r.status} /> <span className="text-[11px] text-warning">不再推荐使用</span></td>
                  <td className={td}>{formatTime(r.publishedAt)}</td>
                  <td className={cn(td, "space-x-1")}>
                    {r.packages.map((p) => (
                      <button key={p.platform} type="button" onClick={() => state.download(r.version, p.platform)} className={btn}>{p.osLabel} {p.archLabel}</button>
                    ))}
                  </td>
                </tr>
              ))}
            </Table>
          </div>
        )}
      </section>
    </div>
  );
}

function AdminReleases({ state }: { state: ConsoleState }) {
  const [open, setOpen] = useState<string | null>(null);
  return (
    <Table head={["", "版本", "状态", "发布时间", "Tag / Commit", "数据库", "下载次数", "操作"]}>
      {state.releases.map((r) => {
        const records = state.downloads.filter((d) => d.version === r.version);
        return (
          <Fragment key={r.version}>
            <tr className={cn(open === r.version && "bg-muted/30")}>
              <td className={td}>
                <button type="button" onClick={() => setOpen(open === r.version ? null : r.version)} className="cursor-pointer">
                  {open === r.version ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                </button>
              </td>
              <td className={cn(td, "font-mono font-semibold")}>{r.version}</td>
              <td className={td}>
                <ReleasePill status={r.status} />
                {r.revokeReason && <p className="mt-1 max-w-48 text-[11px] text-destructive">{r.revokeReason}</p>}
              </td>
              <td className={td}>{formatTime(r.publishedAt)}</td>
              <td className={cn(td, "font-mono text-muted-foreground")}>{r.tag} · {r.commit}</td>
              <td className={td}>{r.dbTypes.join(", ")}</td>
              <td className={td}>{records.length}</td>
              <td className={cn(td, "space-x-1 whitespace-nowrap")}>
                {releaseActionsFor(r.status).map((a) => (
                  <button key={a} type="button" onClick={() => state.releaseActions[a](r.version)} className={cn(btn, a === "revoke" && "text-destructive")}>
                    {RELEASE_ACTION_LABEL[a]}
                  </button>
                ))}
              </td>
            </tr>
            {open === r.version && (
              <tr className="bg-muted/20">
                <td />
                <td colSpan={7} className="px-3 py-3">
                  <div className="grid grid-cols-2 gap-6">
                    <div>
                      <p className="mb-1 text-[11px] font-semibold">发布包</p>
                      {r.packages.map((p) => (
                        <div key={p.platform} className="flex items-center gap-3 py-0.5">
                          <span className="w-28">{p.osLabel} {p.archLabel}</span>
                          <span className="w-16">{formatSize(p.size)}</span>
                          <CopySha sha={p.sha256} />
                          <button type="button" onClick={() => state.download(r.version, p.platform)} className={btn}>下载</button>
                        </div>
                      ))}
                      <pre className="mt-2 whitespace-pre-wrap text-muted-foreground">{r.notes}</pre>
                    </div>
                    <div>
                      <p className="mb-1 text-[11px] font-semibold">下载记录（{records.length}）</p>
                      {records.map((d) => (
                        <p key={d.id} className="text-muted-foreground">
                          {formatTime(d.at)} · {state.userById(d.userId)?.displayName} · {d.platform}
                        </p>
                      ))}
                      {records.length === 0 && <p className="text-muted-foreground">暂无</p>}
                    </div>
                  </div>
                </td>
              </tr>
            )}
          </Fragment>
        );
      })}
    </Table>
  );
}

function UsersTable({ state }: { state: ConsoleState }) {
  const [filter, setFilter] = useState<AccountStatus | "all">(state.pendingCount > 0 ? "pending" : "all");
  const list = state.users.filter((u) => filter === "all" || u.status === filter);
  const a = state.userActions;
  const tabs: (AccountStatus | "all")[] = ["all", "pending", "active", "rejected", "disabled"];
  const tabLabel = { all: "全部", pending: "待审批", active: "正常", rejected: "已拒绝", disabled: "已禁用" };

  return (
    <div className="space-y-3">
      <div className="flex gap-1 border-b border-border">
        {tabs.map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setFilter(t)}
            className={cn("px-3 py-1.5 text-xs cursor-pointer border-b-2 -mb-px", filter === t ? "border-primary text-primary" : "border-transparent text-muted-foreground")}
          >
            {tabLabel[t]}（{t === "all" ? state.users.length : state.users.filter((u) => u.status === t).length}）
          </button>
        ))}
      </div>
      <Table head={["用户名", "姓名", "邮箱", "团队", "角色", "状态", "申请说明 / 原因", "最近操作", "操作"]}>
        {list.map((u) => (
          <tr key={u.id}>
            <td className={cn(td, "font-mono")}>{u.username}{u.id === state.me.id && <span className="ml-1 text-[10px] text-primary">（我）</span>}</td>
            <td className={td}>{u.displayName}</td>
            <td className={td}>{u.email}</td>
            <td className={td}>{u.team}</td>
            <td className={td}>{u.role === "admin" ? "管理员" : "普通用户"}</td>
            <td className={td}><AccountPill status={u.status} /></td>
            <td className={cn(td, "max-w-52 text-muted-foreground")}>{u.reason ? <span className="text-destructive">{u.reason}</span> : u.note}</td>
            <td className={cn(td, "text-muted-foreground")}>{u.lastAction ? `${u.lastAction.action} · ${u.lastAction.by} · ${formatTime(u.lastAction.at)}` : "—"}</td>
            <td className={cn(td, "space-x-1 whitespace-nowrap")}>
              {u.status === "pending" && (
                <>
                  <button type="button" onClick={() => a.approve(u.id)} className={cn(btn, "text-primary")}>批准</button>
                  <button type="button" onClick={() => a.reject(u.id)} className={cn(btn, "text-destructive")}>拒绝</button>
                </>
              )}
              {u.status === "active" && (
                <>
                  <button type="button" onClick={() => a.toggleRole(u.id)} className={btn}>{u.role === "admin" ? "降为普通用户" : "升为管理员"}</button>
                  <button type="button" onClick={() => a.resetPassword(u.id)} className={btn}>重置密码</button>
                  <button type="button" onClick={() => a.disable(u.id)} className={cn(btn, "text-destructive")}>禁用</button>
                </>
              )}
              {u.status === "disabled" && <button type="button" onClick={() => a.enable(u.id)} className={btn}>启用</button>}
            </td>
          </tr>
        ))}
      </Table>
    </div>
  );
}

function TasksTable({ state }: { state: ConsoleState }) {
  const isAdmin = state.me.role === "admin";
  const [submitter, setSubmitter] = useState("all");
  const [status, setStatus] = useState("all");
  const list = state.tasks.filter((t) => (submitter === "all" || t.submitterId === submitter) && (status === "all" || t.status === status));
  const select = "rounded-md border border-border bg-card px-2 py-1 text-xs";

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 text-xs">
        <select className={select} value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="all">全部状态</option>
          <option value="success">已完成</option>
          <option value="partial">部分成功</option>
          <option value="failed">执行失败</option>
          <option value="processing">处理中</option>
        </select>
        <select className={select} defaultValue="30d">
          <option value="7d">近 7 天</option>
          <option value="30d">近 30 天</option>
          <option value="all">全部时间</option>
        </select>
        {isAdmin && (
          <select className={select} value={submitter} onChange={(e) => setSubmitter(e.target.value)}>
            <option value="all">全部提交人</option>
            {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
          </select>
        )}
        <span className="text-muted-foreground">共 {list.length} 个任务</span>
      </div>
      <Table head={["任务", "提交时间", ...(isAdmin ? ["提交人"] : []), "状态", "报告项（采集器版本）", ""]}>
        {list.map((t) => (
          <tr key={t.id}>
            <td className={cn(td, "font-mono font-semibold")}>{t.id}</td>
            <td className={td}>{formatTime(t.createdAt)}</td>
            {isAdmin && <td className={td}>{state.userById(t.submitterId)?.displayName}</td>}
            <td className={td}><TaskPill status={t.status} /></td>
            <td className={cn(td, "space-y-1")}>
              {t.items.map((i) => (
                <div key={i.name} className="flex flex-wrap items-center gap-2">
                  {i.outcome === "failed" ? <XCircle className="h-3 w-3 text-destructive" /> : <span className="h-3 w-3" />}
                  <span className="font-mono">{i.name}</span>
                  <span className="text-muted-foreground">{i.dbType} · v{i.collectorVersion}</span>
                  <VersionNotice notice={state.versionNotice(i.collectorVersion)} />
                </div>
              ))}
            </td>
            <td className={cn(td, "text-right whitespace-nowrap")}>
              {t.filesExpired ? (
                <span className="inline-flex items-center gap-1 text-muted-foreground"><Clock className="h-3 w-3" />文件已过期</span>
              ) : t.status === "processing" ? null : (
                <button type="button" className={btn}>下载报告</button>
              )}
            </td>
          </tr>
        ))}
      </Table>
    </div>
  );
}

function DownloadsTable({ state }: { state: ConsoleState }) {
  const [user, setUser] = useState("all");
  const list = state.downloads.filter((d) => user === "all" || d.userId === user);
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 text-xs">
        <select className="rounded-md border border-border bg-card px-2 py-1" value={user} onChange={(e) => setUser(e.target.value)}>
          <option value="all">全部用户</option>
          {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
        </select>
        <select className="rounded-md border border-border bg-card px-2 py-1" defaultValue="30d">
          <option value="7d">近 7 天</option>
          <option value="30d">近 30 天</option>
          <option value="all">全部时间</option>
        </select>
      </div>
      <Table head={["时间", "用户", "版本", "发布包"]}>
        {list.map((d) => {
          const r = state.releases.find((x) => x.version === d.version);
          return (
            <tr key={d.id}>
              <td className={td}>{formatTime(d.at)}</td>
              <td className={td}>{state.userById(d.userId)?.displayName}</td>
              <td className={cn(td, "font-mono")}>{d.version} {r && <ReleasePill status={r.status} />}</td>
              <td className={td}>{d.platform}</td>
            </tr>
          );
        })}
      </Table>
    </div>
  );
}

function AccountGate({ state }: { state: ConsoleState }) {
  const { me } = state;
  return (
    <div className="flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-6 space-y-4">
        <div className="flex items-center gap-2.5">
          <Database className="h-5 w-5 text-primary" />
          <span className="text-sm font-bold">DB-Check</span>
          <AccountPill status={me.status} />
        </div>
        {me.status === "pending" && (
          <>
            <h1 className="text-lg font-semibold">账号等待管理员审批</h1>
            <p className="text-sm text-muted-foreground">{me.displayName}（{me.username}）于 {formatTime(me.registeredAt)} 提交申请。审批通过后即可使用。</p>
          </>
        )}
        {me.status === "rejected" && (
          <>
            <h1 className="text-lg font-semibold">申请未通过</h1>
            <p className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">原因：{me.reason}</p>
            <ResubmitFields state={state} />
          </>
        )}
        {me.status === "disabled" && <p className="text-sm text-destructive">账号已禁用，无法登录：{me.reason}</p>}
      </div>
    </div>
  );
}

function ResubmitFields({ state }: { state: ConsoleState }) {
  const { me } = state;
  const input = "w-full rounded-md border border-border bg-background px-2 py-1.5 text-sm";
  return (
    <div className="space-y-2 text-xs">
      <label className="block">用户名<input className={input} value={me.username} disabled /></label>
      <label className="block">邮箱<input className={input} value={me.email} disabled /></label>
      <label className="block">显示名称<input className={input} defaultValue={me.displayName} /></label>
      <label className="block">团队<input className={input} defaultValue={me.team} /></label>
      <label className="block">申请说明<textarea className={input} defaultValue={me.note} rows={3} /></label>
      <button type="button" onClick={() => state.userActions.resubmit(me.id)} className="w-full rounded-md bg-primary py-2 text-sm font-medium text-primary-foreground cursor-pointer">
        重新提交申请
      </button>
    </div>
  );
}
