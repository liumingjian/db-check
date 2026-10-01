// PROTOTYPE — throwaway. Variant C: icon rail plus master-detail split panes
// (list left, detail right) for releases, users, report tasks and downloads.
"use client";

import { useState } from "react";
import {
  ClipboardList,
  Clock,
  Database,
  Download,
  FileBarChart,
  Hourglass,
  Package,
  ShieldAlert,
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
  type AccountStatus,
  type ConsoleState,
  type ReleaseStatus,
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
  "new-report": <FileBarChart className="h-5 w-5" />,
  reports: <ClipboardList className="h-5 w-5" />,
  collectors: <Package className="h-5 w-5" />,
  users: <Users className="h-5 w-5" />,
  downloads: <Download className="h-5 w-5" />,
};

const RELEASE_DOT: Record<ReleaseStatus, string> = {
  "pre-release": "bg-accent",
  latest: "bg-primary",
  deprecated: "bg-warning",
  revoked: "bg-destructive",
};

const ACCOUNT_DOT: Record<AccountStatus, string> = {
  pending: "bg-warning",
  active: "bg-primary",
  rejected: "bg-destructive",
  disabled: "bg-muted-foreground",
};

const action = "rounded-md border border-border px-2.5 py-1 text-xs hover:bg-muted cursor-pointer";

export function VariantC({ state, screen, setScreen }: VariantProps) {
  const { me } = state;
  if (me.status !== "active") return <AccountSplit state={state} />;

  return (
    <div className="flex h-screen">
      <aside className="flex w-14 flex-col items-center gap-1 border-r border-border bg-card py-3">
        <Database className="mb-3 h-5 w-5 text-primary" />
        {screensFor(me.role).map((s) => (
          <button
            key={s}
            type="button"
            title={SCREEN_LABEL[s]}
            onClick={() => setScreen(s)}
            className={cn(
              "relative flex h-10 w-10 items-center justify-center rounded-lg cursor-pointer",
              screen === s ? "bg-primary/15 text-primary" : "text-muted-foreground hover:bg-muted hover:text-foreground",
            )}
          >
            {ICONS[s]}
            {s === "users" && state.pendingCount > 0 && (
              <span className="absolute -top-0.5 -right-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-warning px-1 text-[10px] font-bold text-background">
                {state.pendingCount}
              </span>
            )}
          </button>
        ))}
        <span
          title={`${me.displayName}（${me.role === "admin" ? "管理员" : "普通用户"}）`}
          className={cn("mt-auto flex h-8 w-8 items-center justify-center rounded-full text-xs font-bold", me.role === "admin" ? "bg-primary/15 text-primary" : "bg-accent/15 text-accent")}
        >
          {me.displayName.slice(0, 1)}
        </span>
      </aside>

      {screen === "new-report" && <div className="flex-1 p-8"><NewReportPlaceholder /></div>}
      {screen === "collectors" && <Releases state={state} />}
      {screen === "users" && <UsersSplit state={state} />}
      {screen === "reports" && <TasksSplit state={state} />}
      {screen === "downloads" && <DownloadsSplit state={state} />}
    </div>
  );
}

function Split({ title, list, detail }: { title: React.ReactNode; list: React.ReactNode; detail: React.ReactNode }) {
  return (
    <>
      <section className="flex w-80 flex-col border-r border-border">
        <header className="flex h-12 items-center gap-2 border-b border-border px-4 text-sm font-semibold">{title}</header>
        <div className="flex-1 overflow-y-auto pb-28">{list}</div>
      </section>
      <section className="flex-1 overflow-y-auto px-8 py-6 pb-28">{detail}</section>
    </>
  );
}

function Item({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn("block w-full border-b border-border px-4 py-3 text-left cursor-pointer", active ? "bg-primary/10 border-l-2 border-l-primary" : "hover:bg-muted/50")}
    >
      {children}
    </button>
  );
}

function Releases({ state }: { state: ConsoleState }) {
  const isAdmin = state.me.role === "admin";
  const visible = state.releases.filter((r) => isAdmin || r.status === "latest" || r.status === "deprecated");
  const [sel, setSel] = useState(state.latest?.version ?? visible[0]?.version);
  const r = visible.find((x) => x.version === sel) ?? visible[0];
  const platform = detectPlatform();
  const records = r ? state.downloads.filter((d) => d.version === r.version) : [];

  return (
    <Split
      title={<>采集器版本 <span className="text-xs font-normal text-muted-foreground">{visible.length}</span></>}
      list={
        <>
          {!state.latest && (
            <p className="border-b border-border bg-warning/10 px-4 py-2 text-xs text-warning">当前没有推荐版本</p>
          )}
          {visible.map((x) => (
            <Item key={x.version} active={x.version === r?.version} onClick={() => setSel(x.version)}>
              <div className="flex items-center gap-2">
                <span className={cn("h-2 w-2 rounded-full", RELEASE_DOT[x.status])} />
                <span className="font-mono text-sm font-semibold">v{x.version}</span>
                <span className="ml-auto"><ReleasePill status={x.status} /></span>
              </div>
              <p className="mt-1 pl-4 text-[11px] text-muted-foreground">{formatTime(x.publishedAt)}{isAdmin && ` · 下载 ${state.downloads.filter((d) => d.version === x.version).length}`}</p>
            </Item>
          ))}
          <div className="p-4 text-[11px] text-muted-foreground">
            <p className="mb-1 font-semibold">使用说明</p>
            <ol className="list-decimal space-y-0.5 pl-4">{QUICKSTART.map((s) => <li key={s}>{s}</li>)}</ol>
          </div>
        </>
      }
      detail={
        r && (
          <div className="max-w-3xl space-y-6">
            <div className="flex items-start gap-3">
              <div>
                <h1 className="flex items-center gap-2 text-2xl font-bold">v{r.version} <ReleasePill status={r.status} /></h1>
                <p className="text-xs text-muted-foreground">{r.tag} · {r.commit} · {formatTime(r.publishedAt)} · 支持 {r.dbTypes.join(" / ")}</p>
              </div>
              {isAdmin && (
                <div className="ml-auto flex gap-1.5">
                  {releaseActionsFor(r.status).map((a) => (
                    <button key={a} type="button" onClick={() => state.releaseActions[a](r.version)} className={cn(action, a === "revoke" && "text-destructive")}>
                      {RELEASE_ACTION_LABEL[a]}
                    </button>
                  ))}
                </div>
              )}
            </div>
            {r.status === "deprecated" && <p className="rounded-lg bg-warning/10 px-3 py-2 text-xs text-warning">此版本已弃用，建议使用最新版本。</p>}
            {r.revokeReason && <p className="rounded-lg bg-destructive/10 px-3 py-2 text-xs text-destructive">撤回原因：{r.revokeReason}</p>}

            <div className="space-y-2">
              {r.packages.map((p) => (
                <div key={p.platform} className={cn("flex items-center gap-4 rounded-lg border px-4 py-3", p.platform === platform ? "border-primary/40 bg-primary/5" : "border-border")}>
                  <div className="w-36">
                    <p className="text-sm font-medium">{p.osLabel} {p.archLabel}</p>
                    {p.platform === platform && <p className="text-[10px] text-primary">当前平台</p>}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="truncate font-mono text-xs">{p.fileName}</p>
                    <CopySha sha={p.sha256} />
                  </div>
                  <span className="text-xs text-muted-foreground">{formatSize(p.size)}</span>
                  <button type="button" onClick={() => state.download(r.version, p.platform)} className="inline-flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground cursor-pointer">
                    <Download className="h-3.5 w-3.5" />下载
                  </button>
                </div>
              ))}
            </div>

            <div>
              <p className="mb-1 text-xs font-semibold">更新内容</p>
              <pre className="whitespace-pre-wrap text-xs text-muted-foreground">{r.notes}</pre>
            </div>

            {isAdmin && (
              <div>
                <p className="mb-2 text-xs font-semibold">下载记录（{records.length}）</p>
                <div className="divide-y divide-border rounded-lg border border-border text-xs">
                  {records.map((d) => (
                    <div key={d.id} className="flex gap-4 px-3 py-2">
                      <span className="text-muted-foreground">{formatTime(d.at)}</span>
                      <span>{state.userById(d.userId)?.displayName}</span>
                      <span className="text-muted-foreground">{d.platform}</span>
                    </div>
                  ))}
                  {records.length === 0 && <p className="px-3 py-2 text-muted-foreground">暂无</p>}
                </div>
              </div>
            )}
          </div>
        )
      }
    />
  );
}

function UsersSplit({ state }: { state: ConsoleState }) {
  const sorted = [...state.users].sort((a, b) => Number(b.status === "pending") - Number(a.status === "pending"));
  const [sel, setSel] = useState(sorted[0]?.id);
  const u = state.users.find((x) => x.id === sel) ?? sorted[0];
  const a = state.userActions;

  return (
    <Split
      title={<>用户 <span className="text-xs font-normal text-muted-foreground">{state.users.length} · 待审批 {state.pendingCount}</span></>}
      list={sorted.map((x) => (
        <Item key={x.id} active={x.id === u.id} onClick={() => setSel(x.id)}>
          <div className="flex items-center gap-2">
            <span className={cn("h-2 w-2 rounded-full", ACCOUNT_DOT[x.status])} />
            <span className="text-sm font-medium">{x.displayName}</span>
            {x.role === "admin" && <span className="text-[10px] text-accent">管理员</span>}
            <span className="ml-auto"><AccountPill status={x.status} /></span>
          </div>
          <p className="mt-1 pl-4 text-[11px] text-muted-foreground">@{x.username} · {x.team}</p>
        </Item>
      ))}
      detail={
        <div className="max-w-2xl space-y-6">
          <div>
            <h1 className="flex items-center gap-2 text-2xl font-bold">{u.displayName} <AccountPill status={u.status} /></h1>
            <p className="text-xs text-muted-foreground">@{u.username} · {u.email} · {u.team} · {u.role === "admin" ? "管理员" : "普通用户"}{u.id === state.me.id && "（我）"}</p>
          </div>
          <dl className="grid grid-cols-[6rem_1fr] gap-y-2 text-sm">
            <dt className="text-muted-foreground">申请说明</dt><dd>{u.note || "—"}</dd>
            <dt className="text-muted-foreground">注册时间</dt><dd>{formatTime(u.registeredAt)}</dd>
            {u.reason && (<><dt className="text-muted-foreground">原因</dt><dd className="text-destructive">{u.reason}</dd></>)}
            <dt className="text-muted-foreground">最近操作</dt>
            <dd>{u.lastAction ? `${u.lastAction.action} · ${u.lastAction.by} · ${formatTime(u.lastAction.at)}` : "—"}</dd>
          </dl>
          <div className="flex flex-wrap gap-2 border-t border-border pt-4">
            {u.status === "pending" && (
              <>
                <button type="button" onClick={() => a.approve(u.id)} className="rounded-md bg-primary px-4 py-1.5 text-sm font-medium text-primary-foreground cursor-pointer">批准</button>
                <button type="button" onClick={() => a.reject(u.id)} className={cn(action, "text-destructive")}>拒绝…</button>
              </>
            )}
            {u.status === "active" && (
              <>
                <button type="button" onClick={() => a.toggleRole(u.id)} className={action}>{u.role === "admin" ? "降为普通用户" : "升为管理员"}</button>
                <button type="button" onClick={() => a.resetPassword(u.id)} className={action}>重置密码</button>
                <button type="button" onClick={() => a.disable(u.id)} className={cn(action, "text-destructive")}>禁用…</button>
              </>
            )}
            {u.status === "disabled" && <button type="button" onClick={() => a.enable(u.id)} className={action}>启用</button>}
            {u.status === "rejected" && <p className="text-xs text-muted-foreground">等待申请人重新提交</p>}
          </div>
          <div>
            <p className="mb-2 text-xs font-semibold">该用户的下载记录</p>
            {state.downloads.filter((d) => d.userId === u.id).map((d) => (
              <p key={d.id} className="text-xs text-muted-foreground">{formatTime(d.at)} · v{d.version} · {d.platform}</p>
            ))}
          </div>
        </div>
      }
    />
  );
}

function TasksSplit({ state }: { state: ConsoleState }) {
  const isAdmin = state.me.role === "admin";
  const [submitter, setSubmitter] = useState("all");
  const list = state.tasks.filter((t) => submitter === "all" || t.submitterId === submitter);
  const [sel, setSel] = useState(list[0]?.id);
  const t = list.find((x) => x.id === sel) ?? list[0];

  return (
    <Split
      title={
        <>
          报告任务
          {isAdmin && (
            <select value={submitter} onChange={(e) => setSubmitter(e.target.value)} className="ml-auto rounded-md border border-border bg-card px-1.5 py-0.5 text-xs font-normal">
              <option value="all">全部提交人</option>
              {state.users.map((u) => <option key={u.id} value={u.id}>{u.displayName}</option>)}
            </select>
          )}
        </>
      }
      list={list.map((x) => {
        const worst = x.items.map((i) => state.versionNotice(i.collectorVersion)).find((n) => n?.tone === "danger");
        return (
          <Item key={x.id} active={x.id === t?.id} onClick={() => setSel(x.id)}>
            <div className="flex items-center gap-2">
              <span className="font-mono text-xs font-semibold">{x.id}</span>
              <span className="ml-auto"><TaskPill status={x.status} /></span>
            </div>
            <p className="mt-1 flex items-center gap-1 text-[11px] text-muted-foreground">
              {formatTime(x.createdAt)} · {x.items.length} 项{isAdmin && ` · ${state.userById(x.submitterId)?.displayName}`}
              {worst && <ShieldAlert className="h-3 w-3 text-destructive" />}
              {x.filesExpired && <Clock className="h-3 w-3" />}
            </p>
          </Item>
        );
      })}
      detail={
        t && (
          <div className="max-w-3xl space-y-5">
            <div className="flex items-start gap-3">
              <div>
                <h1 className="flex items-center gap-2 font-mono text-xl font-bold">{t.id} <TaskPill status={t.status} /></h1>
                <p className="text-xs text-muted-foreground">{formatTime(t.createdAt)} · 提交人 {state.userById(t.submitterId)?.displayName}</p>
              </div>
              <div className="ml-auto">
                {t.filesExpired ? (
                  <span className="inline-flex items-center gap-1 text-xs text-muted-foreground"><Clock className="h-3.5 w-3.5" />文件已过期（保留 30 天）</span>
                ) : t.status !== "processing" && (
                  <button type="button" className="inline-flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground cursor-pointer"><Download className="h-3.5 w-3.5" />下载报告</button>
                )}
              </div>
            </div>
            <div className="divide-y divide-border rounded-lg border border-border">
              {t.items.map((i) => (
                <div key={i.name} className="space-y-1 px-4 py-3">
                  <div className="flex items-center gap-3 text-sm">
                    <span className="font-mono">{i.name}</span>
                    <span className="text-xs text-muted-foreground">{i.dbType}</span>
                    <span className="ml-auto text-xs">{i.outcome === "success" ? <span className="text-primary">成功</span> : <span className="text-destructive">失败</span>}</span>
                  </div>
                  <p className="text-xs text-muted-foreground">采集器版本 v{i.collectorVersion}</p>
                  <VersionNotice notice={state.versionNotice(i.collectorVersion)} />
                </div>
              ))}
            </div>
          </div>
        )
      }
    />
  );
}

function DownloadsSplit({ state }: { state: ConsoleState }) {
  const [sel, setSel] = useState<string>("all");
  const list = state.downloads.filter((d) => sel === "all" || d.userId === sel);
  const counts = (id: string) => state.downloads.filter((d) => d.userId === id).length;

  return (
    <Split
      title="下载记录 · 按用户"
      list={
        <>
          <Item active={sel === "all"} onClick={() => setSel("all")}>
            <span className="text-sm font-medium">全部用户</span>
            <span className="float-right text-xs text-muted-foreground">{state.downloads.length}</span>
          </Item>
          {state.users.filter((u) => counts(u.id) > 0).map((u) => (
            <Item key={u.id} active={sel === u.id} onClick={() => setSel(u.id)}>
              <span className="text-sm">{u.displayName}</span>
              <span className="float-right text-xs text-muted-foreground">{counts(u.id)}</span>
            </Item>
          ))}
        </>
      }
      detail={
        <div className="max-w-3xl divide-y divide-border rounded-lg border border-border text-sm">
          {list.map((d) => {
            const r = state.releases.find((x) => x.version === d.version);
            return (
              <div key={d.id} className="flex items-center gap-4 px-4 py-2.5">
                <span className="w-36 text-xs text-muted-foreground">{formatTime(d.at)}</span>
                <span className="w-20">{state.userById(d.userId)?.displayName}</span>
                <span className="font-mono">v{d.version}</span>
                {r && <ReleasePill status={r.status} />}
                <span className="ml-auto text-xs text-muted-foreground">{d.platform}</span>
              </div>
            );
          })}
        </div>
      }
    />
  );
}

function AccountSplit({ state }: { state: ConsoleState }) {
  const { me } = state;
  const input = "w-full rounded-md border border-border bg-background px-3 py-2 text-sm";
  return (
    <div className="grid min-h-screen grid-cols-[2fr_3fr]">
      <div className="flex flex-col justify-between bg-card p-10">
        <div className="flex items-center gap-2.5">
          <Database className="h-6 w-6 text-primary" />
          <span className="font-bold">DB-Check 数据库巡检平台</span>
        </div>
        <ol className="space-y-3 text-sm">
          {[["注册申请", true], ["管理员审批", me.status === "rejected"], ["开始使用", false]].map(([label, done], i) => (
            <li key={String(label)} className="flex items-center gap-3">
              <span className={cn("flex h-6 w-6 items-center justify-center rounded-full text-xs", done ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground")}>{i + 1}</span>
              {label}
            </li>
          ))}
        </ol>
        <p className="text-xs text-muted-foreground">{me.displayName} · @{me.username}</p>
      </div>
      <div className="flex items-center p-10 pb-28">
        <div className="w-full max-w-lg space-y-5">
          {me.status === "pending" ? (
            <>
              <Hourglass className="h-8 w-8 text-warning" />
              <h1 className="text-2xl font-bold">等待管理员审批</h1>
              <p className="text-sm text-muted-foreground">申请提交于 {formatTime(me.registeredAt)}。团队：{me.team}。</p>
            </>
          ) : (
            <>
              <h1 className="text-2xl font-bold">申请未通过</h1>
              <p className="border-l-2 border-destructive pl-3 text-sm text-destructive">{me.reason}</p>
              <p className="text-xs text-muted-foreground">修改信息后可重新提交，用户名与邮箱不变。</p>
              <div className="space-y-2">
                <input className={input} defaultValue={me.displayName} placeholder="显示名称" />
                <input className={input} defaultValue={me.team} placeholder="团队" />
                <textarea className={input} defaultValue={me.note} rows={4} placeholder="申请说明" />
                <button type="button" onClick={() => state.userActions.resubmit(me.id)} className="rounded-md bg-primary px-5 py-2 text-sm font-medium text-primary-foreground cursor-pointer">
                  重新提交申请
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
