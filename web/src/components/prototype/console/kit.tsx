// PROTOTYPE — throwaway. Shared kit for the round-2/3 console variants:
// primitives (button, menu, dialog, toast), the admin pages and the
// account-status page. Colours come from the CSS variables each variant sets
// on its root, so these pieces adopt every variant's palette.
"use client";

import { createContext, useContext, useEffect, useRef, useState } from "react";
import { Check, Copy, Download, Hourglass, Loader2, MoreHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  RELEASE_ACTION_LABEL,
  releaseActionsFor,
  type ConsoleState,
  type DbType,
  type Release,
  type ReportTask,
  type User,
} from "./console-state";

export const EASE_OUT = "ease-[cubic-bezier(0.23,1,0.32,1)]";
export const PRESS = `transition-[transform,background-color,color,opacity] duration-150 ${EASE_OUT} active:scale-[0.97]`;
export const TODAY = "2026-10-01";
export const DB_LABEL: Record<DbType, string> = { mysql: "MySQL", oracle: "Oracle", gaussdb: "GaussDB" };

/* ── Primitives ── */

export function Button({
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

export interface AskOpts {
  title: string;
  body?: React.ReactNode;
  input?: string;
  confirm: string;
  danger?: boolean;
  onConfirm?: (value: string) => void;
}

export const Ui = createContext<{ toast: (msg: string) => void; ask: (o: AskOpts) => void }>({
  toast: () => {},
  ask: () => {},
});

export function Dialog({ opts, onClose }: { opts: AskOpts; onClose: () => void }) {
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

export function Menu({ items }: { items: { label: string; onSelect: () => void; danger?: boolean }[] }) {
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

export function CopyText({ text, label }: { text: string; label?: string }) {
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

export function PageHead({ title, sub }: { title: string; sub: string }) {
  return (
    <header className="mb-8">
      <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
      <p className="mt-1 text-sm text-muted-foreground">{sub}</p>
    </header>
  );
}

export function when(iso: string): string {
  const day = iso.slice(0, 10);
  const time = iso.slice(11, 16);
  if (day === TODAY) return `今天 ${time}`;
  if (day === "2026-09-30") return `昨天 ${time}`;
  return `${Number(iso.slice(5, 7))}月${Number(iso.slice(8, 10))}日 ${time}`;
}


/** Toast stack and dialog host; render inside the variant's token scope. */
export function UiHost({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<{ id: number; msg: string }[]>([]);
  const [dialog, setDialog] = useState<AskOpts | null>(null);
  const seq = useRef(0);

  function toast(msg: string) {
    const id = ++seq.current;
    setToasts((t) => [...t.slice(-2), { id, msg }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4000);
  }

  return (
    <Ui.Provider value={{ toast, ask: setDialog }}>
      {children}
      <div className="fixed right-5 bottom-5 z-[95] flex w-80 flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            className="flex items-center gap-2 rounded-xl bg-popover px-4 py-3 text-sm text-foreground shadow-lg ring-1 ring-border transition-[opacity,transform] duration-400 ease-out starting:translate-y-full starting:opacity-0"
          >
            <Check className="h-4 w-4 shrink-0 text-success" />
            {t.msg}
          </div>
        ))}
      </div>
      {dialog && <Dialog opts={dialog} onClose={() => setDialog(null)} />}
    </Ui.Provider>
  );
}

/* ── 采集器 ── */

export const USAGE: Record<DbType, string> = {
  mysql: "./db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password '***' --dbname dbcheck",
  oracle: "./db-collector --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password '***' --dbname ORCL",
  gaussdb: "./db-collector --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password '***' --dbname postgres",
};

export function releaseMenu(state: ConsoleState, ask: (o: AskOpts) => void, r: Release) {
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

/* ── 我的报告 ── */

export function ReportRow({ state, task, showSubmitter }: { state: ConsoleState; task: ReportTask; showSubmitter?: boolean }) {
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

/* ── 管理 ── */

export function Admin({ state }: { state: ConsoleState }) {
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

export function UsersAdmin({ state }: { state: ConsoleState }) {
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

export function DownloadsAdmin({ state }: { state: ConsoleState }) {
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

export function ReportsAdmin({ state }: { state: ConsoleState }) {
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

export function AccountGate({ state }: { state: ConsoleState }) {
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
