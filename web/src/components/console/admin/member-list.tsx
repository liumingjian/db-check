"use client";

import { useState } from "react";
import { ApiError, type Account } from "@/lib/api";
import type { AccountActionKind } from "@/lib/api/users/contract";
import { useAccountsStore } from "@/stores/accounts-store";
import { useAuthStore } from "@/stores/auth-store";
import { appliedAtLabel } from "@/components/console/account/form";
import { useDialogs } from "@/components/console/dialog-host";
import { CAPTION, Chip, CopyText, Menu, type MenuItem } from "@/components/console/kit";

const ACTION_LABEL: Record<AccountActionKind, string> = {
  approve: "批准",
  reject: "拒绝",
  disable: "禁用",
  enable: "启用",
  promote: "设为管理员",
  demote: "取消管理员",
  reset: "重置密码",
};

const FILTERS = {
  all: { label: "全部", keep: () => true },
  engineers: { label: "工程师", keep: (a: Account) => a.status === "active" && a.role === "user" },
  admins: { label: "管理员", keep: (a: Account) => a.status === "active" && a.role === "admin" },
  disabled: { label: "已禁用", keep: (a: Account) => a.status === "disabled" },
} satisfies Record<string, { label: string; keep: (a: Account) => boolean }>;

type Filter = keyof typeof FILTERS;

/** Members are accounts past the application stage: active or disabled. */
function isMember(a: Account): boolean {
  return a.status === "active" || a.status === "disabled";
}

function MemberRow({ member, self, actions }: { member: Account; self: boolean; actions: MenuItem[] }) {
  return (
    <div className="flex items-center gap-6 py-4">
      <div className="min-w-0 flex-1">
        <p className="text-base font-semibold">
          {member.displayName}
          {self && <span className="ml-2 text-sm font-normal text-muted-foreground">（你）</span>}
        </p>
        <p className="mt-0.5 text-sm text-muted-foreground">
          @{member.username} · {member.team}
        </p>
      </div>
      <div className="w-56 text-sm">
        <span className={member.role === "admin" ? "text-primary" : "text-[#ccc]"}>{member.role === "admin" ? "管理员" : "工程师"}</span>
        {member.status === "disabled" && <span className="ml-3 text-destructive">已禁用</span>}
        {member.mustChangePassword && <span className="ml-3 text-warning">待改密码</span>}
        {member.status === "disabled" && member.reason && <p className="mt-0.5 truncate text-muted-foreground">{member.reason}</p>}
      </div>
      <p className="w-52 text-sm text-muted-foreground tabular-nums">
        {member.lastAction && `${member.lastAction.by} ${ACTION_LABEL[member.lastAction.action]} · ${appliedAtLabel(member.lastAction.at)}`}
      </p>
      <div className="w-7">
        <Menu items={actions} />
      </div>
    </div>
  );
}

/** 管理 → 用户, below the applicants: every member with their `···` account actions. */
export function MemberList({ accounts }: { accounts: Account[] }) {
  const token = useAuthStore((s) => s.token);
  const selfId = useAuthStore((s) => s.user?.id);
  const { disable, enable, promote, demote, resetPassword } = useAccountsStore();
  const { toast, ask } = useDialogs();
  const [filter, setFilter] = useState<Filter>("all");

  if (!token) return null;
  const members = accounts.filter(isMember);
  const shown = members.filter(FILTERS[filter].keep);

  async function run<T>(action: Promise<T>, done: (result: T) => void) {
    try {
      done(await action);
    } catch (e) {
      if (!(e instanceof ApiError)) throw e;
      toast(e.message);
    }
  }

  const askDisable = (m: Account) =>
    ask({
      title: `禁用 ${m.displayName}`,
      body: "对方将无法登录，已提交的报告任务保留。可以随时重新启用。",
      input: "禁用原因",
      confirm: "禁用",
      danger: true,
      onConfirm: (reason) => void run(disable(token, m.id, reason), () => toast(`已禁用 ${m.displayName}`)),
    });

  const askReset = (m: Account) =>
    ask({
      title: `重置 ${m.displayName} 的密码`,
      body: "会生成一个临时密码，请当面交给对方。对方下次登录必须先改密码。",
      confirm: "重置",
      danger: true,
      onConfirm: () =>
        void run(resetPassword(token, m.id), (temporaryPassword) =>
          ask({
            title: `${m.displayName} 的临时密码`,
            body: (
              <div className="mt-2 flex flex-col gap-3">
                <CopyText text={temporaryPassword} className="text-lg text-foreground" />
                <p>这个密码只显示这一次。</p>
              </div>
            ),
            confirm: "我已记下",
          }),
        ),
    });

  const actionsFor = (m: Account): MenuItem[] => {
    // An admin can neither disable nor demote themselves; resetting their own password would lock them out mid-session.
    if (m.id === selfId) return [];
    if (m.status === "disabled") {
      return [
        { label: "启用", onSelect: () => void run(enable(token, m.id), () => toast(`已启用 ${m.displayName}`)) },
        { label: "重置密码…", onSelect: () => askReset(m) },
      ];
    }
    return [
      m.role === "user"
        ? { label: "设为管理员", onSelect: () => void run(promote(token, m.id), () => toast(`${m.displayName} 已是管理员`)) }
        : { label: "取消管理员", onSelect: () => void run(demote(token, m.id), () => toast(`${m.displayName} 已改为工程师`)) },
      { label: "重置密码…", onSelect: () => askReset(m) },
      { label: "禁用…", onSelect: () => askDisable(m), danger: true },
    ];
  };

  return (
    <section className="mt-20">
      <div className="flex items-center justify-between">
        <p className={CAPTION}>成员 · {members.length}</p>
        <div className="flex gap-1">
          {(Object.keys(FILTERS) as Filter[]).map((key) => (
            <Chip key={key} on={filter === key} onClick={() => setFilter(key)}>
              {FILTERS[key].label}
            </Chip>
          ))}
        </div>
      </div>
      <div className="mt-4 divide-y divide-border border-y border-border">
        {shown.map((m) => (
          <MemberRow key={m.id} member={m} self={m.id === selfId} actions={actionsFor(m)} />
        ))}
        {shown.length === 0 && <p className="py-4 text-sm text-muted-foreground">没有符合条件的成员。</p>}
      </div>
    </section>
  );
}
