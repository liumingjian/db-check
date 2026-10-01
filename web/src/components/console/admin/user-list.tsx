"use client";

import { useState } from "react";
import type { UserProfile } from "@/lib/api";
import type { AccountActionKind } from "@/lib/api/users/contract";
import { useAuthStore } from "@/stores/auth-store";
import { appliedAtLabel } from "@/components/console/account/form";
import { useUserActions } from "@/components/console/admin/use-user-actions";
import { CAPTION, Chip, Menu, type MenuItem } from "@/components/console/kit";

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
  engineers: { label: "工程师", keep: (a: UserProfile) => a.status === "active" && a.role === "user" },
  admins: { label: "管理员", keep: (a: UserProfile) => a.status === "active" && a.role === "admin" },
  disabled: { label: "已禁用", keep: (a: UserProfile) => a.status === "disabled" },
} satisfies Record<string, { label: string; keep: (a: UserProfile) => boolean }>;

type Filter = keyof typeof FILTERS;

/** Users past the application stage: active or disabled. */
function wasApproved(a: UserProfile): boolean {
  return a.status === "active" || a.status === "disabled";
}

function UserRow({ user, self, actions }: { user: UserProfile; self: boolean; actions: MenuItem[] }) {
  const latest = user.actions.at(-1);
  return (
    <div className="flex items-center gap-6 py-4">
      <div className="min-w-0 flex-1">
        <p className="text-base font-semibold">
          {user.displayName}
          {self && <span className="ml-2 text-sm font-normal text-muted-foreground">（你）</span>}
        </p>
        <p className="mt-0.5 text-sm text-muted-foreground">
          @{user.username} · {user.team}
        </p>
      </div>
      <div className="w-56 text-sm">
        <span className={user.role === "admin" ? "text-primary" : "text-[#ccc]"}>{user.role === "admin" ? "管理员" : "工程师"}</span>
        {user.status === "disabled" && <span className="ml-3 text-destructive">已禁用</span>}
        {user.mustChangePassword && <span className="ml-3 text-warning">待改密码</span>}
        {user.status === "disabled" && user.reason && <p className="mt-0.5 truncate text-muted-foreground">{user.reason}</p>}
      </div>
      <p className="w-52 text-sm text-muted-foreground tabular-nums">
        {latest && `${latest.by} ${ACTION_LABEL[latest.action]} · ${appliedAtLabel(latest.at)}`}
      </p>
      <div className="w-7">
        <Menu items={actions} />
      </div>
    </div>
  );
}

/** 管理 → 用户, below the applicants: every approved user with their `···` account actions. */
export function UserList({ users }: { users: UserProfile[] }) {
  const selfId = useAuthStore((s) => s.user?.id);
  const actionsFor = useUserActions();
  const [filter, setFilter] = useState<Filter>("all");
  const approved = users.filter(wasApproved);
  const shown = approved.filter(FILTERS[filter].keep);

  return (
    <section className="mt-20">
      <div className="flex items-center justify-between">
        <p className={CAPTION}>用户 · {approved.length}</p>
        <div className="flex gap-1">
          {(Object.keys(FILTERS) as Filter[]).map((key) => (
            <Chip key={key} on={filter === key} onClick={() => setFilter(key)}>
              {FILTERS[key].label}
            </Chip>
          ))}
        </div>
      </div>
      <div className="mt-4 divide-y divide-border border-y border-border">
        {shown.map((u) => (
          <UserRow key={u.id} user={u} self={u.id === selfId} actions={actionsFor(u)} />
        ))}
        {shown.length === 0 && <p className="py-4 text-sm text-muted-foreground">没有符合条件的用户。</p>}
      </div>
    </section>
  );
}
