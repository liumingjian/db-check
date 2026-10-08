"use client";

import { ApiError, type UserProfile } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";
import { useUsersStore } from "@/stores/users-store";
import { useDialogs, type AskOptions } from "@/components/console/dialog-host";
import { CopyText, type MenuItem } from "@/components/console/kit";

/**
 * The `···` account actions an admin may take on an approved user. Failures
 * from the API show as a toast; anything else is rethrown.
 */
export function useUserActions(): (user: UserProfile) => MenuItem[] {
  const token = useAuthStore((s) => s.token);
  const selfId = useAuthStore((s) => s.user?.id);
  const { disable, enable, promote, demote, resetPassword } = useUsersStore();
  const { toast, ask } = useDialogs();

  async function run<T>(action: Promise<T>, done: (result: T) => void) {
    try {
      done(await action);
    } catch (e) {
      if (!(e instanceof ApiError)) throw e;
      toast(e.message, "error");
    }
  }

  const askDisable = (token: string, u: UserProfile) =>
    ask({
      title: `禁用 ${u.displayName}`,
      body: "对方将无法登录，已提交的报告任务保留。可以随时重新启用。",
      input: "禁用原因",
      confirm: "禁用",
      danger: true,
      onConfirm: (reason) => void run(disable(token, u.id, reason), () => toast(`已禁用 ${u.displayName}`)),
    });

  const askReset = (token: string, u: UserProfile) =>
    ask({
      title: `重置 ${u.displayName} 的密码`,
      body: "会生成一个临时密码，请当面交给对方。对方下次登录必须先改密码。",
      confirm: "重置",
      danger: true,
      onConfirm: () => void run(resetPassword(token, u.id), (password) => ask(temporaryPasswordNotice(u, password))),
    });

  return (u) => {
    // An admin can neither disable nor demote themselves; resetting their own password would lock them out mid-session.
    if (!token || u.id === selfId) return [];
    const reset = { label: "重置密码…", onSelect: () => askReset(token, u) };
    if (u.status === "disabled") {
      return [{ label: "启用", onSelect: () => void run(enable(token, u.id), () => toast(`已启用 ${u.displayName}`)) }, reset];
    }
    return [
      u.role === "user"
        ? { label: "设为管理员", onSelect: () => void run(promote(token, u.id), () => toast(`${u.displayName} 已是管理员`)) }
        : { label: "取消管理员", onSelect: () => void run(demote(token, u.id), () => toast(`${u.displayName} 已改为工程师`)) },
      reset,
      { label: "禁用…", onSelect: () => askDisable(token, u), danger: true },
    ];
  };
}

/** Shows a reset's temporary password once, for the admin to hand over in person. */
function temporaryPasswordNotice(u: UserProfile, temporaryPassword: string): AskOptions {
  return {
    title: `${u.displayName} 的临时密码`,
    body: (
      <div className="mt-2 flex flex-col gap-3">
        <CopyText text={temporaryPassword} className="text-lg text-foreground" />
        <p>这个密码只显示这一次。</p>
      </div>
    ),
    confirm: "我已记下",
  };
}
