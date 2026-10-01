"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useAuthStore } from "@/stores/auth-store";
import { FormError, INPUT } from "@/components/console/account/form";
import { toastOnNextScreen } from "@/components/console/dialog-host";
import { Brand, CAPTION, TOP_BAR_ACTION, YellowButton } from "@/components/console/kit";
import { ResetMockDataButton } from "@/components/console/reset-mock-data";
import { SessionGuard } from "@/components/console/session-guard";

function ChangePasswordForm() {
  const router = useRouter();
  const displayName = useAuthStore((s) => s.user?.displayName);
  // Fixed on arrival: a finished forced change must not flip the copy before the page leaves.
  const [forced] = useState(() => Boolean(useAuthStore.getState().user?.mustChangePassword));
  const changePassword = useAuthStore((s) => s.changePassword);
  const logout = useAuthStore((s) => s.logout);
  const [current, setCurrent] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!forced && !current) {
      setError("请输入当前密码");
      return;
    }
    if (!password.trim()) {
      setError("请输入新密码");
      return;
    }
    if (password !== confirm) {
      setError("两次输入的密码不一致");
      return;
    }
    setBusy(true);
    const failure = await changePassword(password, forced ? undefined : current);
    setBusy(false);
    if (failure) {
      setError(failure);
      return;
    }
    toastOnNextScreen("密码已修改");
    router.replace("/");
  }

  return (
    <main className="min-h-screen px-8 py-7">
      <div className="flex items-center justify-between">
        <Brand />
        <div className="flex items-center gap-6">
          <ResetMockDataButton className={TOP_BAR_ACTION} />
          {forced ? (
            <button type="button" onClick={() => void logout()} className={TOP_BAR_ACTION}>
              退出登录
            </button>
          ) : (
            <Link href="/" className={TOP_BAR_ACTION}>
              返回
            </Link>
          )}
        </div>
      </div>
      <div className="mx-auto mt-[12vh] max-w-[440px]">
        <p className={CAPTION}>Change password</p>
        <h1 className="mt-4 text-[56px] leading-[1.05] font-bold tracking-[-2px]">
          {forced ? "设个新密码" : "修改密码"}
          <span className="text-primary">。</span>
        </h1>
        <p className="mt-4 text-base text-muted-foreground">
          {forced
            ? `${displayName}，管理员重置了你的密码。换成只有你知道的密码后才能继续。`
            : `${displayName}，先输入当前密码，再设一个新密码。`}
        </p>

        <form onSubmit={submit} className="mt-10 flex flex-col gap-3">
          {!forced && (
            <input
              aria-label="当前密码"
              type="password"
              autoComplete="current-password"
              autoFocus
              value={current}
              onChange={(e) => setCurrent(e.target.value)}
              placeholder="当前密码"
              className={INPUT}
            />
          )}
          <input
            aria-label="新密码"
            type="password"
            autoComplete="new-password"
            autoFocus={forced}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="新密码"
            className={INPUT}
          />
          <input
            aria-label="确认新密码"
            type="password"
            autoComplete="new-password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            placeholder="确认新密码"
            className={INPUT}
          />
          <FormError message={error} />
          <YellowButton type="submit" disabled={busy} className="mt-3 w-full">
            修改密码
          </YellowButton>
        </form>
      </div>
    </main>
  );
}

/**
 * `/change-password`: every route leads here after a password reset until the
 * user sets their own; an active user also comes here from the account menu.
 */
export default function ChangePasswordPage() {
  return (
    <SessionGuard access="password">
      <ChangePasswordForm />
    </SessionGuard>
  );
}
