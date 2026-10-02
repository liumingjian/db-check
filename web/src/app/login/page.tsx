"use client";

import { useState } from "react";
import Link from "next/link";
import { apiMode } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";
import { FormError, INPUT } from "@/components/console/account/form";
import { Brand, CAPTION, PRESS, YellowButton } from "@/components/console/kit";
import { ResetMockDataButton } from "@/components/console/reset-mock-data";
import { SessionGuard } from "@/components/console/session-guard";

function SignInForm() {
  const login = useAuthStore((s) => s.login);
  const quickLogin = useAuthStore((s) => s.quickLogin);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!username.trim() || !password) {
      setError("请输入用户名和密码");
      return;
    }
    setBusy(true);
    const failure = await login(username.trim(), password);
    setBusy(false);
    // On success the session guard moves the now signed-in visitor to "/".
    if (failure) setError(failure);
  }

  return (
    <main className="min-h-screen px-8 py-7">
      <Brand />
      <div className="mx-auto mt-[12vh] max-w-[440px]">
        <p className={CAPTION}>Sign in</p>
        <h1 className="mt-4 text-[56px] leading-[1.05] font-bold tracking-[-2px]">
          登录<span className="text-primary">。</span>
        </h1>

        <form onSubmit={submit} className="mt-10 flex flex-col gap-3">
          <input
            aria-label="用户名"
            autoComplete="username"
            autoFocus
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="用户名"
            className={INPUT}
          />
          <input
            aria-label="密码"
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="密码"
            className={INPUT}
          />
          <FormError message={error} />
          <YellowButton type="submit" disabled={busy} className="mt-3 w-full">
            登录
          </YellowButton>
        </form>

        <p className="mt-6 text-sm text-muted-foreground">
          还没有账号？
          <Link href="/register" className="ml-1 font-semibold text-foreground hover:text-primary">
            申请账号 →
          </Link>
        </p>

        {apiMode === "mock" && (
          <div className="mt-12 border-t border-border pt-6">
            <p className="text-xs text-muted-foreground">Mock 数据模式，用预设账号快速进入：</p>
            <div className="mt-3 flex gap-6">
              {(
                [
                  ["admin", "管理员"],
                  ["user", "工程师"],
                ] as const
              ).map(([role, label]) => (
                <button
                  key={role}
                  type="button"
                  onClick={() => void quickLogin(role)}
                  className={cn("cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground", PRESS)}
                >
                  {label} →
                </button>
              ))}
              <ResetMockDataButton className={cn("ml-auto cursor-pointer text-sm text-muted-foreground hover:text-foreground", PRESS)} />
            </div>
          </div>
        )}
      </div>
    </main>
  );
}

export default function LoginPage() {
  return (
    <SessionGuard access="public">
      <SignInForm />
    </SessionGuard>
  );
}
