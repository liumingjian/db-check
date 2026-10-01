"use client";

import { useState } from "react";
import Link from "next/link";
import type { Registration } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";
import { FormError, INPUT, TEXTAREA } from "@/components/console/account/form";
import { Brand, CAPTION, YellowButton } from "@/components/console/kit";
import { SessionGuard } from "@/components/console/session-guard";

const EMPTY: Registration = { username: "", password: "", displayName: "", email: "", team: "", note: "" };

const FIELDS: { key: Exclude<keyof Registration, "note" | "password">; label: string; type?: string; autoComplete: string }[] = [
  { key: "username", label: "用户名", autoComplete: "username" },
  { key: "displayName", label: "显示名称", autoComplete: "name" },
  { key: "email", label: "邮箱", type: "email", autoComplete: "email" },
  { key: "team", label: "团队", autoComplete: "organization" },
];

function RegisterForm() {
  const register = useAuthStore((s) => s.register);
  const [form, setForm] = useState<Registration>(EMPTY);
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  function set<K extends keyof Registration>(key: K, value: Registration[K]) {
    setForm((f) => ({ ...f, [key]: value }));
  }

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (FIELDS.some(({ key }) => !form[key].trim()) || !form.password) {
      setError("请填写用户名、显示名称、邮箱、团队和密码");
      return;
    }
    if (form.password !== confirm) {
      setError("两次输入的密码不一致");
      return;
    }
    setBusy(true);
    const failure = await register(form);
    setBusy(false);
    // On success the session guard moves the new, pending applicant to /pending.
    if (failure) setError(failure);
  }

  return (
    <main className="min-h-screen px-8 py-7">
      <Brand />
      <div className="mx-auto mt-[8vh] max-w-[440px] pb-24">
        <p className={CAPTION}>Register</p>
        <h1 className="mt-4 text-[56px] leading-[1.05] font-bold tracking-[-2px]">
          申请账号<span className="text-primary">。</span>
        </h1>
        <p className="mt-4 text-muted-foreground">管理员批准后即可登录使用。</p>

        <form onSubmit={submit} className="mt-10 flex flex-col gap-3">
          {FIELDS.map(({ key, label, type, autoComplete }, i) => (
            <input
              key={key}
              aria-label={label}
              placeholder={label}
              type={type}
              autoComplete={autoComplete}
              autoFocus={i === 0}
              value={form[key]}
              onChange={(e) => set(key, e.target.value)}
              className={INPUT}
            />
          ))}
          <textarea
            aria-label="申请说明"
            placeholder="申请说明（选填）"
            rows={3}
            value={form.note}
            onChange={(e) => set("note", e.target.value)}
            className={TEXTAREA}
          />
          <input
            aria-label="密码"
            placeholder="密码"
            type="password"
            autoComplete="new-password"
            value={form.password}
            onChange={(e) => set("password", e.target.value)}
            className={INPUT}
          />
          <input
            aria-label="确认密码"
            placeholder="确认密码"
            type="password"
            autoComplete="new-password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            className={INPUT}
          />
          <FormError message={error} />
          <YellowButton type="submit" disabled={busy} className="mt-3 w-full">
            提交申请
          </YellowButton>
        </form>

        <p className="mt-6 text-sm text-muted-foreground">
          已有账号？
          <Link href="/login" className="ml-1 font-semibold text-foreground hover:text-primary">
            登录 →
          </Link>
        </p>
      </div>
    </main>
  );
}

export default function RegisterPage() {
  return (
    <SessionGuard access="public">
      <RegisterForm />
    </SessionGuard>
  );
}
