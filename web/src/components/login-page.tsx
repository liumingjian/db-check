"use client";

import { useState } from "react";
import { Database, Shield, User } from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

export function LoginPage() {
  const login = useAuthStore((s) => s.login);
  const quickLogin = useAuthStore((s) => s.quickLogin);

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  function handleLogin() {
    setError("");
    if (!username.trim() || !password.trim()) {
      setError("请输入用户名和密码");
      return;
    }
    const ok = login(username.trim(), password.trim());
    if (!ok) setError("用户名或密码错误");
  }

  return (
    <div className="flex min-h-[80vh] items-center justify-center">
      <div className="w-full max-w-sm space-y-8">
        {/* Brand */}
        <div className="flex flex-col items-center gap-3">
          <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-primary/20">
            <Database className="h-7 w-7 text-primary" />
          </div>
          <div className="text-center">
            <h1 className="text-2xl font-bold tracking-tight">DB-Check</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              数据库巡检平台
            </p>
          </div>
        </div>

        {/* Login form */}
        <div className="space-y-4 rounded-xl border border-border bg-card p-6">
          <div className="space-y-2">
            <label
              htmlFor="login-username"
              className="text-sm font-medium"
            >
              用户名
            </label>
            <input
              id="login-username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleLogin()}
              placeholder="输入用户名"
              className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none transition-colors focus:border-primary focus:ring-1 focus:ring-primary/30"
            />
          </div>
          <div className="space-y-2">
            <label
              htmlFor="login-password"
              className="text-sm font-medium"
            >
              密码
            </label>
            <input
              id="login-password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleLogin()}
              placeholder="输入密码"
              className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none transition-colors focus:border-primary focus:ring-1 focus:ring-primary/30"
            />
          </div>

          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}

          <button
            type="button"
            onClick={handleLogin}
            className={cn(
              "w-full rounded-lg px-4 py-2.5 text-sm font-medium",
              "bg-primary text-primary-foreground",
              "hover:bg-primary/90 transition-colors duration-200",
              "cursor-pointer",
            )}
          >
            登录
          </button>
        </div>

        {/* Quick login */}
        <div className="space-y-3">
          <div className="flex items-center gap-3">
            <div className="h-px flex-1 bg-border" />
            <span className="text-xs text-muted-foreground">快速进入</span>
            <div className="h-px flex-1 bg-border" />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <button
              type="button"
              onClick={() => quickLogin("admin")}
              className={cn(
                "flex items-center justify-center gap-2 rounded-lg border border-border px-4 py-2.5 text-sm font-medium",
                "hover:bg-primary/10 hover:border-primary/40 transition-colors duration-200",
                "cursor-pointer",
              )}
            >
              <Shield className="h-4 w-4 text-primary" />
              管理员
            </button>
            <button
              type="button"
              onClick={() => quickLogin("user")}
              className={cn(
                "flex items-center justify-center gap-2 rounded-lg border border-border px-4 py-2.5 text-sm font-medium",
                "hover:bg-accent/10 hover:border-accent/40 transition-colors duration-200",
                "cursor-pointer",
              )}
            >
              <User className="h-4 w-4 text-accent" />
              普通用户
            </button>
          </div>

          <p className="text-center text-xs text-muted-foreground">
            当前为 Mock 数据模式，快速进入使用预设账号
          </p>
        </div>
      </div>
    </div>
  );
}
