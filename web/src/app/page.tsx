"use client";

import { useEffect } from "react";
import {
  Database,
  FileBarChart,
  Wrench,
  ClipboardList,
  Shield,
  User,
  LogOut,
  ChevronDown,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";
import { useNavStore, type NavTab } from "@/stores/nav-store";
import { LoginPage } from "@/components/login-page";
import { ReportPage } from "@/components/report-page";
import { ToolsPage } from "@/components/tools-page";
import { HistoryPage } from "@/components/history-page";
import { useState } from "react";

const NAV_TABS: { id: NavTab; label: string; icon: React.ReactNode }[] = [
  {
    id: "report",
    label: "报告生成",
    icon: <FileBarChart className="h-4 w-4" />,
  },
  {
    id: "tools",
    label: "巡检工具",
    icon: <Wrench className="h-4 w-4" />,
  },
  {
    id: "history",
    label: "任务记录",
    icon: <ClipboardList className="h-4 w-4" />,
  },
];

function UserMenu() {
  const user = useAuthStore((s) => s.user);
  const logout = useAuthStore((s) => s.logout);
  const quickLogin = useAuthStore((s) => s.quickLogin);
  const [open, setOpen] = useState(false);

  if (!user) return null;

  const isAdmin = user.role === "admin";

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 rounded-lg px-3 py-1.5 text-sm hover:bg-muted transition-colors cursor-pointer"
      >
        <span
          className={cn(
            "flex h-6 w-6 items-center justify-center rounded-full text-xs font-bold",
            isAdmin
              ? "bg-primary/15 text-primary"
              : "bg-accent/15 text-accent",
          )}
        >
          {isAdmin ? (
            <Shield className="h-3 w-3" />
          ) : (
            <User className="h-3 w-3" />
          )}
        </span>
        <span className="hidden sm:inline text-sm font-medium">
          {user.displayName}
        </span>
        <ChevronDown className="h-3 w-3 text-muted-foreground" />
      </button>

      {open && (
        <>
          {/* Backdrop */}
          <div
            className="fixed inset-0 z-40"
            onClick={() => setOpen(false)}
          />
          {/* Dropdown */}
          <div className="absolute right-0 top-full z-50 mt-1 w-48 rounded-xl border border-border bg-card p-1 shadow-lg">
            <div className="px-3 py-2 border-b border-border">
              <p className="text-sm font-medium">{user.displayName}</p>
              <p className="text-xs text-muted-foreground">
                {isAdmin ? "管理员" : "普通用户"}
              </p>
            </div>
            <button
              type="button"
              onClick={() => {
                quickLogin(isAdmin ? "user" : "admin");
                setOpen(false);
              }}
              className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm hover:bg-muted transition-colors cursor-pointer mt-1"
            >
              {isAdmin ? (
                <User className="h-3.5 w-3.5" />
              ) : (
                <Shield className="h-3.5 w-3.5" />
              )}
              切换为{isAdmin ? "普通用户" : "管理员"}
            </button>
            <button
              type="button"
              onClick={() => {
                logout();
                setOpen(false);
              }}
              className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-destructive hover:bg-destructive/10 transition-colors cursor-pointer"
            >
              <LogOut className="h-3.5 w-3.5" />
              退出登录
            </button>
          </div>
        </>
      )}
    </div>
  );
}

export default function Home() {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const hydrate = useAuthStore((s) => s.hydrate);
  const activeTab = useNavStore((s) => s.activeTab);
  const setActiveTab = useNavStore((s) => s.setActiveTab);

  useEffect(() => {
    hydrate();
  }, [hydrate]);

  if (!isAuthenticated) {
    return (
      <main className="mx-auto max-w-4xl px-4 py-8">
        <LoginPage />
      </main>
    );
  }

  return (
    <>
      {/* Topbar */}
      <nav className="fixed top-0 left-0 right-0 z-50 border-b border-border bg-card/80 backdrop-blur-md">
        <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4">
          {/* Left: Brand */}
          <div className="flex items-center gap-2.5">
            <Database className="h-5 w-5 text-primary" />
            <span className="text-sm font-bold tracking-tight">DB-Check</span>
          </div>

          {/* Center: Tabs */}
          <div className="flex items-center gap-1">
            {NAV_TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "relative flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium",
                  "transition-colors duration-200 cursor-pointer",
                  activeTab === tab.id
                    ? "text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50",
                )}
              >
                {tab.icon}
                <span className="hidden sm:inline">{tab.label}</span>
                {activeTab === tab.id && (
                  <span className="nav-active-indicator absolute -bottom-[calc(0.5rem+1px)] left-2 right-2 h-0.5 rounded-full bg-primary" />
                )}
              </button>
            ))}
          </div>

          {/* Right: Mock badge + User */}
          <div className="flex items-center gap-3">
            <span className="rounded-md bg-warning/10 px-2 py-0.5 text-[10px] font-semibold text-warning">
              Mock
            </span>
            <UserMenu />
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main
        className={cn(
          "mx-auto px-4 pt-20 pb-16",
          activeTab === "report" ? "max-w-4xl" : "max-w-6xl",
        )}
      >
        {activeTab === "report" && <ReportPage />}
        {activeTab === "tools" && <ToolsPage />}
        {activeTab === "history" && <HistoryPage />}
      </main>
    </>
  );
}
