// PROTOTYPE — throwaway. In-memory seed and stub mutations for the console
// layout prototype (docs/specs/collector-and-user-management.md, Phase 1 seed).
// Nothing persists; a reload restores the seed.
"use client";

import { useCallback, useMemo, useState } from "react";

export type Role = "engineer" | "admin";
export type AccountStatus = "pending" | "active" | "rejected" | "disabled";
export type ReleaseStatus = "pre-release" | "latest" | "deprecated" | "revoked";
export type DbType = "mysql" | "oracle" | "gaussdb";

/** What the prototype bar lets you sign in as. */
export type Persona = "engineer" | "admin" | "pending" | "rejected";
export type Screen = "new-report" | "reports" | "collectors" | "users" | "downloads";

export interface User {
  id: string;
  username: string;
  displayName: string;
  email: string;
  team: string;
  note: string;
  role: Role;
  status: AccountStatus;
  reason?: string;
  registeredAt: string;
  lastAction?: { by: string; at: string; action: string };
}

export interface ReleasePackage {
  platform: string;
  osLabel: string;
  archLabel: string;
  fileName: string;
  size: number;
  sha256: string;
}

export interface Release {
  version: string;
  tag: string;
  commit: string;
  publishedAt: string;
  status: ReleaseStatus;
  revokeReason?: string;
  notes: string;
  dbTypes: DbType[];
  packages: ReleasePackage[];
}

export interface DownloadRecord {
  id: string;
  userId: string;
  version: string;
  platform: string;
  at: string;
}

export interface ReportItem {
  name: string;
  dbType: DbType;
  collectorVersion: string;
  outcome: "success" | "failed";
}

export interface ReportTask {
  id: string;
  submitterId: string;
  createdAt: string;
  status: "success" | "partial" | "failed" | "processing";
  filesExpired: boolean;
  items: ReportItem[];
}

const PLATFORMS = [
  { platform: "linux-amd64", osLabel: "Linux", archLabel: "x86_64" },
  { platform: "linux-arm64", osLabel: "Linux", archLabel: "ARM64" },
  { platform: "windows-amd64", osLabel: "Windows", archLabel: "x86_64" },
  { platform: "darwin-arm64", osLabel: "macOS", archLabel: "ARM64" },
];

function fakeSha(seed: string): string {
  let h = 2166136261;
  let out = "";
  for (let i = 0; out.length < 64; i++) {
    h ^= seed.charCodeAt(i % seed.length) + i;
    h = Math.imul(h, 16777619) >>> 0;
    out += h.toString(16).padStart(8, "0");
  }
  return out.slice(0, 64);
}

function packagesFor(version: string): ReleasePackage[] {
  return PLATFORMS.map((p, i) => ({
    ...p,
    fileName: `db-collector-${version}-${p.platform}.${p.platform.startsWith("windows") ? "zip" : "tar.gz"}`,
    size: 9_400_000 + i * 310_000 + version.length * 1000,
    sha256: fakeSha(`${version}-${p.platform}`),
  }));
}

const SEED_USERS: User[] = [
  { id: "u-admin", username: "admin", displayName: "平台管理员", email: "admin@example.com", team: "DBA 平台组", note: "初始管理员（部署配置）", role: "admin", status: "active", registeredAt: "2026-08-01T09:00:00" },
  { id: "u-zhang", username: "zhangsan", displayName: "张三", email: "zhangsan@example.com", team: "华东交付一部", note: "负责某银行 Oracle 巡检", role: "engineer", status: "active", registeredAt: "2026-08-15T10:12:00", lastAction: { by: "admin", at: "2026-08-15T11:00:00", action: "批准" } },
  { id: "u-li", username: "lisi", displayName: "李四", email: "lisi@example.com", team: "华南交付二部", note: "新入职，需要 GaussDB 巡检", role: "engineer", status: "pending", registeredAt: "2026-09-30T16:40:00" },
  { id: "u-zhao", username: "zhaoliu", displayName: "赵六", email: "zhaoliu@partner.com", team: "外部合作方", note: "协助客户巡检", role: "engineer", status: "rejected", reason: "外部合作方账号需由项目经理邮件确认后再申请", registeredAt: "2026-09-20T14:05:00", lastAction: { by: "admin", at: "2026-09-21T09:30:00", action: "拒绝" } },
  { id: "u-wang", username: "wangwu", displayName: "王五", email: "wangwu@example.com", team: "华北交付部", note: "", role: "engineer", status: "disabled", reason: "已离职", registeredAt: "2026-08-10T08:00:00", lastAction: { by: "admin", at: "2026-09-25T17:00:00", action: "禁用" } },
];

const SEED_RELEASES: Release[] = [
  { version: "1.3.0-rc1", tag: "v1.3.0-rc1", commit: "9f3c2ab", publishedAt: "2026-09-28T20:11:00", status: "pre-release", notes: "- 新增 GaussDB WDR 自动采集\n- 采集超时可配置", dbTypes: ["mysql", "oracle", "gaussdb"], packages: packagesFor("1.3.0-rc1") },
  { version: "1.2.0", tag: "v1.2.0", commit: "5e81d07", publishedAt: "2026-09-10T18:30:00", status: "latest", notes: "- Oracle AWR 采集支持 RAC\n- 修复 MySQL 8.4 权限检查误报", dbTypes: ["mysql", "oracle", "gaussdb"], packages: packagesFor("1.2.0") },
  { version: "1.1.0", tag: "v1.1.0", commit: "c47aa10", publishedAt: "2026-08-20T15:02:00", status: "deprecated", notes: "- 新增 GaussDB 基础采集", dbTypes: ["mysql", "oracle", "gaussdb"], packages: packagesFor("1.1.0") },
  { version: "1.0.0", tag: "v1.0.0", commit: "1b2d3e4", publishedAt: "2026-08-01T12:00:00", status: "revoked", revokeReason: "Oracle 采集会在 11g 上锁表，请勿使用", notes: "- 首个版本：MySQL、Oracle", dbTypes: ["mysql", "oracle"], packages: packagesFor("1.0.0") },
];

const SEED_DOWNLOADS: DownloadRecord[] = [
  { id: "d1", userId: "u-zhang", version: "1.0.0", platform: "linux-amd64", at: "2026-08-02T09:10:00" },
  { id: "d2", userId: "u-wang", version: "1.1.0", platform: "linux-arm64", at: "2026-08-22T14:00:00" },
  { id: "d3", userId: "u-zhang", version: "1.1.0", platform: "linux-amd64", at: "2026-08-25T10:20:00" },
  { id: "d4", userId: "u-zhang", version: "1.2.0", platform: "linux-amd64", at: "2026-09-11T08:45:00" },
  { id: "d5", userId: "u-zhang", version: "1.2.0", platform: "windows-amd64", at: "2026-09-12T16:30:00" },
  { id: "d6", userId: "u-admin", version: "1.3.0-rc1", platform: "darwin-arm64", at: "2026-09-29T09:00:00" },
  { id: "d7", userId: "u-admin", version: "1.2.0", platform: "linux-arm64", at: "2026-09-15T11:11:00" },
];

const SEED_TASKS: ReportTask[] = [
  { id: "T-20260930-0007", submitterId: "u-zhang", createdAt: "2026-09-30T15:20:00", status: "success", filesExpired: false, items: [
    { name: "bank-ora-01.zip", dbType: "oracle", collectorVersion: "1.2.0", outcome: "success" },
    { name: "bank-ora-02.zip", dbType: "oracle", collectorVersion: "1.2.0", outcome: "success" },
  ] },
  { id: "T-20260928-0003", submitterId: "u-zhang", createdAt: "2026-09-28T10:02:00", status: "partial", filesExpired: false, items: [
    { name: "mall-mysql.zip", dbType: "mysql", collectorVersion: "1.1.0", outcome: "success" },
    { name: "legacy-ora.zip", dbType: "oracle", collectorVersion: "1.0.0", outcome: "success" },
    { name: "broken.zip", dbType: "mysql", collectorVersion: "0.9.3-dev", outcome: "failed" },
  ] },
  { id: "T-20260925-0001", submitterId: "u-admin", createdAt: "2026-09-25T09:00:00", status: "success", filesExpired: false, items: [
    { name: "gauss-core.zip", dbType: "gaussdb", collectorVersion: "1.3.0-rc1", outcome: "success" },
  ] },
  { id: "T-20260901-0002", submitterId: "u-wang", createdAt: "2026-08-26T13:45:00", status: "success", filesExpired: true, items: [
    { name: "ops-mysql.zip", dbType: "mysql", collectorVersion: "1.1.0", outcome: "success" },
  ] },
  { id: "T-20260930-0009", submitterId: "u-admin", createdAt: "2026-09-30T17:58:00", status: "processing", filesExpired: false, items: [
    { name: "erp-ora.zip", dbType: "oracle", collectorVersion: "1.2.0", outcome: "success" },
  ] },
];

export const PERSONA_USER: Record<Persona, string> = {
  admin: "u-admin",
  engineer: "u-zhang",
  pending: "u-li",
  rejected: "u-zhao",
};

export const RELEASE_STATUS_LABEL: Record<ReleaseStatus, string> = {
  "pre-release": "预发布",
  latest: "最新",
  deprecated: "已弃用",
  revoked: "已撤回",
};

export const RELEASE_STATUS_CLASS: Record<ReleaseStatus, string> = {
  "pre-release": "bg-accent/15 text-accent",
  latest: "bg-primary/15 text-primary",
  deprecated: "bg-warning/15 text-warning",
  revoked: "bg-destructive/15 text-destructive",
};

export const ACCOUNT_STATUS_LABEL: Record<AccountStatus, string> = {
  pending: "待审批",
  active: "正常",
  rejected: "已拒绝",
  disabled: "已禁用",
};

export const ACCOUNT_STATUS_CLASS: Record<AccountStatus, string> = {
  pending: "bg-warning/15 text-warning",
  active: "bg-primary/15 text-primary",
  rejected: "bg-destructive/15 text-destructive",
  disabled: "bg-muted text-muted-foreground",
};

export const TASK_STATUS_LABEL: Record<ReportTask["status"], string> = {
  success: "已完成",
  partial: "部分成功",
  failed: "执行失败",
  processing: "处理中",
};

export const TASK_STATUS_CLASS: Record<ReportTask["status"], string> = {
  success: "bg-primary/15 text-primary",
  partial: "bg-warning/15 text-warning",
  failed: "bg-destructive/15 text-destructive",
  processing: "bg-accent/15 text-accent",
};

export const SCREEN_LABEL: Record<Screen, string> = {
  "new-report": "生成报告",
  reports: "报告任务",
  collectors: "采集器",
  users: "用户管理",
  downloads: "下载记录",
};

export function screensFor(role: Role): Screen[] {
  return role === "admin"
    ? ["new-report", "reports", "collectors", "users", "downloads"]
    : ["new-report", "reports", "collectors"];
}

export function formatSize(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatTime(iso: string): string {
  return iso.replace("T", " ").slice(0, 16);
}

export function detectPlatform(): string {
  if (typeof navigator === "undefined") return "linux-amd64";
  const ua = navigator.userAgent;
  if (/Windows/i.test(ua)) return "windows-amd64";
  if (/Mac OS X|Macintosh/i.test(ua)) return "darwin-arm64";
  if (/aarch64|arm64/i.test(ua)) return "linux-arm64";
  return "linux-amd64";
}

function now(): string {
  return new Date().toISOString().slice(0, 19);
}

export function useConsoleState(persona: Persona) {
  const [users, setUsers] = useState(SEED_USERS);
  const [releases, setReleases] = useState(SEED_RELEASES);
  const [downloads, setDownloads] = useState(SEED_DOWNLOADS);
  const [tasks] = useState(SEED_TASKS);
  const [lastEvent, setLastEvent] = useState("（种子数据）");

  const me = users.find((u) => u.id === PERSONA_USER[persona])!;
  const userById = useCallback(
    (id: string) => users.find((u) => u.id === id),
    [users],
  );

  function setReleaseStatus(version: string, next: ReleaseStatus, reason?: string) {
    setReleases((rs) =>
      rs.map((r) => {
        if (r.version === version) return { ...r, status: next, revokeReason: next === "revoked" ? reason : undefined };
        if (next === "latest" && r.status === "latest") return { ...r, status: "deprecated" };
        return r;
      }),
    );
  }

  const releaseActions = {
    promote(version: string) {
      setReleaseStatus(version, "latest");
      setLastEvent(`设为最新：${version}（原最新版本转为已弃用）`);
    },
    deprecate(version: string) {
      setReleaseStatus(version, "deprecated");
      setLastEvent(`弃用：${version}`);
    },
    revoke(version: string) {
      const reason = window.prompt(`撤回 ${version} 的原因（必填）`);
      if (!reason?.trim()) return;
      setReleaseStatus(version, "revoked", reason.trim());
      setLastEvent(`撤回：${version}，原因：${reason.trim()}`);
    },
    restore(version: string) {
      setReleaseStatus(version, "deprecated");
      setLastEvent(`恢复：${version} → 已弃用`);
    },
  };

  function download(version: string, platform: string) {
    setDownloads((ds) => [
      { id: `d${ds.length + 1}-${Date.now()}`, userId: me.id, version, platform, at: now() },
      ...ds,
    ]);
    setLastEvent(`${me.displayName} 下载 ${version} / ${platform}（已记录下载记录）`);
  }

  function patchUser(id: string, patch: Partial<User>, action: string) {
    setUsers((us) =>
      us.map((u) =>
        u.id === id ? { ...u, ...patch, lastAction: { by: me.username, at: now(), action } } : u,
      ),
    );
    setLastEvent(`${action}：${userById(id)?.displayName}`);
  }

  const activeAdmins = users.filter((u) => u.role === "admin" && u.status === "active").length;

  const userActions = {
    approve: (id: string) => patchUser(id, { status: "active", reason: undefined }, "批准"),
    reject(id: string) {
      const reason = window.prompt("拒绝原因（必填，将展示给申请人）");
      if (reason?.trim()) patchUser(id, { status: "rejected", reason: reason.trim() }, "拒绝");
    },
    disable(id: string) {
      const u = userById(id)!;
      if (id === me.id) return setLastEvent("不能禁用自己");
      if (u.role === "admin" && activeAdmins <= 1) return setLastEvent("平台至少保留一名正常状态的管理员");
      const reason = window.prompt("禁用原因（必填）");
      if (reason?.trim()) patchUser(id, { status: "disabled", reason: reason.trim() }, "禁用");
    },
    enable: (id: string) => patchUser(id, { status: "active", reason: undefined }, "启用"),
    toggleRole(id: string) {
      const u = userById(id)!;
      if (u.role === "admin") {
        if (id === me.id) return setLastEvent("不能降级自己");
        if (u.status === "active" && activeAdmins <= 1) return setLastEvent("平台至少保留一名正常状态的管理员");
        patchUser(id, { role: "engineer" }, "降为普通用户");
      } else {
        patchUser(id, { role: "admin" }, "升为管理员");
      }
    },
    resetPassword(id: string) {
      const temp = Math.random().toString(36).slice(2, 10);
      patchUser(id, {}, "重置密码");
      window.alert(`临时密码：${temp}\n请线下交给用户，下次登录时必须修改。`);
    },
    resubmit: (id: string) => patchUser(id, { status: "pending", reason: undefined }, "重新提交申请"),
  };

  const pendingCount = users.filter((u) => u.status === "pending").length;
  const latest = releases.find((r) => r.status === "latest");

  const visibleTasks = useMemo(
    () => (me.role === "admin" ? tasks : tasks.filter((t) => t.submitterId === me.id)),
    [me, tasks],
  );

  function versionNotice(version: string): { tone: "warn" | "danger"; text: string } | null {
    const r = releases.find((x) => x.version === version);
    if (!r) return null;
    if (r.status === "deprecated") return { tone: "warn", text: `采集器 ${version} 已弃用，建议升级到最新版本` };
    if (r.status === "revoked") return { tone: "danger", text: `采集器 ${version} 已撤回：${r.revokeReason}` };
    return null;
  }

  return {
    me,
    users,
    releases,
    downloads,
    tasks: visibleTasks,
    latest,
    pendingCount,
    lastEvent,
    userById,
    releaseActions,
    userActions,
    download,
    versionNotice,
  };
}

export type ConsoleState = ReturnType<typeof useConsoleState>;

/** Release status actions available from the given status (spec transition table). */
export function releaseActionsFor(status: ReleaseStatus): ("promote" | "deprecate" | "revoke" | "restore")[] {
  switch (status) {
    case "pre-release":
      return ["promote", "deprecate", "revoke"];
    case "latest":
      return ["deprecate", "revoke"];
    case "deprecated":
      return ["promote", "revoke"];
    case "revoked":
      return ["restore"];
  }
}

export const RELEASE_ACTION_LABEL = {
  promote: "设为最新",
  deprecate: "弃用",
  revoke: "撤回",
  restore: "恢复",
} as const;
