"use client";

import { useEffect, useState } from "react";
import {
  Download,
  Package,
  ChevronDown,
  ChevronUp,
  Plus,
  Trash2,
  Monitor,
  HardDrive,
  ArrowRight,
  ShieldCheck,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useToolStore } from "@/stores/tool-store";
import { useAuthStore } from "@/stores/auth-store";
import { useNavStore } from "@/stores/nav-store";
import { PLATFORMS, type Platform, type ToolRelease } from "@/lib/tool-types";

const PLATFORM_ICONS: Record<Platform, React.ReactNode> = {
  "linux-amd64": <Monitor className="h-4 w-4" />,
  "linux-arm64": <Monitor className="h-4 w-4" />,
  "windows-amd64": <HardDrive className="h-4 w-4" />,
};

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleDateString("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
  } catch {
    return iso;
  }
}

/* ── Release card ── */

function ReleaseCard({
  release,
  platform,
  isAdmin,
  onRemove,
}: {
  release: ToolRelease;
  platform: Platform;
  isAdmin: boolean;
  onRemove: (id: string) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const pkg = release.packages.find((p) => p.platform === platform);
  if (!pkg) return null;

  return (
    <div
      className={cn(
        "rounded-xl border bg-card p-5 transition-colors duration-200",
        release.isLatest
          ? "border-primary/30 ring-1 ring-primary/10"
          : "border-border",
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div
            className={cn(
              "flex h-10 w-10 items-center justify-center rounded-lg",
              release.isLatest ? "bg-primary/10" : "bg-muted",
            )}
          >
            <Package
              className={cn(
                "h-5 w-5",
                release.isLatest ? "text-primary" : "text-muted-foreground",
              )}
            />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold">{release.version}</span>
              {release.isLatest && (
                <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[10px] font-semibold text-primary">
                  Latest
                </span>
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              {formatDate(release.releasedAt)}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {isAdmin && (
            <button
              type="button"
              onClick={() => onRemove(release.id)}
              className="rounded-md p-1.5 text-muted-foreground hover:text-destructive hover:bg-destructive/10 cursor-pointer transition-colors"
              aria-label="下架版本"
            >
              <Trash2 className="h-4 w-4" />
            </button>
          )}
          <button
            type="button"
            onClick={() => {
              /* Mock download — in production this would trigger a real download. */
            }}
            className={cn(
              "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium",
              "bg-primary text-primary-foreground",
              "hover:bg-primary/90 transition-colors duration-200 cursor-pointer",
            )}
          >
            <Download className="h-3.5 w-3.5" />
            下载
          </button>
        </div>
      </div>

      {/* File info */}
      <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <span>{pkg.fileName}</span>
        <span>{formatSize(pkg.fileSize)}</span>
        <span className="font-mono">MD5: {pkg.md5.slice(0, 12)}…</span>
      </div>

      {/* Changelog toggle */}
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="mt-3 inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground cursor-pointer transition-colors"
      >
        {expanded ? (
          <ChevronUp className="h-3 w-3" />
        ) : (
          <ChevronDown className="h-3 w-3" />
        )}
        更新日志
      </button>
      {expanded && (
        <pre className="mt-2 whitespace-pre-wrap rounded-md bg-muted/50 p-3 font-mono text-xs text-muted-foreground">
          {release.changelog}
        </pre>
      )}
    </div>
  );
}

/* ── Publish form ── */

function PublishForm({
  onPublish,
  isPublishing,
}: {
  onPublish: (version: string, changelog: string) => void;
  isPublishing: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [version, setVersion] = useState("");
  const [changelog, setChangelog] = useState("");

  function handleSubmit() {
    if (!version.trim()) return;
    onPublish(version.trim(), changelog.trim());
    setVersion("");
    setChangelog("");
    setOpen(false);
  }

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={cn(
          "inline-flex items-center gap-2 rounded-lg border border-dashed border-border px-4 py-2 text-sm font-medium",
          "hover:border-primary/50 hover:bg-primary/5 transition-colors duration-200 cursor-pointer",
        )}
      >
        <Plus className="h-4 w-4" />
        发布新版本
      </button>
    );
  }

  return (
    <div className="rounded-xl border border-primary/30 bg-card p-5 space-y-4">
      <h3 className="text-sm font-semibold">发布新版本</h3>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div className="space-y-1">
          <label htmlFor="pub-version" className="text-xs font-medium text-muted-foreground">
            版本号
          </label>
          <input
            id="pub-version"
            type="text"
            value={version}
            onChange={(e) => setVersion(e.target.value)}
            placeholder="v1.3.0"
            className="w-full rounded-md border border-border bg-background px-3 py-1.5 text-sm outline-none focus:border-primary"
          />
        </div>
      </div>
      <div className="space-y-1">
        <label htmlFor="pub-changelog" className="text-xs font-medium text-muted-foreground">
          更新日志
        </label>
        <textarea
          id="pub-changelog"
          rows={3}
          value={changelog}
          onChange={(e) => setChangelog(e.target.value)}
          placeholder="- 新增 xxx 功能&#10;- 修复 xxx 问题"
          className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm outline-none resize-none focus:border-primary"
        />
      </div>
      <div className="flex gap-2">
        <button
          type="button"
          onClick={handleSubmit}
          disabled={isPublishing || !version.trim()}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-lg px-4 py-2 text-sm font-medium",
            "bg-primary text-primary-foreground hover:bg-primary/90",
            "transition-colors duration-200 cursor-pointer",
            "disabled:opacity-50 disabled:cursor-not-allowed",
          )}
        >
          {isPublishing ? "发布中…" : "确认发布"}
        </button>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="rounded-lg border border-border px-4 py-2 text-sm font-medium hover:bg-muted transition-colors cursor-pointer"
        >
          取消
        </button>
      </div>
    </div>
  );
}

/* ── Main page ── */

export function ToolsPage() {
  const releases = useToolStore((s) => s.releases);
  const selectedPlatform = useToolStore((s) => s.selectedPlatform);
  const isPublishing = useToolStore((s) => s.isPublishing);
  const loadReleases = useToolStore((s) => s.loadReleases);
  const setSelectedPlatform = useToolStore((s) => s.setSelectedPlatform);
  const publishRelease = useToolStore((s) => s.publishRelease);
  const removeRelease = useToolStore((s) => s.removeRelease);

  const user = useAuthStore((s) => s.user);
  const isAdmin = user?.role === "admin";

  useEffect(() => {
    loadReleases();
  }, [loadReleases]);

  return (
    <div className="flex flex-col gap-8">
      {/* Header */}
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold tracking-tight">巡检工具下载</h1>
          <span className="inline-flex items-center gap-1 rounded-full bg-primary/10 px-2.5 py-0.5 text-xs font-semibold text-primary">
            <ShieldCheck className="h-3.5 w-3.5" />
            统一全引擎采集工具
          </span>
        </div>
        <p className="text-sm text-muted-foreground leading-relaxed">
          单二进制轻量采集器（db-collector），支持在客户内网直接运行。单一工具内置 MySQL、Oracle、GaussDB、PostgreSQL、达梦全引擎驱动，执行 <code className="text-foreground font-mono text-xs bg-muted px-1.5 py-0.5 rounded">./db-collector --help</code> 即可查看完整命令参数。
        </p>
      </div>

      {/* Admin: Publish */}
      {isAdmin && (
        <PublishForm onPublish={publishRelease} isPublishing={isPublishing} />
      )}

      {/* Platform tabs */}
      <div className="flex flex-wrap gap-2">
        {PLATFORMS.map((p) => (
          <button
            key={p.platform}
            type="button"
            onClick={() => setSelectedPlatform(p.platform)}
            className={cn(
              "inline-flex flex-col sm:flex-row items-start sm:items-center gap-1.5 sm:gap-2 rounded-lg px-4 py-2.5 text-sm font-medium",
              "border transition-all duration-200 cursor-pointer",
              selectedPlatform === p.platform
                ? "border-primary bg-primary/10 text-primary"
                : "border-border hover:border-primary/40 hover:bg-primary/5 text-muted-foreground hover:text-foreground",
            )}
          >
            <div className="flex items-center gap-2">
              {PLATFORM_ICONS[p.platform]}
              <span>{p.osLabel} {p.archLabel}</span>
            </div>
            <span className="text-[11px] opacity-75 font-normal">
              ({p.description})
            </span>
          </button>
        ))}
      </div>

      {/* Release list */}
      <div className="space-y-4">
        {releases.map((r) => (
          <ReleaseCard
            key={r.id}
            release={r}
            platform={selectedPlatform}
            isAdmin={isAdmin}
            onRemove={removeRelease}
          />
        ))}
        {releases.length === 0 && (
          <p className="py-12 text-center text-sm text-muted-foreground">
            暂无发布版本
          </p>
        )}
      </div>

      {/* Two-Phase Workflow Stepper & CTA */}
      <div className="rounded-xl border border-border bg-card p-6 space-y-6">
        <div className="space-y-1">
          <h2 className="text-base font-semibold">两阶段离线巡检工作流程</h2>
          <p className="text-xs text-muted-foreground">
            DB-Check 采用采集与分析解耦架构，保障客户内网生产数据不出网与低侵入运行
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="rounded-lg border border-border/60 bg-muted/30 p-4 space-y-2">
            <div className="flex items-center gap-2 text-primary font-medium text-sm">
              <span className="flex h-6 w-6 items-center justify-center rounded-full bg-primary/10 text-xs font-bold">1</span>
              <span>下载介质</span>
            </div>
            <p className="text-xs text-muted-foreground leading-relaxed">
              在本页面选择与客户主机系统匹配的架构包（Linux x86_64、ARM64 或 Windows），离线传输至客户网络。
            </p>
          </div>

          <div className="rounded-lg border border-border/60 bg-muted/30 p-4 space-y-2">
            <div className="flex items-center gap-2 text-primary font-medium text-sm">
              <span className="flex h-6 w-6 items-center justify-center rounded-full bg-primary/10 text-xs font-bold">2</span>
              <span>内网主机执行采集</span>
            </div>
            <p className="text-xs text-muted-foreground leading-relaxed">
              单二进制无需安装额外依赖。目标主机解压后执行 <code className="text-primary/90 font-mono">./db-collector --help</code> 获取参数并执行，生成指标 ZIP 包。
            </p>
          </div>

          <div className="rounded-lg border border-border/60 bg-muted/30 p-4 space-y-2">
            <div className="flex items-center gap-2 text-primary font-medium text-sm">
              <span className="flex h-6 w-6 items-center justify-center rounded-full bg-primary/10 text-xs font-bold">3</span>
              <span>上传并产出报告</span>
            </div>
            <p className="text-xs text-muted-foreground leading-relaxed">
              收集各数据库主机的指标压缩包，返回平台直接上传。引擎全自动探测数据库类型并产出综合巡检报告。
            </p>
          </div>
        </div>

        <div className="flex flex-col sm:flex-row items-center justify-between gap-4 pt-2 border-t border-border/60">
          <div className="text-xs text-muted-foreground">
            提示：采集工具内置 MySQL、Oracle、GaussDB、PostgreSQL、达梦全引擎驱动，单工具支持全量类型。
          </div>
          <button
            type="button"
            onClick={() => useNavStore.getState().setActiveTab("report")}
            className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors cursor-pointer shrink-0"
          >
            已有采集数据？前往生成报告
            <ArrowRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
