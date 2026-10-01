"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowDown } from "lucide-react";
import { downloadRecordsHref } from "@/components/console/admin/download-records";
import { ConsoleSection } from "@/components/console/console-shell";
import { useDialogs } from "@/components/console/dialog-host";
import { CAPTION, Chip, CopyText, EASE_OUT, Menu, type MenuItem } from "@/components/console/kit";
import {
  api,
  ApiError,
  type CollectorRelease,
  type ReleaseDbType,
  type ReleasePackage,
  type ReleaseStatus,
} from "@/lib/api";
import { releaseActionsFor, type ReleaseAction } from "@/lib/api/releases/contract";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

const DB_LABEL: Record<ReleaseDbType, string> = { mysql: "MySQL", oracle: "Oracle", gaussdb: "GaussDB" };

/** The QUICKSTART commands shipped in each package (`scripts/build_release_packages.sh`). */
const USAGE: Record<ReleaseDbType, string> = {
  mysql: "./db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password '***' --dbname dbcheck",
  oracle: "./db-collector --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password '***' --dbname ORCL",
  gaussdb: "./db-collector --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password '***' --dbname postgres",
};

const STEPS = ["上传到客户数据库主机并解压", "运行右侧的采集命令", "把 ZIP 拖回本页顶部"];

const HISTORY_STATUS: Record<Exclude<ReleaseStatus, "latest">, { label: string; className: string }> = {
  deprecated: { label: "已弃用 · 建议使用最新版本", className: "text-warning" },
  "pre-release": { label: "预发布 · 仅管理员可见", className: "text-muted-foreground" },
  revoked: { label: "已撤回", className: "text-destructive" },
};

const ACTION_LABEL: Record<ReleaseAction, string> = {
  promote: "设为最新",
  deprecate: "弃用",
  revoke: "撤回",
  restore: "恢复",
};

/** 采集器: the latest release as four equal tiles, usage guide, and older releases collapsed (spec #19). */
export function CollectorsSection() {
  const token = useAuthStore((s) => s.token);
  const isAdmin = useAuthStore((s) => s.user?.role === "admin");
  const { toast, ask } = useDialogs();
  const router = useRouter();
  const [releases,setReleases] = useState<CollectorRelease[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Bumped after a status change; the list lives here, so it must be re-fetched.
  const [reloads, setReloads] = useState(0);

  useEffect(() => {
    if (!token) return;
    api.releases.list(token).then(setReleases, (e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [token, reloads]);

  async function changeStatus(release: CollectorRelease, action: ReleaseAction, reason = "") {
    if (!token) return;
    const { version } = release;
    try {
      await (action === "revoke" ? api.releases.revoke(token, version, reason) : api.releases[action](token, version));
      toast(`v${version} 已${ACTION_LABEL[action]}`);
    } catch (e) {
      toast(e instanceof ApiError ? e.message : `操作失败：${String(e)}`);
    }
    setReloads((n) => n + 1);
  }

  function releaseMenu(release: CollectorRelease): MenuItem[] {
    if (!isAdmin) return [];
    const actions: MenuItem[] = releaseActionsFor(release.status).map((action) => ({
      label: action === "revoke" ? "撤回…" : ACTION_LABEL[action],
      danger: action === "revoke",
      onSelect: () => {
        if (action === "revoke") {
          ask({
            title: `撤回 v${release.version}`,
            body: "撤回后工程师看不到也下载不了这个版本。",
            input: "撤回原因（必填）",
            confirm: "撤回",
            danger: true,
            onConfirm: (reason) => void changeStatus(release, action, reason),
          });
        } else if (action === "deprecate" && release.status === "latest") {
          ask({
            title: `弃用 v${release.version}`,
            body: "这是当前最新版本。弃用后平台暂无推荐版本，直到另一个版本被设为最新。",
            confirm: "弃用",
            danger: true,
            onConfirm: () => void changeStatus(release, action),
          });
        } else {
          void changeStatus(release, action);
        }
      },
    }));
    return [...actions, { label: "查看下载记录", onSelect: () => router.push(downloadRecordsHref({ release: release.version })) }];
  }

  async function download(release: CollectorRelease, pkg: ReleasePackage) {
    if (!token) return;
    try {
      saveBlob(await api.downloads.download(token, release.version, pkg.platform), pkg.fileName);
      toast(`正在下载 ${pkg.fileName}`);
    } catch (e) {
      toast(e instanceof ApiError ? e.message : `下载失败：${String(e)}`);
    }
  }

  const latest = releases?.find((r) => r.status === "latest");
  const older = releases?.filter((r) => r !== latest) ?? [];

  return (
    <ConsoleSection id="collectors">
      <div className="mx-auto max-w-[1240px] px-8 py-24">
        <p className={CAPTION}>Collector</p>
        <div className="mt-4 flex items-center gap-4">
          <h2 className="text-[56px] leading-[1.1] font-bold tracking-[-2px]">
            下载采集器 {latest && <span className="text-primary">v{latest.version}</span>}
          </h2>
          {latest && <Menu items={releaseMenu(latest)} label={`v${latest.version} 的操作`} />}
        </div>
        <p className="mt-4 text-lg text-[#ccc]">按客户主机的系统和架构选择，四个包功能完全一致。</p>
        {error && <p className="mt-8 text-sm text-destructive">{error}</p>}

        {latest ? (
          <>
            <LatestRelease release={latest} onDownload={(pkg) => void download(latest, pkg)} />
            <UsageGuide dbTypes={latest.dbTypes} />
          </>
        ) : (
          releases && <NoLatest hasOlder={older.length > 0} />
        )}
        {older.length > 0 && (
          <OlderReleases
            releases={older}
            defaultOpen={!latest}
            menuFor={releaseMenu}
            onDownload={(r, pkg) => void download(r, pkg)}
          />
        )}
      </div>
    </ConsoleSection>
  );
}

function LatestRelease({ release, onDownload }: { release: CollectorRelease; onDownload: (pkg: ReleasePackage) => void }) {
  return (
    <>
      <div className="mt-12 grid grid-cols-4 gap-4">
        {release.packages.map((pkg) => (
          <button
            key={pkg.platform}
            type="button"
            onClick={() => onDownload(pkg)}
            className={cn(
              "group flex h-60 cursor-pointer flex-col justify-between rounded-2xl bg-card p-6 text-left",
              `transition-[background-color,color,transform] duration-150 ${EASE_OUT} hover:bg-primary hover:text-primary-foreground active:scale-[0.98]`,
            )}
          >
            <span className="text-sm font-semibold text-muted-foreground group-hover:text-primary-foreground/60">{pkg.os}</span>
            <span className="text-[40px] leading-none font-bold tracking-[-1.5px]">{pkg.arch}</span>
            <span className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground tabular-nums group-hover:text-primary-foreground/60">
                {formatSize(pkg.size)} · .zip
              </span>
              <span className="flex items-center gap-1 font-semibold">
                下载 <ArrowDown className="h-4 w-4" />
              </span>
            </span>
          </button>
        ))}
      </div>
      <div className="mt-4 flex flex-wrap gap-x-8 gap-y-1">
        {release.packages.map((pkg) => (
          <CopyText
            key={pkg.platform}
            text={pkg.sha256}
            label={`${pkg.os} ${pkg.arch} sha256 ${pkg.sha256.slice(0, 12)}`}
            className="text-[#5a5a5a]"
          />
        ))}
      </div>
      <div className="mt-12 grid grid-cols-[1fr_1.6fr] gap-16 text-sm">
        <div className="space-y-2 text-muted-foreground">
          <p>
            发布于 {formatDate(release.publishedAt)} · <span className="font-mono">{release.tag}</span> ·{" "}
            <span className="font-mono">{release.commit}</span>
          </p>
          <p>支持数据库：{release.dbTypes.map((t) => DB_LABEL[t]).join(" / ")}</p>
        </div>
        <ReleaseNotes notes={release.notes} />
      </div>
    </>
  );
}

function ReleaseNotes({ notes }: { notes: string }) {
  const lines = notes
    .split("\n")
    .map((l) => l.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
  return (
    <ul className="space-y-1.5 text-[#ccc]">
      {lines.map((line) => (
        <li key={line} className="flex gap-3">
          <span className="text-primary">—</span>
          {line}
        </li>
      ))}
    </ul>
  );
}

function UsageGuide({ dbTypes }: { dbTypes: ReleaseDbType[] }) {
  const [picked, setPicked] = useState<ReleaseDbType | null>(null);
  // A pick the shown release doesn't support falls back to its first type.
  const db = picked && dbTypes.includes(picked) ? picked : dbTypes[0];
  if (!db) return null;
  return (
    <div className="mt-20 grid grid-cols-[1fr_1.6fr] gap-16">
      <div>
        <h3 className="text-2xl font-bold tracking-[-0.3px]">三步用起来</h3>
        <ol className="mt-6 space-y-5">
          {STEPS.map((step, i) => (
            <li key={step} className="flex items-baseline gap-4">
              <span className="text-[32px] leading-none font-bold tracking-[-1px] text-primary">{i + 1}</span>
              <span className="text-base text-[#ccc]">{step}</span>
            </li>
          ))}
        </ol>
      </div>
      <div className="overflow-hidden rounded-2xl bg-card">
        <div className="flex gap-2 px-4 pt-4">
          {dbTypes.map((t) => (
            <Chip key={t} on={db === t} onClick={() => setPicked(t)}>
              {DB_LABEL[t]}
            </Chip>
          ))}
        </div>
        <div className="flex items-start gap-4 p-5">
          <code className="flex-1 font-mono text-sm leading-7 break-all text-[#e6e6e6]">{USAGE[db]}</code>
          <CopyText text={USAGE[db]} label="" />
        </div>
        <p className="px-5 pb-5 text-xs text-muted-foreground">Windows 上运行 db-collector.exe，参数相同。</p>
      </div>
    </div>
  );
}

/** In place of the latest release while an admin has deprecated or revoked it and promoted none. */
function NoLatest({ hasOlder }: { hasOlder: boolean }) {
  return (
    <div className="mt-12 rounded-2xl bg-card p-10">
      <p className="text-[32px] leading-tight font-bold tracking-[-1px]">暂无推荐版本，请联系管理员</p>
      {hasOlder && <p className="mt-3 text-sm text-muted-foreground">下方历史版本仍可下载。</p>}
    </div>
  );
}

function OlderReleases({
  releases,
  defaultOpen,
  menuFor,
  onDownload,
}: {
  releases: CollectorRelease[];
  defaultOpen: boolean;
  menuFor: (release: CollectorRelease) => MenuItem[];
  onDownload: (release: CollectorRelease, pkg: ReleasePackage) => void;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="mt-16">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className="cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground"
      >
        历史版本 {open ? "↑" : "↓"}
      </button>
      {open && (
        <div className="mt-4 divide-y divide-border border-y border-border">
          {releases.map((r) => {
            const status = r.status === "latest" ? null : HISTORY_STATUS[r.status];
            return (
              <div key={r.version} className="flex items-center gap-6 py-3 text-sm">
                <span className="w-28 font-semibold tabular-nums">v{r.version}</span>
                <span className={cn("w-64 truncate text-xs", status?.className)} title={r.revokeReason}>
                  {status?.label}
                  {r.revokeReason && ` · ${r.revokeReason}`}
                </span>
                <span className="flex flex-1 gap-4 text-xs">
                  {r.packages.map((pkg) => (
                    <button
                      key={pkg.platform}
                      type="button"
                      onClick={() => onDownload(r, pkg)}
                      className="cursor-pointer text-muted-foreground hover:text-foreground"
                    >
                      {pkg.os} {pkg.arch}
                    </button>
                  ))}
                </span>
                <Menu items={menuFor(r)} label={`v${r.version} 的操作`} />
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function formatSize(bytes: number): string {
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("zh-CN");
}

function saveBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}
