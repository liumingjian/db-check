"use client";

import { useRouter } from "next/navigation";
import { downloadRecordsHref } from "@/components/console/admin/download-records";
import { useDialogs, type AskOptions } from "@/components/console/dialog-host";
import type { MenuItem } from "@/components/console/kit";
import { api, ApiError, errorMessage, type CollectorRelease } from "@/lib/api";
import { releaseActionsFor, type ReleaseAction } from "@/lib/api/releases/contract";
import { useAuthStore } from "@/stores/auth-store";

const ACTION_LABEL: Record<ReleaseAction, string> = {
  promote: "设为最新",
  deprecate: "弃用",
  revoke: "撤回",
  restore: "恢复",
};

/** The confirmation an action asks for first, or `null` when it runs at once. */
function confirmationFor(release: CollectorRelease, action: ReleaseAction): Omit<AskOptions, "onConfirm"> | null {
  if (action === "revoke") {
    return {
      title: `撤回 v${release.version}`,
      body: "撤回后工程师看不到也下载不了这个版本。",
      input: "撤回原因（必填）",
      confirm: "撤回",
      danger: true,
    };
  }
  if (action === "deprecate" && release.status === "latest") {
    return {
      title: `弃用 v${release.version}`,
      body: "这是当前最新版本。弃用后平台暂无推荐版本，直到另一个版本被设为最新。",
      confirm: "弃用",
      danger: true,
    };
  }
  return null;
}

/**
 * The `···` menu of a release: admins get its status actions and 查看下载记录;
 * engineers get none. `onChanged` runs after every status action, so the
 * caller re-fetches the list.
 */
export function useReleaseMenu(onChanged: () => void): (release: CollectorRelease) => MenuItem[] {
  const token = useAuthStore((s) => s.token);
  const isAdmin = useAuthStore((s) => s.user?.role === "admin");
  const { toast, ask } = useDialogs();
  const router = useRouter();

  async function changeStatus(release: CollectorRelease, action: ReleaseAction, reason = "") {
    if (!token) return;
    const { version } = release;
    try {
      await (action === "revoke" ? api.releases.revoke(token, version, reason) : api.releases[action](token, version));
      toast(`v${version} 已${ACTION_LABEL[action]}`);
    } catch (e) {
      toast(e instanceof ApiError ? e.message : `操作失败：${errorMessage(e)}`);
    }
    onChanged();
  }

  function select(release: CollectorRelease, action: ReleaseAction) {
    const confirmation = confirmationFor(release, action);
    if (!confirmation) void changeStatus(release, action);
    else ask({ ...confirmation, onConfirm: (reason) => void changeStatus(release, action, reason) });
  }

  return (release) => {
    if (!isAdmin) return [];
    const actions: MenuItem[] = releaseActionsFor(release.status).map((action) => ({
      label: action === "revoke" ? "撤回…" : ACTION_LABEL[action],
      danger: action === "revoke",
      onSelect: () => select(release, action),
    }));
    return [...actions, { label: "查看下载记录", onSelect: () => router.push(downloadRecordsHref({ release: release.version })) }];
  };
}
