"use client";

import { useEffect, useState } from "react";
import { ConsoleSection } from "@/components/console/console-shell";
import { useDialogs } from "@/components/console/dialog-host";
import { LatestRelease, NoLatest } from "@/components/console/home/collectors/latest-release";
import { OlderReleases } from "@/components/console/home/collectors/older-releases";
import { UsageGuide } from "@/components/console/home/collectors/usage-guide";
import { useReleaseMenu } from "@/components/console/home/collectors/use-release-menu";
import { CAPTION, Menu, type MenuItem } from "@/components/console/kit";
import { useBlobDownload } from "@/components/console/use-blob-download";
import { api, errorMessage, type CollectorRelease, type ReleasePackage } from "@/lib/api";
import { useAuthStore } from "@/stores/auth-store";

/** 采集器: the latest release as four equal tiles, usage guide, and older releases collapsed (spec #19). */
export function CollectorsSection() {
  const token = useAuthStore((s) => s.token);
  const { toast } = useDialogs();
  const { download: saveFile } = useBlobDownload();
  const [releases, setReleases] = useState<CollectorRelease[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Bumped after a status change; the list lives here, so it must be re-fetched.
  const [reloads, setReloads] = useState(0);
  const releaseMenu = useReleaseMenu(() => setReloads((n) => n + 1));

  useEffect(() => {
    if (!token) return;
    api.releases.list(token).then(setReleases, (e: unknown) => setError(errorMessage(e)));
  }, [token, reloads]);

  async function download(release: CollectorRelease, pkg: ReleasePackage) {
    if (!token) return;
    if (await saveFile(() => api.downloads.download(token, release.version, pkg.platform), pkg.fileName)) {
      toast(`正在下载 ${pkg.fileName}`);
    }
  }

  const latest = releases?.find((r) => r.status === "latest");
  const older = releases?.filter((r) => r !== latest) ?? [];

  return (
    <ConsoleSection id="collectors">
      <div className="mx-auto max-w-[1240px] px-8 py-24">
        <Heading latest={latest} menu={latest ? releaseMenu(latest) : []} />
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

/** The caption, the headline with the latest version and its `···` menu, and the subline. */
function Heading({ latest, menu }: { latest: CollectorRelease | undefined; menu: MenuItem[] }) {
  return (
    <>
      <p className={CAPTION}>Collector</p>
      <div className="mt-4 flex items-center gap-4">
        <h2 className="text-[56px] leading-[1.1] font-bold tracking-[-2px]">
          下载采集器 {latest && <span className="text-primary">v{latest.version}</span>}
        </h2>
        {latest && <Menu items={menu} label={`v${latest.version} 的操作`} />}
      </div>
      <p className="mt-4 text-lg text-[#ccc]">按客户主机的系统和架构选择，四个包功能完全一致。</p>
    </>
  );
}
