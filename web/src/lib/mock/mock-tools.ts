import type { ToolRelease, Platform, ToolPackage } from "@/lib/tool-types";

function pkg(
  version: string,
  platform: Platform,
  ext: string,
): ToolPackage {
  const fileName = `db-collector-${platform}-${version}${ext}`;
  return {
    platform,
    fileName,
    fileSize: 12_400_000 + Math.floor(Math.random() * 2_000_000),
    md5: Array.from({ length: 32 }, () =>
      "0123456789abcdef"[Math.floor(Math.random() * 16)],
    ).join(""),
    downloadUrl: `/api/tools/download/${fileName}`,
  };
}

function release(
  id: string,
  version: string,
  releasedAt: string,
  changelog: string,
  isLatest: boolean,
): ToolRelease {
  const platforms: Platform[] = [
    "linux-amd64",
    "linux-arm64",
    "windows-amd64",
  ];
  return {
    id,
    version,
    releasedAt,
    changelog,
    isLatest,
    packages: platforms.map((p) =>
      pkg(version, p, p.startsWith("windows") ? ".zip" : ".tar.gz"),
    ),
  };
}

const SEED_RELEASES: ToolRelease[] = [
  release(
    "rel-001",
    "v1.2.0",
    "2026-09-15T10:00:00Z",
    "- 统一采集器支持 MySQL、Oracle、GaussDB、PostgreSQL、达梦全引擎\n- 强化 ARM64（鲲鹏/飞腾）架构内网兼容性\n- 优化指标压缩打包与 manifest 校验速度",
    true,
  ),
  release(
    "rel-002",
    "v1.1.0",
    "2026-08-20T08:00:00Z",
    "- 内置跨引擎自适应驱动，命令行执行 --help 获取完整参数\n- 修复 Windows 控制台路径解析问题\n- 增强 OS 硬件与内核指标探针",
    false,
  ),
];

let releases = [...SEED_RELEASES];

export function mockGetReleases(): ToolRelease[] {
  return [...releases];
}

export function mockPublishRelease(
  version: string,
  changelog: string,
): ToolRelease {
  // Mark old latest as non-latest.
  releases = releases.map((r) => ({ ...r, isLatest: false }));

  const newRelease = release(
    `rel-${Date.now()}`,
    version,
    new Date().toISOString(),
    changelog,
    true,
  );
  releases = [newRelease, ...releases];
  return newRelease;
}

export function mockRemoveRelease(id: string): void {
  releases = releases.filter((r) => r.id !== id);
}
