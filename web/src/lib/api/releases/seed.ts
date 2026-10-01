import type { CollectorRelease, ReleasePackage } from "@/lib/api/releases/contract";

const PLATFORMS: Array<Pick<ReleasePackage, "platform" | "os" | "arch">> = [
  { platform: "linux-amd64", os: "Linux", arch: "x86_64" },
  { platform: "linux-arm64", os: "Linux", arch: "ARM64" },
  { platform: "windows-amd64", os: "Windows", arch: "x86_64" },
  { platform: "windows-arm64", os: "Windows", arch: "ARM64" },
];

/** A stable fake SHA256 (FNV-1a stretched to 64 hex chars), so reseeding keeps checksums. */
function fakeSha256(seed: string): string {
  let h = 2166136261;
  let out = "";
  for (let i = 0; out.length < 64; i++) {
    h = Math.imul(h ^ seed.charCodeAt(i % seed.length) ^ i, 16777619) >>> 0;
    out += h.toString(16).padStart(8, "0");
  }
  return out.slice(0, 64);
}

function packagesFor(version: string): ReleasePackage[] {
  return PLATFORMS.map((p, i) => ({
    ...p,
    fileName: `db-collector-${version}-${p.platform}.zip`,
    size: 9_400_000 + i * 310_000 + version.length * 1000,
    sha256: fakeSha256(`${version}-${p.platform}`),
  }));
}

export function seedReleases(): CollectorRelease[] {
  return [
    {
      version: "1.3.0-rc1",
      tag: "v1.3.0-rc1",
      commit: "9f3c2ab",
      publishedAt: "2026-09-28T12:11:00Z",
      status: "pre-release",
      notes: "- 新增 GaussDB WDR 自动采集\n- 采集超时可配置",
      dbTypes: ["mysql", "oracle", "gaussdb"],
      packages: packagesFor("1.3.0-rc1"),
    },
    {
      version: "1.2.0",
      tag: "v1.2.0",
      commit: "5e81d07",
      publishedAt: "2026-09-10T10:30:00Z",
      status: "latest",
      notes: "- Oracle AWR 采集支持 RAC\n- 修复 MySQL 8.4 权限检查误报",
      dbTypes: ["mysql", "oracle", "gaussdb"],
      packages: packagesFor("1.2.0"),
    },
    {
      version: "1.1.0",
      tag: "v1.1.0",
      commit: "c47aa10",
      publishedAt: "2026-08-20T07:02:00Z",
      status: "deprecated",
      notes: "- 新增 GaussDB 基础采集",
      dbTypes: ["mysql", "oracle", "gaussdb"],
      packages: packagesFor("1.1.0"),
    },
    {
      version: "1.0.0",
      tag: "v1.0.0",
      commit: "1b2d3e4",
      publishedAt: "2026-08-01T04:00:00Z",
      status: "revoked",
      revokeReason: "Oracle 采集会在 11g 上锁表，请勿使用",
      notes: "- 首个版本：MySQL、Oracle",
      dbTypes: ["mysql", "oracle"],
      packages: packagesFor("1.0.0"),
    },
  ];
}
