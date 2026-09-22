export type Platform =
  | "linux-amd64"
  | "linux-arm64"
  | "windows-amd64";

export interface PlatformMeta {
  platform: Platform;
  osLabel: string;
  archLabel: string;
  description: string;
}

export const PLATFORMS: PlatformMeta[] = [
  {
    platform: "linux-amd64",
    osLabel: "Linux",
    archLabel: "x86_64",
    description: "CentOS / RHEL / Ubuntu / 麒麟 x86",
  },
  {
    platform: "linux-arm64",
    osLabel: "Linux",
    archLabel: "ARM64",
    description: "鲲鹏 / 飞腾 / 银河麒麟 aarch64",
  },
  {
    platform: "windows-amd64",
    osLabel: "Windows",
    archLabel: "x86_64",
    description: "Windows 64位操作系统",
  },
];

export interface ToolPackage {
  platform: Platform;
  fileName: string;
  fileSize: number;
  md5: string;
  downloadUrl: string;
}

export interface ToolRelease {
  id: string;
  version: string;
  releasedAt: string;
  changelog: string;
  isLatest: boolean;
  packages: ToolPackage[];
}
