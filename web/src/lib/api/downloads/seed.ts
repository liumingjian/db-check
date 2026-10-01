import type { DownloadRecord } from "@/lib/api/downloads/contract";

/** Download records across the seed users and releases. */
export function seedDownloadRecords(): DownloadRecord[] {
  return [
    { id: "d-seed-001", userId: "u-user-001", version: "1.0.0", platform: "linux-amd64", at: "2026-08-02T01:10:00Z" },
    { id: "d-seed-002", userId: "u-user-001", version: "1.1.0", platform: "linux-arm64", at: "2026-08-22T06:00:00Z" },
    { id: "d-seed-003", userId: "u-user-001", version: "1.2.0", platform: "linux-amd64", at: "2026-09-11T00:45:00Z" },
    { id: "d-seed-004", userId: "u-user-001", version: "1.2.0", platform: "windows-amd64", at: "2026-09-12T08:30:00Z" },
    { id: "d-seed-005", userId: "u-admin-001", version: "1.2.0", platform: "linux-arm64", at: "2026-09-15T03:11:00Z" },
    { id: "d-seed-006", userId: "u-admin-001", version: "1.3.0-rc1", platform: "windows-arm64", at: "2026-09-29T01:00:00Z" },
  ];
}
