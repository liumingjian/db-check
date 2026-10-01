import type { DownloadRecord } from "@/lib/api/downloads/contract";
import { seedFixture, seedTime } from "@/lib/api/seed-fixture";

/** The fixture's download records across the seed users and releases. */
export function seedDownloadRecords(now: number): DownloadRecord[] {
  return seedFixture.downloadRecords.map((record) => ({ ...record, at: seedTime(record.at, now) })) as DownloadRecord[];
}
