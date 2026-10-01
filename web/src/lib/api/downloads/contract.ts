/** Release package downloads and download records (#24; filters in #26). */
import type { Platform } from "@/lib/api/releases/contract";

/** An audit entry: which user downloaded which release package, and when. */
export interface DownloadRecord {
  id: string;
  userId: string;
  version: string;
  platform: Platform;
  /** ISO timestamp. */
  at: string;
}

/** Narrows `records`; every set field must match. Omitted fields don't filter. */
export interface DownloadRecordFilter {
  userId?: string;
  /** Release version, e.g. `1.2.0`. */
  version?: string;
  /** ISO timestamp; keeps records at or after it. */
  from?: string;
  /** ISO timestamp; keeps records strictly before it. */
  to?: string;
}

export interface DownloadsApi {
  /**
   * Downloads one release package (a `.zip`) and writes one download record
   * for the session's user. Rejects with `not_found` for a package the user
   * may not see (engineers: pre-release and revoked releases).
   */
  download(token: string, version: string, platform: Platform): Promise<Blob>;
  /** Download records matching `filter`, newest first. Admins only; others get `forbidden`. */
  records(token: string, filter?: DownloadRecordFilter): Promise<DownloadRecord[]>;
}
