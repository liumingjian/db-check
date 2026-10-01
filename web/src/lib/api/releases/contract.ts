/**
 * Collector releases and release status. Releases come from CI only (ADR
 * 0002); the platform reads them and, for admins, changes their status (#25).
 */

/** pre-release: admins only. latest: the one recommended release. revoked: admins only, always with a reason. */
export type ReleaseStatus = "pre-release" | "latest" | "deprecated" | "revoked";

/** The four equal release package platforms; every package is a `.zip`. */
export type Platform = "linux-amd64" | "linux-arm64" | "windows-amd64" | "windows-arm64";

/** A database type a collector release supports. */
export type ReleaseDbType = "mysql" | "oracle" | "gaussdb";

export interface ReleasePackage {
  platform: Platform;
  /** Display labels, e.g. `Linux` / `ARM64`. */
  os: string;
  arch: string;
  fileName: string;
  /** Bytes. */
  size: number;
  sha256: string;
}

export interface CollectorRelease {
  version: string;
  tag: string;
  commit: string;
  publishedAt: string;
  status: ReleaseStatus;
  /** Set exactly when `status` is `revoked`. */
  revokeReason?: string;
  /** `- ` bullet lines from the tag annotation or CHANGELOG. */
  notes: string;
  dbTypes: ReleaseDbType[];
  /** In platform order: linux-amd64, linux-arm64, windows-amd64, windows-arm64. */
  packages: ReleasePackage[];
}

export interface ReleasesApi {
  /**
   * The releases the session's user may see, newest first: engineers get
   * latest and deprecated only; admins get every status.
   */
  list(token: string): Promise<CollectorRelease[]>;
}
