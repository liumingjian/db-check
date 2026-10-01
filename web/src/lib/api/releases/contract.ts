/**
 * Collector releases and release status. Releases come from CI only (ADR
 * 0002); the platform reads them and, for admins, changes their status (#25).
 */
import type { DbType } from "@/lib/types";

/** pre-release: admins only. latest: the one recommended release. revoked: admins only, always with a reason. */
export type ReleaseStatus = "pre-release" | "latest" | "deprecated" | "revoked";

/** The four equal release package platforms; every package is a `.zip`. */
export type Platform = "linux-amd64" | "linux-arm64" | "windows-amd64" | "windows-arm64";

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
  dbTypes: DbType[];
  /** In platform order: linux-amd64, linux-arm64, windows-amd64, windows-arm64. */
  packages: ReleasePackage[];
}

/** An admin's release status action. */
export type ReleaseAction = "promote" | "deprecate" | "revoke" | "restore";

/** The release status table (spec #19): the statuses each action starts from, and where it ends. */
export const RELEASE_TRANSITIONS: Record<ReleaseAction, { from: ReleaseStatus[]; to: ReleaseStatus }> = {
  promote: { from: ["pre-release", "deprecated"], to: "latest" },
  deprecate: { from: ["latest", "deprecated", "pre-release"], to: "deprecated" },
  revoke: { from: ["pre-release", "latest", "deprecated"], to: "revoked" },
  restore: { from: ["revoked"], to: "deprecated" },
};

/** The actions worth offering on a release: allowed by the table and actually changing its status. */
export function releaseActionsFor(status: ReleaseStatus): ReleaseAction[] {
  return (Object.keys(RELEASE_TRANSITIONS) as ReleaseAction[]).filter((action) => {
    const { from, to } = RELEASE_TRANSITIONS[action];
    return from.includes(status) && to !== status;
  });
}

export interface ReleasesApi {
  /**
   * The releases the session's user may see, newest first: engineers get
   * latest and deprecated only; admins get every status.
   */
  list(token: string): Promise<CollectorRelease[]>;

  // Admin-only status actions, per `RELEASE_TRANSITIONS`. Each rejects with
  // `forbidden` for engineers, `not_found` for an unknown version, and
  // `invalid` for a transition the table does not allow.

  /** Makes the release latest; the previous latest becomes deprecated. */
  promote(token: string, version: string): Promise<void>;
  /** Deprecating the latest release leaves no latest release. */
  deprecate(token: string, version: string): Promise<void>;
  /** `reason` is required (`invalid` when blank) and kept as `revokeReason`. */
  revoke(token: string, version: string, reason: string): Promise<void>;
  /** Revoked becomes deprecated and loses its revocation reason. */
  restore(token: string, version: string): Promise<void>;
}
