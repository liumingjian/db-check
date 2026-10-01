import type { User } from "@/lib/auth-types";
import { requireSessionUser } from "@/lib/api/auth/mock";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, type MockContext } from "@/lib/api/mock-storage";
import {
  RELEASE_TRANSITIONS,
  type CollectorRelease,
  type ReleaseAction,
  type ReleasesApi,
} from "@/lib/api/releases/contract";
import { seedReleases } from "@/lib/api/releases/seed";

/** The release records every mock domain reads, e.g. downloads to find a package. */
export function mockReleaseRecords(ctx: MockContext) {
  return mockCollection<CollectorRelease[]>(ctx.storage, "releases", seedReleases);
}

/** Engineers see latest and deprecated releases; admins see every status. */
export function canSeeRelease(user: User, release: CollectorRelease): boolean {
  return user.role === "admin" || release.status === "latest" || release.status === "deprecated";
}

export function createMockReleases(ctx: MockContext): ReleasesApi {
  return {
    async list(token) {
      const user = requireSessionUser(ctx, token);
      return mockReleaseRecords(ctx)
        .read()
        .filter((r) => canSeeRelease(user, r))
        .sort((a, b) => b.publishedAt.localeCompare(a.publishedAt));
    },

    promote: async (token, version) => changeStatus(ctx, token, version, "promote"),
    deprecate: async (token, version) => changeStatus(ctx, token, version, "deprecate"),
    async revoke(token, version, reason) {
      const revokeReason = reason.trim();
      if (!revokeReason) throw new ApiError("invalid", "请填写撤回原因");
      changeStatus(ctx, token, version, "revoke", revokeReason);
    },
    restore: async (token, version) => changeStatus(ctx, token, version, "restore"),
  };
}

/** Applies one row of the release status table; the only writer of release statuses. */
function changeStatus(ctx: MockContext, token: string, version: string, action: ReleaseAction, revokeReason?: string) {
  if (requireSessionUser(ctx, token).role !== "admin") throw new ApiError("forbidden", "只有管理员可以修改版本状态");
  const records = mockReleaseRecords(ctx);
  const releases = records.read();
  const target = releases.find((r) => r.version === version);
  if (!target) throw new ApiError("not_found", `版本 ${version} 不存在`);
  const { from, to } = RELEASE_TRANSITIONS[action];
  if (!from.includes(target.status)) throw new ApiError("invalid", `版本 ${version} 当前状态不允许此操作`);

  records.write(
    releases.map((r): CollectorRelease => {
      if (r === target) {
        const next: CollectorRelease = { ...r, status: to, revokeReason };
        if (!revokeReason) delete next.revokeReason;
        return next;
      }
      // Only one release is ever latest.
      return to === "latest" && r.status === "latest" ? { ...r, status: "deprecated" } : r;
    }),
  );
}
