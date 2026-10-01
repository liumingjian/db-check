import type { User } from "@/lib/auth-types";
import { requireSessionUser } from "@/lib/api/auth/mock";
import { mockCollection, type MockContext } from "@/lib/api/mock-storage";
import type { CollectorRelease, ReleasesApi } from "@/lib/api/releases/contract";
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
  };
}
