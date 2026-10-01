import type { CollectorRelease } from "@/lib/api/releases/contract";
import { seedFixture, seedTime } from "@/lib/api/seed-fixture";

/** The fixture's releases: a pre-release, the latest, a deprecated, and a revoked one. */
export function seedReleases(now: number): CollectorRelease[] {
  return seedFixture.releases.map((release) => ({
    ...release,
    publishedAt: seedTime(release.publishedAt, now),
  })) as CollectorRelease[];
}
