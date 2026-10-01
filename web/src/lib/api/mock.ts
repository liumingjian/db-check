import type { DbCheckApi } from "@/lib/api/contract";
import { createMockAuth } from "@/lib/api/auth/mock";
import { createMockDownloads } from "@/lib/api/downloads/mock";
import { clearMockData, type MockContext } from "@/lib/api/mock-storage";
import { createMockReleases } from "@/lib/api/releases/mock";
import { createMockReports } from "@/lib/api/reports/mock";
import { createMockUsers } from "@/lib/api/users/mock";

export interface MockApi extends DbCheckApi {
  /** Restores every domain to its seed and ends all mock sessions. */
  resetMockData(): void;
}

export function createMockApi({
  storage,
  stepDelayMs = 250,
  now = Date.now,
}: Partial<MockContext> & { storage: Storage }): MockApi {
  const ctx: MockContext = { storage, stepDelayMs, now };
  return {
    auth: createMockAuth(ctx),
    users: createMockUsers(ctx),
    releases: createMockReleases(ctx),
    downloads: createMockDownloads(ctx),
    reports: createMockReports(ctx),
    resetMockData: () => clearMockData(storage),
  };
}
