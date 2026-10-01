import { requireSessionUser } from "@/lib/api/auth/mock";
import type { DownloadRecord, DownloadsApi } from "@/lib/api/downloads/contract";
import { seedDownloadRecords } from "@/lib/api/downloads/seed";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import { canSeeRelease, mockReleaseRecords } from "@/lib/api/releases/mock";

function mockDownloadRecords(ctx: MockContext) {
  return mockCollection<DownloadRecord[]>(ctx.storage, "downloads", seedDownloadRecords);
}

export function createMockDownloads(ctx: MockContext): DownloadsApi {
  return {
    async download(token, version, platform) {
      const user = requireSessionUser(ctx, token);
      const release = mockReleaseRecords(ctx)
        .read()
        .find((r) => r.version === version && canSeeRelease(user, r));
      const pkg = release?.packages.find((p) => p.platform === platform);
      if (!pkg) throw new ApiError("not_found", `没有 v${version} 的 ${platform} 采集器包`);

      const records = mockDownloadRecords(ctx);
      const record: DownloadRecord = { id: mockId("d"), userId: user.id, version, platform, at: new Date().toISOString() };
      records.write([...records.read(), record]);
      // A stand-in for the real package: the mock holds no binaries.
      return new Blob([`mock package ${pkg.fileName}\nsha256 ${pkg.sha256}\n`], { type: "application/zip" });
    },

    async records(token) {
      const user = requireSessionUser(ctx, token);
      if (user.role !== "admin") throw new ApiError("forbidden", "只有管理员可以查看下载记录");
      return mockDownloadRecords(ctx)
        .read()
        .sort((a, b) => b.at.localeCompare(a.at));
    },
  };
}
