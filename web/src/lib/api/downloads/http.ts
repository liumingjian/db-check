import type { DownloadRecord, DownloadsApi } from "@/lib/api/downloads/contract";
import type { HttpClient } from "@/lib/api/http-client";

/** Package downloads (`/api/releases/{version}/packages/{platform}`) and download records (`/api/downloads`) on db-web. */
export function createHttpDownloads(client: HttpClient): DownloadsApi {
  return {
    async download(token, version, platform) {
      const path = `/api/releases/${encodeURIComponent(version)}/packages/${encodeURIComponent(platform)}`;
      const resp = await client.request("下载采集器失败", path, token);
      return resp.blob();
    },

    async records(token, filter = {}) {
      const query = new URLSearchParams();
      for (const [key, value] of Object.entries(filter)) {
        if (value !== undefined) query.set(key, value);
      }
      const search = query.toString();
      const resp = await client.request("读取下载记录失败", `/api/downloads${search && `?${search}`}`, token);
      return (await resp.json()) as DownloadRecord[];
    },
  };
}
