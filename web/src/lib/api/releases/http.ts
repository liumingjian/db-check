import type { HttpClient } from "@/lib/api/http-client";
import type { CollectorRelease, ReleaseAction, ReleasesApi } from "@/lib/api/releases/contract";

/** Collector releases on db-web: `/api/releases`. */
export function createHttpReleases(client: HttpClient): ReleasesApi {
  const act = async (token: string, version: string, action: ReleaseAction, body?: { reason: string }) => {
    const path = `/api/releases/${encodeURIComponent(version)}/${action}`;
    await client.post({ action: "修改版本状态失败", path, token, body });
  };
  return {
    async list(token) {
      const resp = await client.request("读取采集器版本失败", "/api/releases", token);
      return (await resp.json()) as CollectorRelease[];
    },
    promote: (token, version) => act(token, version, "promote"),
    deprecate: (token, version) => act(token, version, "deprecate"),
    revoke: (token, version, reason) => act(token, version, "revoke", { reason }),
    restore: (token, version) => act(token, version, "restore"),
  };
}
