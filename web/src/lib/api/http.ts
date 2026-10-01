import type { DbCheckApi } from "@/lib/api/contract";
import { createHttpAuth } from "@/lib/api/auth/http";
import { createHttpDownloads } from "@/lib/api/downloads/http";
import { createHttpReleases } from "@/lib/api/releases/http";
import { createHttpReports } from "@/lib/api/reports/http";
import { createHttpUsers } from "@/lib/api/users/http";

/** The real implementation: db-web over HTTP and WebSocket. */
export function createHttpApi(): DbCheckApi {
  return {
    auth: createHttpAuth(),
    users: createHttpUsers(),
    releases: createHttpReleases(),
    downloads: createHttpDownloads(),
    reports: createHttpReports(),
  };
}
