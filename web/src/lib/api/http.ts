import type { DbCheckApi } from "@/lib/api/contract";
import { createHttpAuth } from "@/lib/api/auth/http";
import { createHttpDownloads } from "@/lib/api/downloads/http";
import { createHttpClient } from "@/lib/api/http-client";
import { createHttpReleases } from "@/lib/api/releases/http";
import { createHttpReports } from "@/lib/api/reports/http";
import { createHttpUsers } from "@/lib/api/users/http";

export interface HttpApiOptions {
  /** db-web's origin; defaults to the base the browser resolves. */
  baseUrl?: string;
}

/** The real implementation: db-web over HTTP and WebSocket. */
export function createHttpApi({ baseUrl }: HttpApiOptions = {}): DbCheckApi {
  const client = createHttpClient(baseUrl);
  return {
    auth: createHttpAuth(client),
    users: createHttpUsers(client),
    releases: createHttpReleases(client),
    downloads: createHttpDownloads(),
    reports: createHttpReports(client),
  };
}
