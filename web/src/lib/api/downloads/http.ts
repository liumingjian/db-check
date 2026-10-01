import type { DownloadsApi } from "@/lib/api/downloads/contract";
import { ApiError } from "@/lib/api/errors";

/** db-web has no download proxy or records yet (a later phase); mock mode is the target for now. */
export function createHttpDownloads(): DownloadsApi {
  const notImplemented = async (): Promise<never> => {
    throw new ApiError("failed", "采集器下载接口尚未实现");
  };
  return { download: notImplemented, records: notImplemented };
}
