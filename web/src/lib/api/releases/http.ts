import { ApiError } from "@/lib/api/errors";
import type { ReleasesApi } from "@/lib/api/releases/contract";

/** db-web has no release store yet (a later phase); mock mode is the target for now. */
export function createHttpReleases(): ReleasesApi {
  const notImplemented = async (): Promise<never> => {
    throw new ApiError("failed", "采集器版本接口尚未实现");
  };
  return {
    list: notImplemented,
    promote: notImplemented,
    deprecate: notImplemented,
    revoke: notImplemented,
    restore: notImplemented,
  };
}
