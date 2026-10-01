import { ApiError } from "@/lib/api/errors";
import type { UsersApi } from "@/lib/api/users/contract";

/** db-web has no accounts yet (spec Phase 2); every users operation fails in real mode. */
export function createHttpUsers(): UsersApi {
  const unavailable = async (): Promise<never> => {
    throw new ApiError("failed", "后端暂未提供账号管理");
  };
  return {
    register: unavailable,
    myAccount: unavailable,
    resubmit: unavailable,
    list: unavailable,
    approve: unavailable,
    reject: unavailable,
  };
}
