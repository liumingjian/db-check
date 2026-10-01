import type { AuthApi } from "@/lib/api/auth/contract";
import { ApiError } from "@/lib/api/errors";
import { httpRequest } from "@/lib/api/http-client";

const PROBE_TASK_ID = "frontend-probe";

/**
 * Bridge until the backend has accounts (spec Phase 2): db-web knows a single
 * bearer credential, so the password typed at sign-in is that credential. It
 * is verified with a status probe (404 for the probe task means accepted) and
 * then carried on every request. Nothing in the frontend defaults it.
 */
export function createHttpAuth(): AuthApi {
  return {
    async signIn(username, password) {
      try {
        await httpRequest("登录校验失败", `/api/reports/status/${PROBE_TASK_ID}`, password);
      } catch (e) {
        if (!(e instanceof ApiError && e.code === "not_found")) throw e;
      }
      return {
        token: password,
        user: { id: `backend-${username}`, username, displayName: username, role: "user" },
      };
    },
  };
}
