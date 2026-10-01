import type { User } from "@/lib/auth-types";
import type { AuthApi } from "@/lib/api/auth/contract";
import { ApiError } from "@/lib/api/errors";
import { httpRequest } from "@/lib/api/http-client";

const PROBE_TASK_ID = "frontend-probe";

/** Accepts the credential when db-web answers the status probe with 404 for the probe task. */
async function verifyCredential(token: string): Promise<void> {
  try {
    await httpRequest("登录校验失败", `/api/reports/status/${PROBE_TASK_ID}`, token);
  } catch (e) {
    if (!(e instanceof ApiError && e.code === "not_found")) throw e;
  }
}

function backendUser(username: string): User {
  return { id: `backend-${username}`, username, displayName: username, role: "user", status: "active" };
}

/**
 * Bridge until the backend has accounts (spec Phase 2): db-web knows a single
 * bearer credential, so the password typed at sign-in is that credential. It
 * is verified with a status probe and then carried on every request. Nothing
 * in the frontend defaults it. db-web keeps no sessions, so current-user only
 * re-checks the credential and cannot recover the typed username, and
 * sign-out has nothing to end on the server.
 */
export function createHttpAuth(): AuthApi {
  return {
    async signIn(username, password) {
      await verifyCredential(password);
      return { token: password, user: backendUser(username) };
    },

    async currentUser(token) {
      await verifyCredential(token);
      return backendUser("backend");
    },

    async signOut() {},
  };
}
