import type { User } from "@/lib/auth-types";
import type { AuthApi, Session } from "@/lib/api/auth/contract";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import { mockUserRecords, publicUser } from "@/lib/api/users/mock";

/** Session token → user id. */
type MockSessions = Record<string, string>;

function mockSessions(ctx: MockContext) {
  return mockCollection<MockSessions>(ctx.storage, "sessions", () => ({}));
}

const DISABLED = "该账号已被禁用，请联系管理员";

/**
 * Resolves a session token to its user whatever their account status, for the
 * calls that pending, rejected and password-changing users still need (current
 * user, own account). Disabling an account ends its sessions.
 */
export function resolveSessionUser(ctx: MockContext, token: string): User {
  const userId = mockSessions(ctx).read()[token];
  const found = mockUserRecords(ctx).read().find((u) => u.id === userId);
  if (!found) throw new ApiError("unauthorized", "登录已失效，请重新登录");
  if (found.status === "disabled") throw new ApiError("unauthorized", DISABLED);
  return publicUser(found);
}

/**
 * Resolves a session token to its user, as the backend would per request, and
 * lets only active accounts with no forced password change due through
 * (`forbidden` otherwise). Every console operation goes through this.
 */
export function requireSessionUser(ctx: MockContext, token: string): User {
  const user = resolveSessionUser(ctx, token);
  if (user.status !== "active") throw new ApiError("forbidden", "账号尚未启用");
  if (user.mustChangePassword) throw new ApiError("forbidden", "请先修改密码");
  return user;
}

/** Ends every session of a user, as a password reset does. */
export function endMockSessions(ctx: MockContext, userId: string): void {
  const sessions = mockSessions(ctx);
  sessions.write(Object.fromEntries(Object.entries(sessions.read()).filter(([, id]) => id !== userId)));
}

/** Opens a session for a user, as sign-in and registration both do. */
export function startMockSession(ctx: MockContext, userId: string): Session {
  const token = mockId("mock-session");
  const sessions = mockSessions(ctx);
  sessions.write({ ...sessions.read(), [token]: userId });
  return { token, user: resolveSessionUser(ctx, token) };
}

export function createMockAuth(ctx: MockContext): AuthApi {
  return {
    async signIn(username, password) {
      const found = mockUserRecords(ctx)
        .read()
        .find((u) => u.username === username && u.password === password);
      if (!found) throw new ApiError("unauthorized", "用户名或密码错误");
      if (found.status === "disabled") throw new ApiError("forbidden", DISABLED);
      return startMockSession(ctx, found.id);
    },

    async currentUser(token) {
      return resolveSessionUser(ctx, token);
    },

    async signOut(token) {
      const sessions = mockSessions(ctx);
      const rest = { ...sessions.read() };
      delete rest[token];
      sessions.write(rest);
    },
  };
}
