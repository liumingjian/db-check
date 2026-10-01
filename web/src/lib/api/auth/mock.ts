import type { User } from "@/lib/auth-types";
import type { AuthApi } from "@/lib/api/auth/contract";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import { mockUserRecords, publicUser } from "@/lib/api/users/mock";

/** Session token → user id. */
type MockSessions = Record<string, string>;

function mockSessions(ctx: MockContext) {
  return mockCollection<MockSessions>(ctx.storage, "sessions", () => ({}));
}

/** Resolves a session token to its user, as the backend would per request. */
export function requireSessionUser(ctx: MockContext, token: string): User {
  const userId = mockSessions(ctx).read()[token];
  const found = mockUserRecords(ctx).read().find((u) => u.id === userId);
  if (!found) throw new ApiError("unauthorized", "登录已失效，请重新登录");
  return publicUser(found);
}

export function createMockAuth(ctx: MockContext): AuthApi {
  return {
    async signIn(username, password) {
      const found = mockUserRecords(ctx)
        .read()
        .find((u) => u.username === username && u.password === password);
      if (!found) throw new ApiError("unauthorized", "用户名或密码错误");
      const token = mockId("mock-session");
      const sessions = mockSessions(ctx);
      sessions.write({ ...sessions.read(), [token]: found.id });
      return { token, user: requireSessionUser(ctx, token) };
    },
  };
}
