import type { User } from "@/lib/auth-types";

const MOCK_USERS: Record<string, { password: string; user: User }> = {
  admin: {
    password: "admin",
    user: {
      id: "u-admin-001",
      username: "admin",
      displayName: "系统管理员",
      role: "admin",
    },
  },
  user: {
    password: "user",
    user: {
      id: "u-user-001",
      username: "user",
      displayName: "巡检工程师",
      role: "user",
    },
  },
};

const TOKEN_PREFIX = "mock-jwt-";

function tokenFor(username: string): string {
  return `${TOKEN_PREFIX}${username}`;
}

export function mockLogin(
  username: string,
  password: string,
): { user: User; token: string } | null {
  const entry = MOCK_USERS[username];
  if (!entry || entry.password !== password) return null;
  return { user: entry.user, token: tokenFor(username) };
}

export function mockGetCurrentUser(token: string): User | null {
  if (!token.startsWith(TOKEN_PREFIX)) return null;
  const username = token.slice(TOKEN_PREFIX.length);
  return MOCK_USERS[username]?.user ?? null;
}

export function mockQuickLogin(role: "admin" | "user"): {
  user: User;
  token: string;
} {
  const entry = MOCK_USERS[role]!;
  return { user: entry.user, token: tokenFor(role) };
}
