import { ApiError } from "@/lib/api/errors";
import type { Session } from "@/lib/api/auth/contract";
import type { HttpClient } from "@/lib/api/http-client";
import type { UserProfile, UsersApi } from "@/lib/api/users/contract";

/** Accounts on db-web: registration and resubmission under `/api/auth`, the rest under `/api/users`. */
export function createHttpUsers(client: HttpClient): UsersApi {
  async function profile(action: string, path: string, token: string, body?: unknown): Promise<UserProfile> {
    const resp = await client.send(action, "POST", path, token, body);
    return (await resp.json()) as UserProfile;
  }

  const account = (id: string, operation: string) => `/api/users/${encodeURIComponent(id)}/${operation}`;

  // db-web does not serve password reset and change yet (#38).
  const passwordsUnavailable = async (): Promise<never> => {
    throw new ApiError("failed", "后端暂未提供密码管理");
  };

  return {
    async register(registration) {
      const resp = await client.send("注册失败", "POST", "/api/auth/register", null, registration);
      return (await resp.json()) as Session;
    },

    async myProfile(token) {
      const resp = await client.request("读取账号信息失败", "/api/users/me", token);
      return (await resp.json()) as UserProfile;
    },

    resubmit: (token, resubmission) => profile("重新提交申请失败", "/api/auth/resubmit", token, resubmission),

    async list(token) {
      const resp = await client.request("读取用户列表失败", "/api/users", token);
      return (await resp.json()) as UserProfile[];
    },

    approve: (token, userId) => profile("批准申请失败", account(userId, "approve"), token),
    reject: (token, userId, reason) => profile("拒绝申请失败", account(userId, "reject"), token, { reason }),
    disable: (token, userId, reason) => profile("禁用账号失败", account(userId, "disable"), token, { reason }),
    enable: (token, userId) => profile("启用账号失败", account(userId, "enable"), token),
    promote: (token, userId) => profile("设为管理员失败", account(userId, "promote"), token),
    demote: (token, userId) => profile("取消管理员失败", account(userId, "demote"), token),
    resetPassword: passwordsUnavailable,
    changePassword: passwordsUnavailable,
  };
}
