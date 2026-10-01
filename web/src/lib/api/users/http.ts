import type { Session } from "@/lib/api/auth/contract";
import type { HttpClient } from "@/lib/api/http-client";
import type { PasswordReset, UserProfile, UsersApi } from "@/lib/api/users/contract";

/** Accounts on db-web: registration and resubmission under `/api/auth`, the rest under `/api/users`. */
export function createHttpUsers(client: HttpClient): UsersApi {
  async function profile(action: string, path: string, token: string, body?: unknown): Promise<UserProfile> {
    const resp = await client.send(action, "POST", path, token, body);
    return (await resp.json()) as UserProfile;
  }

  const account = (id: string, operation: string) => `/api/users/${encodeURIComponent(id)}/${operation}`;

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

    async resetPassword(token, userId) {
      const resp = await client.send("重置密码失败", "POST", account(userId, "reset-password"), token);
      return (await resp.json()) as PasswordReset;
    },

    changePassword: (token, newPassword, currentPassword) =>
      profile("修改密码失败", "/api/users/me/password", token, { newPassword, currentPassword }),
  };
}
