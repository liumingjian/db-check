import type { Session } from "@/lib/api/auth/contract";
import type { HttpClient, JsonPost } from "@/lib/api/http-client";
import type { PasswordReset, UserProfile, UsersApi } from "@/lib/api/users/contract";

/** Accounts on db-web: registration and resubmission under `/api/auth`, the rest under `/api/users`. */
export function createHttpUsers(client: HttpClient): UsersApi {
  async function profile(req: JsonPost): Promise<UserProfile> {
    const resp = await client.post(req);
    return (await resp.json()) as UserProfile;
  }

  const account = (id: string, operation: string) => `/api/users/${encodeURIComponent(id)}/${operation}`;

  return {
    async register(registration) {
      const resp = await client.post({ action: "注册失败", path: "/api/auth/register", token: null, body: registration });
      return (await resp.json()) as Session;
    },

    async myProfile(token) {
      const resp = await client.request("读取账号信息失败", "/api/users/me", token);
      return (await resp.json()) as UserProfile;
    },

    resubmit: (token, resubmission) => profile({ action: "重新提交申请失败", path: "/api/auth/resubmit", token, body: resubmission }),

    async list(token) {
      const resp = await client.request("读取用户列表失败", "/api/users", token);
      return (await resp.json()) as UserProfile[];
    },

    approve: (token, userId) => profile({ action: "批准申请失败", path: account(userId, "approve"), token }),
    reject: (token, userId, reason) => profile({ action: "拒绝申请失败", path: account(userId, "reject"), token, body: { reason } }),
    disable: (token, userId, reason) => profile({ action: "禁用账号失败", path: account(userId, "disable"), token, body: { reason } }),
    enable: (token, userId) => profile({ action: "启用账号失败", path: account(userId, "enable"), token }),
    promote: (token, userId) => profile({ action: "设为管理员失败", path: account(userId, "promote"), token }),
    demote: (token, userId) => profile({ action: "取消管理员失败", path: account(userId, "demote"), token }),

    async resetPassword(token, userId) {
      const resp = await client.post({ action: "重置密码失败", path: account(userId, "reset-password"), token });
      return (await resp.json()) as PasswordReset;
    },

    changePassword: (token, newPassword, currentPassword) =>
      profile({ action: "修改密码失败", path: "/api/users/me/password", token, body: { newPassword, currentPassword } }),
  };
}
