import type { User } from "@/lib/auth-types";
import type { AuthApi, Session } from "@/lib/api/auth/contract";
import type { HttpClient } from "@/lib/api/http-client";

/** Sessions on db-web: `/api/auth`. */
export function createHttpAuth(client: HttpClient): AuthApi {
  return {
    async signIn(username, password) {
      const resp = await client.post({ action: "登录失败", path: "/api/auth/sign-in", token: null, body: { username, password } });
      return (await resp.json()) as Session;
    },

    async currentUser(token) {
      const resp = await client.request("读取当前用户失败", "/api/auth/me", token);
      return (await resp.json()) as User;
    },

    async signOut(token) {
      await client.post({ action: "退出登录失败", path: "/api/auth/sign-out", token });
    },
  };
}
