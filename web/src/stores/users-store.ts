import { create } from "zustand";
import { api, type UserProfile } from "@/lib/api";

/**
 * The admin's view of every user. One list feeds both 管理 → 用户 and the
 * pending badge on the rail, so each decision updates both at once.
 */
interface UsersStore {
  /** null until the first load. */
  users: UserProfile[] | null;
  load: (token: string) => Promise<void>;
  /** Each action rejects with the API's `ApiError`; the list reloads either way. */
  approve: (token: string, userId: string) => Promise<UserProfile>;
  reject: (token: string, userId: string, reason: string) => Promise<UserProfile>;
  disable: (token: string, userId: string, reason: string) => Promise<UserProfile>;
  enable: (token: string, userId: string) => Promise<UserProfile>;
  promote: (token: string, userId: string) => Promise<UserProfile>;
  demote: (token: string, userId: string) => Promise<UserProfile>;
  /** Resolves to the temporary password for the admin to hand over. */
  resetPassword: (token: string, userId: string) => Promise<string>;
}

export const useUsersStore = create<UsersStore>((set, get) => {
  async function thenReload<T>(token: string, action: Promise<T>): Promise<T> {
    try {
      return await action;
    } finally {
      await get().load(token);
    }
  }

  return {
    users: null,
    load: async (token) => set({ users: await api.users.list(token) }),
    approve: (token, userId) => thenReload(token, api.users.approve(token, userId)),
    reject: (token, userId, reason) => thenReload(token, api.users.reject(token, userId, reason)),
    disable: (token, userId, reason) => thenReload(token, api.users.disable(token, userId, reason)),
    enable: (token, userId) => thenReload(token, api.users.enable(token, userId)),
    promote: (token, userId) => thenReload(token, api.users.promote(token, userId)),
    demote: (token, userId) => thenReload(token, api.users.demote(token, userId)),
    resetPassword: async (token, userId) => (await thenReload(token, api.users.resetPassword(token, userId))).temporaryPassword,
  };
});

export function pendingUsers(users: UserProfile[] | null): UserProfile[] {
  return (users ?? []).filter((a) => a.status === "pending");
}

/** The count behind the 管理 badge. */
export function usePendingCount(): number {
  return useUsersStore((s) => pendingUsers(s.users).length);
}
