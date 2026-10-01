import { create } from "zustand";
import { api, type Account } from "@/lib/api";

/**
 * The admin's view of every account. One list feeds both 管理 → 用户 and the
 * pending badge on the rail, so each decision updates both at once.
 */
interface AccountsStore {
  /** null until the first load. */
  accounts: Account[] | null;
  load: (token: string) => Promise<void>;
  /** Rejects with the API's `ApiError`; the list reloads either way. */
  approve: (token: string, userId: string) => Promise<void>;
  reject: (token: string, userId: string, reason: string) => Promise<void>;
}

export const useAccountsStore = create<AccountsStore>((set, get) => {
  async function thenReload(token: string, action: Promise<unknown>) {
    try {
      await action;
    } finally {
      await get().load(token);
    }
  }

  return {
    accounts: null,
    load: async (token) => set({ accounts: await api.users.list(token) }),
    approve: (token, userId) => thenReload(token, api.users.approve(token, userId)),
    reject: (token, userId, reason) => thenReload(token, api.users.reject(token, userId, reason)),
  };
});

export function pendingAccounts(accounts: Account[] | null): Account[] {
  return (accounts ?? []).filter((a) => a.status === "pending");
}

/** The count behind the 管理 badge. */
export function usePendingCount(): number {
  return useAccountsStore((s) => pendingAccounts(s.accounts).length);
}
