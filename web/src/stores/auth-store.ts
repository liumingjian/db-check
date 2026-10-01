import { create } from "zustand";
import type { User, UserRole } from "@/lib/auth-types";
import { api, ApiError, type Registration, type Session } from "@/lib/api";

const SESSION_KEY = "dbcheck_session";

/** `unknown` until `hydrate` has asked the API who the stored token belongs to. */
export type AuthStatus = "unknown" | "anonymous" | "authenticated";

interface AuthStore {
  status: AuthStatus;
  user: User | null;
  token: string | null;

  /** Resolves to an error message, or null on success. */
  login: (username: string, password: string) => Promise<string | null>;
  /** Registers and signs in the new, pending applicant. Resolves to an error message, or null on success. */
  register: (registration: Registration) => Promise<string | null>;
  /** Sets the caller's own password, ending a forced change. Resolves to an error message, or null on success. */
  changePassword: (newPassword: string) => Promise<string | null>;
  /** Mock mode only: signs in with the seed account of a role. */
  quickLogin: (role: UserRole) => Promise<void>;
  logout: () => Promise<void>;
  /** Resolves the token kept in this tab through current-user; a dead session signs out. */
  hydrate: () => Promise<void>;
  /** Re-reads the signed-in user, e.g. after an account status change; a dead session signs out. */
  refresh: () => Promise<void>;
}

export const useAuthStore = create<AuthStore>((set, get) => {
  function start(token: string, user: User) {
    sessionStorage.setItem(SESSION_KEY, token);
    set({ status: "authenticated", user, token });
  }

  function clear() {
    sessionStorage.removeItem(SESSION_KEY);
    set({ status: "anonymous", user: null, token: null });
  }

  /** Starts the session `open` resolves to, or returns the API's error message. */
  async function attempt(open: () => Promise<Session>): Promise<string | null> {
    try {
      const session = await open();
      start(session.token, session.user);
      return null;
    } catch (e) {
      if (e instanceof ApiError) return e.message;
      throw e;
    }
  }

  async function resolve(token: string) {
    try {
      start(token, await api.auth.currentUser(token));
    } catch {
      // An ended session or an unreachable backend: either way, sign in again.
      clear();
    }
  }

  return {
    status: "unknown",
    user: null,
    token: null,

    login: (username, password) => attempt(() => api.auth.signIn(username, password)),

    register: (registration) => attempt(() => api.users.register(registration)),

    changePassword: async (newPassword) => {
      const { token } = get();
      if (!token) return null;
      try {
        await api.users.changePassword(token, newPassword);
      } catch (e) {
        if (e instanceof ApiError) return e.message;
        throw e;
      }
      await resolve(token);
      return null;
    },

    // Seed accounts use the role name as username and password.
    quickLogin: async (role) => {
      const session = await api.auth.signIn(role, role);
      start(session.token, session.user);
    },

    logout: async () => {
      const { token } = get();
      clear();
      if (token) await api.auth.signOut(token).catch(() => undefined);
    },

    hydrate: async () => {
      if (get().status !== "unknown") return;
      const token = sessionStorage.getItem(SESSION_KEY);
      if (!token) return clear();
      await resolve(token);
    },

    refresh: async () => {
      const { token } = get();
      if (token) await resolve(token);
    },
  };
});
