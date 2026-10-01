import { create } from "zustand";
import type { User, UserRole } from "@/lib/auth-types";
import { api, ApiError, type Session } from "@/lib/api";

const SESSION_KEY = "dbcheck_session";

interface AuthStore {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;

  /** Resolves to an error message, or null on success. */
  login: (username: string, password: string) => Promise<string | null>;
  /** Mock mode only: signs in with the seed account of a role. */
  quickLogin: (role: UserRole) => Promise<void>;
  logout: () => void;
  hydrate: () => void;
}

export const useAuthStore = create<AuthStore>((set) => {
  function start(session: Session) {
    sessionStorage.setItem(SESSION_KEY, JSON.stringify(session));
    set({ user: session.user, token: session.token, isAuthenticated: true });
  }

  return {
    user: null,
    token: null,
    isAuthenticated: false,

    login: async (username, password) => {
      try {
        start(await api.auth.signIn(username, password));
        return null;
      } catch (e) {
        if (e instanceof ApiError) return e.message;
        throw e;
      }
    },

    // Seed accounts use the role name as username and password.
    quickLogin: async (role) => start(await api.auth.signIn(role, role)),

    logout: () => {
      sessionStorage.removeItem(SESSION_KEY);
      set({ user: null, token: null, isAuthenticated: false });
    },

    hydrate: () => {
      if (typeof window === "undefined") return;
      const raw = sessionStorage.getItem(SESSION_KEY);
      if (!raw) return;
      const session = JSON.parse(raw) as Session;
      set({ user: session.user, token: session.token, isAuthenticated: true });
    },
  };
});
