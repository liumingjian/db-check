import { create } from "zustand";
import type { User, UserRole } from "@/lib/auth-types";
import {
  mockLogin,
  mockGetCurrentUser,
  mockQuickLogin,
} from "@/lib/mock/mock-auth";

const AUTH_TOKEN_KEY = "dbcheck_auth_token";

interface AuthStore {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;

  login: (username: string, password: string) => boolean;
  quickLogin: (role: UserRole) => void;
  logout: () => void;
  hydrate: () => void;
}

export const useAuthStore = create<AuthStore>((set) => ({
  user: null,
  token: null,
  isAuthenticated: false,

  login: (username, password) => {
    const result = mockLogin(username, password);
    if (!result) return false;
    sessionStorage.setItem(AUTH_TOKEN_KEY, result.token);
    set({ user: result.user, token: result.token, isAuthenticated: true });
    return true;
  },

  quickLogin: (role) => {
    const result = mockQuickLogin(role);
    sessionStorage.setItem(AUTH_TOKEN_KEY, result.token);
    set({ user: result.user, token: result.token, isAuthenticated: true });
  },

  logout: () => {
    sessionStorage.removeItem(AUTH_TOKEN_KEY);
    set({ user: null, token: null, isAuthenticated: false });
  },

  hydrate: () => {
    if (typeof window === "undefined") return;
    const token = sessionStorage.getItem(AUTH_TOKEN_KEY);
    if (!token) return;
    const user = mockGetCurrentUser(token);
    if (user) {
      set({ user, token, isAuthenticated: true });
    }
  },
}));
