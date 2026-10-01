import type { User } from "@/lib/auth-types";
import type { AuthStatus } from "@/stores/auth-store";

/**
 * Who a route is for: `public` (signed out: /login, /register), `password`
 * (/change-password: a forced change is due, or an active user changes theirs),
 * `applicant` (pending or rejected: /pending), `console` (active users),
 * `admin` (active admins).
 */
export type RouteAccess = "public" | "password" | "applicant" | "console" | "admin";

/**
 * Where a visitor must go instead of `access`, or null to stay. Every redirect
 * rule of the console lives here (spec #19, Navigation).
 */
export function redirectFor(access: RouteAccess, status: AuthStatus, user: User | null): string | null {
  if (status === "unknown") return null;
  if (!user) return access === "public" ? null : "/login";
  // After a password reset, every route leads to the change-password form until it is done.
  if (user.mustChangePassword) return access === "password" ? null : "/change-password";
  // Pending and rejected users see only the waiting page (and its resubmit form).
  const applying = user.status === "pending" || user.status === "rejected";
  if (access === "applicant") return applying ? null : "/";
  if (applying) return "/pending";
  if (access === "public") return "/";
  if (access === "admin" && user.role !== "admin") return "/";
  return null;
}
