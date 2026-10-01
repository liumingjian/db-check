"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import type { User } from "@/lib/auth-types";
import { useAuthStore, type AuthStatus } from "@/stores/auth-store";

/**
 * Who a route is for: `public` (signed out: /login, /register), `applicant`
 * (pending or rejected: /pending), `console` (active users), `admin` (active admins).
 */
export type RouteAccess = "public" | "applicant" | "console" | "admin";

/**
 * Where a visitor must go instead of `access`, or null to stay. Every redirect
 * rule of the console lives here (spec #19, Navigation): later account states
 * (forced password change) add their rules to this function.
 */
export function redirectFor(access: RouteAccess, status: AuthStatus, user: User | null): string | null {
  if (status === "unknown") return null;
  if (!user) return access === "public" ? null : "/login";
  // Pending and rejected users see only the waiting page (and its resubmit form).
  const applying = user.status === "pending" || user.status === "rejected";
  if (access === "applicant") return applying ? null : "/";
  if (applying) return "/pending";
  if (access === "public") return "/";
  if (access === "admin" && user.role !== "admin") return "/";
  return null;
}

/**
 * Resolves the session once, then keeps the visitor on routes their session
 * allows. Renders the children only when the visitor may stay.
 */
export function SessionGuard({ access, children }: { access: RouteAccess; children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const status = useAuthStore((s) => s.status);
  const user = useAuthStore((s) => s.user);
  const hydrate = useAuthStore((s) => s.hydrate);
  const target = redirectFor(access, status, user);

  useEffect(() => {
    void hydrate();
  }, [hydrate]);

  useEffect(() => {
    if (target && target !== pathname) router.replace(target);
  }, [target, pathname, router]);

  if (status === "unknown" || target) return null;
  return <>{children}</>;
}
