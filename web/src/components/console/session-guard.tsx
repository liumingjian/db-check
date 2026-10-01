"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import type { User } from "@/lib/auth-types";
import { useAuthStore, type AuthStatus } from "@/stores/auth-store";

export type RouteAccess = "public" | "console" | "admin";

/**
 * Where a visitor must go instead of `access`, or null to stay. Every redirect
 * rule of the console lives here (spec #19, Navigation): later account states
 * (pending, rejected, forced password change) add their rules to this function.
 */
export function redirectFor(access: RouteAccess, status: AuthStatus, user: User | null): string | null {
  if (status === "unknown") return null;
  if (access === "public") return user ? "/" : null;
  if (!user) return "/login";
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
