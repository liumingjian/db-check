"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useAuthStore } from "@/stores/auth-store";
import { redirectFor, type RouteAccess } from "@/components/console/route-access";

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
