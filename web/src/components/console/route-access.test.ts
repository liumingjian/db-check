import { describe, expect, it } from "vitest";
import type { User } from "@/lib/auth-types";
import { redirectFor } from "@/components/console/route-access";

function user(fields: Partial<User> = {}): User {
  return { id: "u-1", username: "user", displayName: "张三", role: "user", status: "active", ...fields };
}

describe("redirectFor /change-password (access password)", () => {
  it("sends a visitor without a session to /login", () => {
    expect(redirectFor("password", "anonymous", null)).toBe("/login");
  });

  it("lets an active user stay to change their own password", () => {
    expect(redirectFor("password", "authenticated", user())).toBeNull();
    expect(redirectFor("password", "authenticated", user({ role: "admin" }))).toBeNull();
  });

  it("holds a user with a forced change there, and sends them there from every other route", () => {
    const forced = user({ mustChangePassword: true });
    expect(redirectFor("password", "authenticated", forced)).toBeNull();
    for (const access of ["public", "applicant", "console", "admin"] as const) {
      expect(redirectFor(access, "authenticated", forced)).toBe("/change-password");
    }
  });

  it.each(["pending", "rejected"] as const)("sends a %s applicant to /pending", (status) => {
    expect(redirectFor("password", "authenticated", user({ status }))).toBe("/pending");
  });
});
