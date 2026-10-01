import type { User } from "@/lib/auth-types";

/** A mock user record; only the mock ever holds a password. */
export interface MockUser extends User {
  password: string;
}

export function seedUsers(): MockUser[] {
  return [
    { id: "u-admin-001", username: "admin", password: "admin", displayName: "系统管理员", role: "admin" },
    { id: "u-user-001", username: "user", password: "user", displayName: "巡检工程师", role: "user" },
  ];
}
