import { seedFixture, seedTime } from "@/lib/api/seed-fixture";
import type { UserProfile } from "@/lib/api/users/contract";

/** A mock user record; only the mock ever holds a password. */
export interface MockUser extends UserProfile {
  password: string;
}

/** The fixture's users: one admin, one active engineer, one pending, one rejected, and one disabled. */
export function seedUsers(now: number): MockUser[] {
  return seedFixture.users.map((user) => ({
    ...user,
    appliedAt: seedTime(user.appliedAt, now),
    actions: user.actions.map((action) => ({ ...action, at: seedTime(action.at, now) })),
  })) as MockUser[];
}
