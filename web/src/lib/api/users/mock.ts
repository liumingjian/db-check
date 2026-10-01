import type { User } from "@/lib/auth-types";
import { mockCollection, type MockContext } from "@/lib/api/mock-storage";
import type { UsersApi } from "@/lib/api/users/contract";
import { seedUsers, type MockUser } from "@/lib/api/users/seed";

/** The user records every mock domain reads, e.g. auth to check passwords. */
export function mockUserRecords(ctx: MockContext) {
  return mockCollection<MockUser[]>(ctx.storage, "users", seedUsers);
}

/** Strips the mock-only password before a record leaves the mock. */
export function publicUser({ id, username, displayName, role }: MockUser): User {
  return { id, username, displayName, role };
}

export function createMockUsers(): UsersApi {
  return {};
}
