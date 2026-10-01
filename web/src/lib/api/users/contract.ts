import type { User } from "@/lib/auth-types";
import type { Session } from "@/lib/api/auth/contract";

/** What a new engineer submits on `/register`. Nobody can register as admin. */
export interface Registration {
  username: string;
  password: string;
  displayName: string;
  email: string;
  team: string;
  /** Application note (申请说明); may be empty. */
  note: string;
}

/** A rejected user's new application; username and email stay as registered. */
export type Resubmission = Pick<Registration, "displayName" | "team" | "note">;

export type AccountActionKind = "approve" | "reject" | "disable" | "enable" | "promote" | "demote" | "reset";

/** The audit stamp of the latest admin action on an account. */
export interface AccountAction {
  action: AccountActionKind;
  /** The acting admin's username. */
  by: string;
  /** ISO timestamp. */
  at: string;
}

/** A user plus the profile and status details the account screens show. */
export interface Account extends User {
  email: string;
  team: string;
  note: string;
  /** ISO timestamp of the latest application (registration or resubmission). */
  appliedAt: string;
  /** The admin's reason while `status` is `rejected` or `disabled`. */
  reason?: string;
  lastAction?: AccountAction;
}

export interface UsersApi {
  /**
   * Creates a pending engineer and signs them in, so they land on the waiting
   * page. Rejects with `invalid` on a missing field or a taken username or email.
   */
  register(registration: Registration): Promise<Session>;
  /** The signed-in user's own account, e.g. for the waiting page and the rejection reason. */
  myAccount(token: string): Promise<Account>;
  /** Rejected → pending with the same username and email; `invalid` from any other status. */
  resubmit(token: string, resubmission: Resubmission): Promise<Account>;
  /** Admin only (`forbidden` otherwise): every account, newest application first. */
  list(token: string): Promise<Account[]>;
  /** Admin only: pending → active; `invalid` from any other status. */
  approve(token: string, userId: string): Promise<Account>;
  /** Admin only: pending → rejected; the reason is required (`invalid` when blank) and shown to the user. */
  reject(token: string, userId: string, reason: string): Promise<Account>;

  /*
   * Account administration, admin only. Every action stamps `lastAction`. An admin
   * can neither disable nor demote themselves, and neither action may leave the
   * platform without an active admin (`invalid` in all those cases).
   */

  /** Active → disabled; the reason is required. The user's sign-in is refused, their report tasks are kept. */
  disable(token: string, userId: string, reason: string): Promise<Account>;
  /** Disabled → active. */
  enable(token: string, userId: string): Promise<Account>;
  /** An active engineer becomes an admin. */
  promote(token: string, userId: string): Promise<Account>;
  /** An active admin becomes an engineer. */
  demote(token: string, userId: string): Promise<Account>;
  /**
   * Sets a temporary password for the admin to hand over in person. The user's
   * next session is held at `/change-password` (`mustChangePassword`) until they change it.
   */
  resetPassword(token: string, userId: string): Promise<PasswordReset>;
  /**
   * The caller's own new password; ends a forced password change. Rejects with
   * `invalid` when blank or equal to the current one.
   */
  changePassword(token: string, newPassword: string): Promise<Account>;
}

export interface PasswordReset {
  account: Account;
  temporaryPassword: string;
}
