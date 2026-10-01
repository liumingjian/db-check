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

export type AccountActionKind = "approve" | "reject";

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
  /** The rejection reason while `status` is `rejected`. */
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
}
