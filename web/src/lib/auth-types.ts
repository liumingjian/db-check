export type UserRole = "admin" | "user";

/** Account status (CONTEXT.md): pending → active, or rejected and resubmitted; active ↔ disabled. */
export type AccountStatus = "pending" | "active" | "rejected" | "disabled";

export interface User {
  id: string;
  username: string;
  displayName: string;
  role: UserRole;
  status: AccountStatus;
  /**
   * True after an admin reset the password: until the user sets their own, the
   * console holds them at `/change-password` and only the session and own-account
   * calls answer them.
   */
  mustChangePassword?: boolean;
}
