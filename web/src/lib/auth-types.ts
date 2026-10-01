export type UserRole = "admin" | "user";

/** Account status (CONTEXT.md): pending → active, or rejected and resubmitted; active ↔ disabled. */
export type AccountStatus = "pending" | "active" | "rejected" | "disabled";

export interface User {
  id: string;
  username: string;
  displayName: string;
  role: UserRole;
  status: AccountStatus;
}
