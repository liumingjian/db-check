import type { User } from "@/lib/auth-types";

/** A signed-in user plus the credential every request carries. */
export interface Session {
  token: string;
  user: User;
}

export interface AuthApi {
  /** Rejects with `unauthorized` on an unknown user or a wrong password. */
  signIn(username: string, password: string): Promise<Session>;
  /** The user behind a session token; rejects with `unauthorized` once the session is gone. */
  currentUser(token: string): Promise<User>;
  /** Ends the session; later calls with this token reject with `unauthorized`. */
  signOut(token: string): Promise<void>;
}
