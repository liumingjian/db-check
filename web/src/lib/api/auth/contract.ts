import type { User } from "@/lib/auth-types";

/** A signed-in user plus the credential every request carries. */
export interface Session {
  token: string;
  user: User;
}

export interface AuthApi {
  /** Rejects with `unauthorized` on an unknown user or a wrong password. */
  signIn(username: string, password: string): Promise<Session>;
}
