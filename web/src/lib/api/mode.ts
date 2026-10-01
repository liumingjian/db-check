export type ApiMode = "mock" | "real";

/**
 * Reads `NEXT_PUBLIC_API_MODE`. Unset or blank means `real`, so a production
 * build that forgets the variable still talks to db-web; the mock needs an
 * explicit `mock` (the `dev:mock` script).
 */
export function parseApiMode(raw: string | undefined): ApiMode {
  const mode = (raw ?? "").trim() || "real";
  if (mode !== "mock" && mode !== "real") {
    throw new Error(`NEXT_PUBLIC_API_MODE must be "mock" or "real", got "${mode}"`);
  }
  return mode;
}
