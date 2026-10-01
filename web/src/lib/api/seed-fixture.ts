/**
 * The shared seed fixture: users, collector releases, download records, and
 * report tasks in one JSON file at the repository root,
 * `tests/fixtures/console-seed.json`, so the mock and the Go test server
 * start from identical data.
 *
 * Every time in it is an offset from "now": `now`, or `now-` followed by
 * days, hours, and minutes in that order, each optional (`now-1d2h3m`,
 * `now-45m`). Readers resolve the offsets with `seedTime`.
 */
import fixture from "../../../../tests/fixtures/console-seed.json";
import { MINUTE_MS } from "@/lib/time";

export const seedFixture = fixture;

const OFFSET = /^now(?:-(?=\d)(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?)?$/;

/** Resolves a fixture offset such as `now-1d2h` to an ISO time. */
export function seedTime(offset: string, now: number): string {
  const match = OFFSET.exec(offset);
  if (!match) throw new Error(`seed fixture: malformed time offset "${offset}"`);
  const [days, hours, minutes] = match.slice(1).map((n) => Number(n ?? 0));
  return new Date(now - ((days * 24 + hours) * 60 + minutes) * MINUTE_MS).toISOString();
}
