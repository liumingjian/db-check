import { describe, expect, it } from "vitest";
import { contractImplementations } from "@/lib/api/testing";

describe.each(contractImplementations)("%s auth contract", (_name, makeApi) => {
  it("signs in a seed engineer as an engineer", async () => {
    const api = makeApi();
    const session = await api.auth.signIn("user", "user");
    expect(session.user.username).toBe("user");
    expect(session.user.role).toBe("user");
    expect(session.token).not.toBe("");
  });

  it("refuses a wrong password", async () => {
    const api = makeApi();
    await expect(api.auth.signIn("user", "nope")).rejects.toMatchObject({ code: "unauthorized" });
  });
});
