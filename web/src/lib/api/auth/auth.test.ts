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

  it("signs in the seed admin as an admin", async () => {
    const api = makeApi();
    const session = await api.auth.signIn("admin", "admin");
    expect(session.user.username).toBe("admin");
    expect(session.user.role).toBe("admin");
  });

  it("refuses a wrong password", async () => {
    const api = makeApi();
    await expect(api.auth.signIn("user", "nope")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it.each([
    ["user", "user"],
    ["admin", "admin"],
  ] as const)("returns the current user for a %s session", async (username, role) => {
    const api = makeApi();
    const { token } = await api.auth.signIn(username, username);
    const user = await api.auth.currentUser(token);
    expect(user).toMatchObject({ username, role });
  });

  it("refuses current-user for an unknown token", async () => {
    const api = makeApi();
    await expect(api.auth.currentUser("no-such-session")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it("refuses a disabled user at sign-in with a message", async () => {
    const api = makeApi();
    await expect(api.auth.signIn("wangwu", "wangwu")).rejects.toMatchObject({ code: "forbidden", message: expect.stringContaining("禁用") });
  });

  it("answers a wrong password for a disabled user as a wrong password", async () => {
    const api = makeApi();
    await expect(api.auth.signIn("wangwu", "nope")).rejects.toMatchObject({ code: "unauthorized" });
  });

  it.each([
    ["pending", "lisi"],
    ["rejected", "zhaoliu"],
  ])("answers current-user and own-account for a %s applicant but refuses the console", async (status, username) => {
    const api = makeApi();
    const { token } = await api.auth.signIn(username, username);
    await expect(api.auth.currentUser(token)).resolves.toMatchObject({ status });
    await expect(api.users.myProfile(token)).resolves.toMatchObject({ status });
    await expect(api.releases.list(token)).rejects.toMatchObject({ code: "forbidden" });
    await expect(api.downloads.records(token)).rejects.toMatchObject({ code: "forbidden" });
    await expect(api.reports.download(token, "t-any")).rejects.toMatchObject({ code: "forbidden" });
  });

  it("ends the session on sign-out", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await api.auth.signOut(token);
    await expect(api.auth.currentUser(token)).rejects.toMatchObject({ code: "unauthorized" });
  });
});
