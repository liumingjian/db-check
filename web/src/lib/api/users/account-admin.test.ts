import { describe, expect, it } from "vitest";
import type { DbCheckApi } from "@/lib/api/contract";
import { contractImplementationsWithReal } from "@/lib/api/testing";

async function adminToken(api: DbCheckApi) {
  return (await api.auth.signIn("admin", "admin")).token;
}

/** Account administration on approved users: disable, enable, promote, and demote. */
describe.each(contractImplementationsWithReal)("%s users contract: account administration", (_name, makeApi) => {
  describe("disable and enable", () => {
    it("disables an active engineer with a reason, stamped with the acting admin", async () => {
      const api = makeApi();
      const disabled = await api.users.disable(await adminToken(api), "u-user-001", "  已转岗  ");
      expect(disabled).toMatchObject({ status: "disabled", reason: "已转岗" });
      expect(disabled.actions.at(-1)).toMatchObject({ action: "disable", by: "admin" });
      expect(Date.parse(disabled.actions.at(-1)!.at)).not.toBeNaN();
      await expect(api.auth.signIn("user", "user")).rejects.toMatchObject({ code: "forbidden" });
    });

    it("ends the sessions a user holds when they are disabled", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("user", "user");
      await api.users.disable(await adminToken(api), "u-user-001", "已转岗");
      await expect(api.auth.currentUser(token)).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.users.myProfile(token)).rejects.toMatchObject({ code: "unauthorized" });
    });

    it("refuses to disable without a reason", async () => {
      const api = makeApi();
      await expect(api.users.disable(await adminToken(api), "u-user-001", " ")).rejects.toMatchObject({ code: "invalid" });
    });

    it.each([
      ["pending", "u-pending-001"],
      ["rejected", "u-rejected-001"],
      ["disabled", "u-disabled-001"],
    ])("refuses to disable a %s account", async (_status, id) => {
      const api = makeApi();
      await expect(api.users.disable(await adminToken(api), id, "原因")).rejects.toMatchObject({ code: "invalid" });
    });

    it("enables a disabled user, who can sign in again", async () => {
      const api = makeApi();
      const enabled = await api.users.enable(await adminToken(api), "u-disabled-001");
      expect(enabled).toMatchObject({ status: "active" });
      expect(enabled.actions.at(-1)).toMatchObject({ action: "enable", by: "admin" });
      expect(enabled.reason).toBeUndefined();
      await expect(api.auth.signIn("wangwu", "wangwu")).resolves.toMatchObject({ user: { status: "active" } });
    });

    it("refuses to enable an account that is not disabled", async () => {
      const api = makeApi();
      await expect(api.users.enable(await adminToken(api), "u-user-001")).rejects.toMatchObject({ code: "invalid" });
    });
  });

  describe("promote and demote", () => {
    it("promotes an active engineer to admin, who can then administer accounts", async () => {
      const api = makeApi();
      const promoted = await api.users.promote(await adminToken(api), "u-user-001");
      expect(promoted).toMatchObject({ role: "admin" });
      expect(promoted.actions.at(-1)).toMatchObject({ action: "promote", by: "admin" });
      const { token } = await api.auth.signIn("user", "user");
      await expect(api.users.list(token)).resolves.not.toHaveLength(0);
    });

    it.each([
      ["an admin", "u-admin-001"],
      ["a pending applicant", "u-pending-001"],
      ["a disabled user", "u-disabled-001"],
    ])("refuses to promote %s", async (_case, id) => {
      const api = makeApi();
      await expect(api.users.promote(await adminToken(api), id)).rejects.toMatchObject({ code: "invalid" });
    });

    it("demotes another admin to engineer, whose open session loses the admin operations at once", async () => {
      const api = makeApi();
      const admin = await adminToken(api);
      await api.users.promote(admin, "u-user-001");
      const { token } = await api.auth.signIn("user", "user");
      await expect(api.users.list(token)).resolves.not.toHaveLength(0);

      const demoted = await api.users.demote(admin, "u-user-001");

      expect(demoted).toMatchObject({ role: "user" });

      expect(demoted.actions.at(-1)).toMatchObject({ action: "demote", by: "admin" });
      await expect(api.auth.currentUser(token)).resolves.toMatchObject({ role: "user" });
      await expect(api.users.list(token)).rejects.toMatchObject({ code: "forbidden" });
    });

    it("refuses to demote an engineer", async () => {
      const api = makeApi();
      await expect(api.users.demote(await adminToken(api), "u-user-001")).rejects.toMatchObject({ code: "invalid" });
    });
  });
});

/** Password reset, the forced change after it, and the voluntary change. */
describe.each(contractImplementationsWithReal)("%s users contract: passwords", (_name, makeApi) => {
  it("refuses a password reset to an engineer", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.users.resetPassword(token, "u-admin-001")).rejects.toMatchObject({ code: "forbidden" });
  });

  describe("password reset and forced change", () => {
    it("resets a password to a temporary one that replaces the old password", async () => {
      const api = makeApi();
      const { profile, temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");

      expect(profile).toMatchObject({ mustChangePassword: true });

      expect(profile.actions.at(-1)).toMatchObject({ action: "reset", by: "admin" });
      expect(temporaryPassword).not.toBe("");
      await expect(api.auth.signIn("user", "user")).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.auth.signIn("user", temporaryPassword)).resolves.toMatchObject({ user: { mustChangePassword: true } });
    });

    it("ends every session of the reset user", async () => {
      const api = makeApi();
      const { token: earlier } = await api.auth.signIn("user", "user");
      await api.users.resetPassword(await adminToken(api), "u-user-001");

      await expect(api.auth.currentUser(earlier)).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.users.myProfile(earlier)).rejects.toMatchObject({ code: "unauthorized" });
    });

    it("holds a reset user's new session to their own account until they change the password", async () => {
      const api = makeApi();
      const { temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");
      const { token } = await api.auth.signIn("user", temporaryPassword);

      await expect(api.auth.currentUser(token)).resolves.toMatchObject({ mustChangePassword: true });
      await expect(api.users.myProfile(token)).resolves.toMatchObject({ username: "user" });
      await expect(api.releases.list(token)).rejects.toMatchObject({ code: "forbidden" });
    });

    it("ends the forced change once the user sets a new password", async () => {
      const api = makeApi();
      const { temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");
      const { token } = await api.auth.signIn("user", temporaryPassword);

      await api.users.changePassword(token, "my-new-pass");

      expect((await api.auth.currentUser(token)).mustChangePassword).toBeFalsy();
      await expect(api.releases.list(token)).resolves.toBeDefined();
      await expect(api.auth.signIn("user", temporaryPassword)).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.auth.signIn("user", "my-new-pass")).resolves.toMatchObject({ user: { username: "user" } });
    });

    it.each([
      ["a blank password", "  "],
      ["the temporary password again", null],
    ])("refuses %s as the new password", async (_case, next) => {
      const api = makeApi();
      const { temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");
      const { token } = await api.auth.signIn("user", temporaryPassword);
      await expect(api.users.changePassword(token, next ?? temporaryPassword)).rejects.toMatchObject({ code: "invalid" });
      await expect(api.auth.currentUser(token)).resolves.toMatchObject({ mustChangePassword: true });
    });
  });

  describe("voluntary password change", () => {
    it("changes an active user's password given the current one, keeping the session", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("admin", "admin");

      await api.users.changePassword(token, "my-new-pass", "admin");

      await expect(api.releases.list(token)).resolves.toBeDefined();
      await expect(api.auth.signIn("admin", "admin")).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.auth.signIn("admin", "my-new-pass")).resolves.toMatchObject({ user: { username: "admin" } });
    });

    it.each([
      ["no current password", "my-new-pass", undefined, "请输入当前密码"],
      ["a wrong current password", "my-new-pass", "wrong", "当前密码不正确"],
      ["a blank new password", "  ", "user", "请输入新密码"],
      ["the current password again", "user", "user", "新密码不能与当前密码相同"],
    ])("refuses %s", async (_case, next, current, message) => {
      const api = makeApi();
      const { token } = await api.auth.signIn("user", "user");
      await expect(api.users.changePassword(token, next, current)).rejects.toMatchObject({ code: "invalid", message });
      await expect(api.auth.signIn("user", "user")).resolves.toMatchObject({ user: { username: "user" } });
    });

    it("refuses an applicant", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("lisi", "lisi");
      await expect(api.users.changePassword(token, "my-new-pass", "lisi")).rejects.toMatchObject({ code: "forbidden" });
    });
  });

});

describe.each(contractImplementationsWithReal)("%s users contract: account administration guards", (_name, makeApi) => {
  describe("admin guards", () => {
    it.each([
      ["demote", (api: DbCheckApi, token: string, id: string) => api.users.demote(token, id)],
      ["disable", (api: DbCheckApi, token: string, id: string) => api.users.disable(token, id, "原因")],
    ])("refuses an admin who tries to %s themselves, even with another admin around", async (_action, act) => {
      const api = makeApi();
      const admin = await adminToken(api);
      await api.users.promote(admin, "u-user-001");
      await expect(act(api, admin, "u-admin-001")).rejects.toMatchObject({ code: "invalid" });
      await expect(api.auth.currentUser(admin)).resolves.toMatchObject({ role: "admin", status: "active" });
    });

    it("keeps the last active admin: once the seed admin is demoted, the remaining admin cannot step down", async () => {
      const api = makeApi();
      await api.users.promote(await adminToken(api), "u-user-001");
      const { token: second } = await api.auth.signIn("user", "user");
      await api.users.demote(second, "u-admin-001");

      await expect(api.users.demote(second, "u-user-001")).rejects.toMatchObject({ code: "invalid" });
      await expect(api.users.disable(second, "u-user-001", "原因")).rejects.toMatchObject({ code: "invalid" });
      const admins = (await api.users.list(second)).filter((a) => a.role === "admin" && a.status === "active");
      expect(admins.map((a) => a.username)).toEqual(["user"]);
    });

    it("disables another admin, who can no longer sign in", async () => {
      const api = makeApi();
      const admin = await adminToken(api);
      await api.users.promote(admin, "u-user-001");
      await expect(api.users.disable(admin, "u-user-001", "已离职")).resolves.toMatchObject({ role: "admin", status: "disabled" });
      await expect(api.auth.signIn("user", "user")).rejects.toMatchObject({ code: "forbidden" });
    });

    it("refuses every account action to an engineer", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("user", "user");
      for (const act of [
        () => api.users.disable(token, "u-admin-001", "原因"),
        () => api.users.disable(token, "u-admin-001", ""),
        () => api.users.enable(token, "u-disabled-001"),
        () => api.users.promote(token, "u-user-001"),
        () => api.users.demote(token, "u-admin-001"),
      ]) {
        await expect(act()).rejects.toMatchObject({ code: "forbidden" });
      }
    });

    it("refuses an action on an unknown account", async () => {
      const api = makeApi();
      await expect(api.users.enable(await adminToken(api), "u-nobody")).rejects.toMatchObject({ code: "not_found" });
    });
  });
});
