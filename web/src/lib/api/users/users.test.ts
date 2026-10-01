import { describe, expect, it } from "vitest";
import type { DbCheckApi } from "@/lib/api/contract";
import { contractImplementations } from "@/lib/api/testing";
import type { Registration } from "@/lib/api/users/contract";

const applicant: Registration = {
  username: "zhouqi",
  password: "zhouqi-pass",
  displayName: "周七",
  email: "zhouqi@example.com",
  team: "华南交付二部",
  note: "需要 GaussDB 巡检",
};

describe.each(contractImplementations)("%s users contract", (_name, makeApi) => {
  it("registers a pending engineer who is signed in straight away", async () => {
    const api = makeApi();
    const session = await api.users.register(applicant);
    expect(session.user).toMatchObject({ username: "zhouqi", displayName: "周七", role: "user", status: "pending" });
    await expect(api.auth.currentUser(session.token)).resolves.toMatchObject({ status: "pending" });
  });

  it.each([
    ["a taken username", { username: "user" }],
    ["a taken email", { email: "user@example.com" }],
    ["a blank username", { username: "  " }],
    ["a blank display name", { displayName: "" }],
    ["a blank email", { email: "" }],
    ["a blank team", { team: "" }],
    ["a blank password", { password: "" }],
  ])("refuses a registration with %s", async (_case, change) => {
    const api = makeApi();
    await expect(api.users.register({ ...applicant, ...change })).rejects.toMatchObject({ code: "invalid" });
  });

  it("signs in a seed pending applicant as pending", async () => {
    const api = makeApi();
    const session = await api.auth.signIn("lisi", "lisi");
    expect(session.user.status).toBe("pending");
  });

  it("signs in a seed rejected applicant, who can read the reason", async () => {
    const api = makeApi();
    const { token, user } = await api.auth.signIn("zhaoliu", "zhaoliu");
    expect(user.status).toBe("rejected");
    const account = await api.users.myAccount(token);
    expect(account.reason).toBe("外部合作方账号需由项目经理邮件确认后再申请");
  });

  async function adminToken(api: DbCheckApi) {
    return (await api.auth.signIn("admin", "admin")).token;
  }

  async function pendingIds(api: DbCheckApi, token: string) {
    return (await api.users.list(token)).filter((u) => u.status === "pending").map((u) => u.username);
  }

  it("lists a new applicant among the pending accounts for an admin", async () => {
    const api = makeApi();
    await api.users.register(applicant);
    expect(await pendingIds(api, await adminToken(api))).toEqual(expect.arrayContaining(["lisi", "zhouqi"]));
  });

  it("refuses the account list to an engineer", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.users.list(token)).rejects.toMatchObject({ code: "forbidden" });
  });

  it("approves a pending applicant, who is then active and stamped with the acting admin", async () => {
    const api = makeApi();
    const applicantSession = await api.users.register(applicant);
    const admin = await adminToken(api);

    const approved = await api.users.approve(admin, applicantSession.user.id);

    expect(approved).toMatchObject({ status: "active", lastAction: { action: "approve", by: "admin" } });
    expect(await pendingIds(api, admin)).not.toContain("zhouqi");
    await expect(api.auth.currentUser(applicantSession.token)).resolves.toMatchObject({ status: "active" });
  });

  it("rejects a pending applicant with a reason the applicant then reads", async () => {
    const api = makeApi();
    const applicantSession = await api.users.register(applicant);
    const admin = await adminToken(api);

    const rejected = await api.users.reject(admin, applicantSession.user.id, "  请注明负责的客户  ");

    expect(rejected).toMatchObject({ status: "rejected", reason: "请注明负责的客户", lastAction: { action: "reject", by: "admin" } });
    expect(await pendingIds(api, admin)).not.toContain("zhouqi");
    await expect(api.users.myAccount(applicantSession.token)).resolves.toMatchObject({ status: "rejected", reason: "请注明负责的客户" });
  });

  it("refuses a rejection without a reason", async () => {
    const api = makeApi();
    const admin = await adminToken(api);
    await expect(api.users.reject(admin, "u-pending-001", "   ")).rejects.toMatchObject({ code: "invalid" });
    expect(await pendingIds(api, admin)).toContain("lisi");
  });

  it.each([
    ["approve", (api: DbCheckApi, token: string, id: string) => api.users.approve(token, id)],
    ["reject", (api: DbCheckApi, token: string, id: string) => api.users.reject(token, id, "原因")],
  ])("refuses to %s an account that is not pending", async (_action, decide) => {
    const api = makeApi();
    const admin = await adminToken(api);
    await expect(decide(api, admin, "u-user-001")).rejects.toMatchObject({ code: "invalid" });
    await expect(decide(api, admin, "u-rejected-001")).rejects.toMatchObject({ code: "invalid" });
  });

  it("refuses approval and rejection to an engineer", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("user", "user");
    await expect(api.users.approve(token, "u-pending-001")).rejects.toMatchObject({ code: "forbidden" });
    await expect(api.users.reject(token, "u-pending-001", "原因")).rejects.toMatchObject({ code: "forbidden" });
  });

  it("lets a rejected user resubmit with the same username and email, back to pending", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("zhaoliu", "zhaoliu");

    const resubmitted = await api.users.resubmit(token, { displayName: "赵六", team: "华东交付一部", note: "已获项目经理邮件确认" });

    expect(resubmitted).toMatchObject({
      username: "zhaoliu",
      email: "zhaoliu@partner.com",
      team: "华东交付一部",
      note: "已获项目经理邮件确认",
      status: "pending",
    });
    expect(resubmitted.reason).toBeUndefined();
    await expect(api.auth.currentUser(token)).resolves.toMatchObject({ status: "pending" });
    expect(await pendingIds(api, await adminToken(api))).toContain("zhaoliu");
  });

  it("refuses a resubmission from a user who is not rejected", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("lisi", "lisi");
    await expect(api.users.resubmit(token, { displayName: "李四", team: "华南交付二部", note: "" })).rejects.toMatchObject({ code: "invalid" });
  });

  it("refuses a resubmission with a blank display name or team", async () => {
    const api = makeApi();
    const { token } = await api.auth.signIn("zhaoliu", "zhaoliu");
    await expect(api.users.resubmit(token, { displayName: " ", team: "外部合作方", note: "" })).rejects.toMatchObject({ code: "invalid" });
    await expect(api.users.resubmit(token, { displayName: "赵六", team: "", note: "" })).rejects.toMatchObject({ code: "invalid" });
  });

  it("lets an approved applicant sign in as an active engineer", async () => {
    const api = makeApi();
    const { user } = await api.users.register(applicant);
    await api.users.approve(await adminToken(api), user.id);
    await expect(api.auth.signIn("zhouqi", "zhouqi-pass")).resolves.toMatchObject({ user: { role: "user", status: "active" } });
  });

  describe("disable and enable", () => {
    it("disables an active engineer with a reason, stamped with the acting admin", async () => {
      const api = makeApi();
      const disabled = await api.users.disable(await adminToken(api), "u-user-001", "  已转岗  ");
      expect(disabled).toMatchObject({ status: "disabled", reason: "已转岗", lastAction: { action: "disable", by: "admin" } });
      expect(Date.parse(disabled.lastAction!.at)).not.toBeNaN();
      await expect(api.auth.signIn("user", "user")).rejects.toMatchObject({ code: "forbidden" });
    });

    it("ends the sessions a user holds when they are disabled", async () => {
      const api = makeApi();
      const { token } = await api.auth.signIn("user", "user");
      await api.users.disable(await adminToken(api), "u-user-001", "已转岗");
      await expect(api.auth.currentUser(token)).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.releases.list(token)).rejects.toMatchObject({ code: "unauthorized" });
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
      expect(enabled).toMatchObject({ status: "active", lastAction: { action: "enable", by: "admin" } });
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
      expect(promoted).toMatchObject({ role: "admin", lastAction: { action: "promote", by: "admin" } });
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

    it("demotes another admin to engineer, who loses the admin operations", async () => {
      const api = makeApi();
      const admin = await adminToken(api);
      await api.users.promote(admin, "u-user-001");

      const demoted = await api.users.demote(admin, "u-user-001");

      expect(demoted).toMatchObject({ role: "user", lastAction: { action: "demote", by: "admin" } });
      const { token } = await api.auth.signIn("user", "user");
      await expect(api.users.list(token)).rejects.toMatchObject({ code: "forbidden" });
    });

    it("refuses to demote an engineer", async () => {
      const api = makeApi();
      await expect(api.users.demote(await adminToken(api), "u-user-001")).rejects.toMatchObject({ code: "invalid" });
    });
  });

  describe("password reset and forced change", () => {
    it("resets a password to a temporary one that replaces the old password", async () => {
      const api = makeApi();
      const { account, temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");

      expect(account).toMatchObject({ mustChangePassword: true, lastAction: { action: "reset", by: "admin" } });
      expect(temporaryPassword).not.toBe("");
      await expect(api.auth.signIn("user", "user")).rejects.toMatchObject({ code: "unauthorized" });
      await expect(api.auth.signIn("user", temporaryPassword)).resolves.toMatchObject({ user: { mustChangePassword: true } });
    });

    it("holds every session of a reset user to their own account until they change the password", async () => {
      const api = makeApi();
      const { token: earlier } = await api.auth.signIn("user", "user");
      const { temporaryPassword } = await api.users.resetPassword(await adminToken(api), "u-user-001");
      const { token } = await api.auth.signIn("user", temporaryPassword);

      for (const t of [earlier, token]) {
        await expect(api.auth.currentUser(t)).resolves.toMatchObject({ mustChangePassword: true });
        await expect(api.users.myAccount(t)).resolves.toMatchObject({ username: "user" });
        await expect(api.releases.list(t)).rejects.toMatchObject({ code: "forbidden" });
      }
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
        () => api.users.resetPassword(token, "u-admin-001"),
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
