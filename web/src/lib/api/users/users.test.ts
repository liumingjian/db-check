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
});
