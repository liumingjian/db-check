import type { User } from "@/lib/auth-types";
import { requireSessionUser, resolveSessionUser, startMockSession } from "@/lib/api/auth/mock";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import type { UserProfile, AccountActionKind, UsersApi } from "@/lib/api/users/contract";
import { seedUsers, type MockUser } from "@/lib/api/users/seed";

/** The user records every mock domain reads, e.g. auth to check passwords. */
export function mockUserRecords(ctx: MockContext) {
  return mockCollection<MockUser[]>(ctx.storage, "users", seedUsers);
}

/** Strips the mock-only password before a record leaves the mock. */
export function publicUser({ id, username, displayName, role, status, mustChangePassword }: MockUser): User {
  return { id, username, displayName, role, status, mustChangePassword };
}

function profile(record: MockUser): UserProfile {
  const copy: UserProfile & { password?: string } = { ...record };
  delete copy.password;
  return copy;
}

/** Letters and digits that read unambiguously aloud and on paper (no 0/O, 1/l/I). */
const TEMPORARY_ALPHABET = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789";

/** A temporary password the admin hands over in person. */
function makeTemporaryPassword(): string {
  const picks = crypto.getRandomValues(new Uint32Array(10));
  return Array.from(picks, (n) => TEMPORARY_ALPHABET[n % TEMPORARY_ALPHABET.length]).join("");
}

export function createMockUsers(ctx: MockContext): UsersApi {
  const records = mockUserRecords(ctx);

  function requireAdmin(token: string): User {
    const caller = requireSessionUser(ctx, token);
    if (caller.role !== "admin") throw new ApiError("forbidden", "仅管理员可操作");
    return caller;
  }

  /** Replaces one record with `change(record)` and returns the stored result. */
  function update(userId: string, change: (record: MockUser) => MockUser): UserProfile {
    const all = records.read();
    const index = all.findIndex((u) => u.id === userId);
    if (index < 0) throw new ApiError("not_found", "用户不存在");
    const next = change(all[index]);
    records.write(all.map((u, i) => (i === index ? next : u)));
    return profile(next);
  }

  /**
   * Applies an admin's account action: `change` checks the record and returns
   * the fields to change; the result is stamped with the acting admin and time.
   */
  function administer(
    token: string,
    userId: string,
    action: AccountActionKind,
    change: (record: MockUser, admin: User) => Partial<MockUser>,
  ): UserProfile {
    const admin = requireAdmin(token);
    return update(userId, (record) => ({
      ...record,
      ...change(record, admin),
      actions: [...record.actions, { action, by: admin.username, at: new Date(ctx.now()).toISOString() }],
    }));
  }

  /** An admin's decision on a pending application. */
  function decide(token: string, userId: string, action: "approve" | "reject", reason?: string): UserProfile {
    return administer(token, userId, action, (record) => {
      if (record.status !== "pending") throw new ApiError("invalid", "只能处理待审批的申请");
      return { status: action === "approve" ? "active" : "rejected", reason };
    });
  }

  /** Disable and demote: never on oneself, and never on the last active admin. */
  function keepAdminSeat(record: MockUser, admin: User) {
    if (record.id === admin.id) throw new ApiError("invalid", "不能对自己执行此操作");
    const activeAdmins = records.read().filter((u) => u.role === "admin" && u.status === "active");
    if (activeAdmins.length === 1 && activeAdmins[0].id === record.id) {
      throw new ApiError("invalid", "平台至少要保留一位可用的管理员");
    }
  }

  /** The trimmed reason, once the caller is known to be an admin; `invalid` when blank. */
  function requireReason(token: string, reason: string, missing: string): string {
    requireAdmin(token); // an engineer gets `forbidden`, not a hint about the reason
    const trimmed = reason.trim();
    if (!trimmed) throw new ApiError("invalid", missing);
    return trimmed;
  }

  return {
    async register(registration) {
      const fields = {
        username: registration.username.trim(),
        displayName: registration.displayName.trim(),
        email: registration.email.trim(),
        team: registration.team.trim(),
        note: registration.note.trim(),
      };
      if (!fields.username || !fields.displayName || !fields.email || !fields.team || !registration.password) {
        throw new ApiError("invalid", "请填写用户名、显示名称、邮箱、团队和密码");
      }
      const all = records.read();
      if (all.some((u) => u.username === fields.username)) throw new ApiError("invalid", "用户名已被占用");
      if (all.some((u) => u.email.toLowerCase() === fields.email.toLowerCase())) throw new ApiError("invalid", "邮箱已被注册");

      const user: MockUser = {
        ...fields,
        password: registration.password,
        id: mockId("u"),
        role: "user",
        status: "pending",
        appliedAt: new Date().toISOString(),
        actions: [],
      };
      records.write([...all, user]);
      return startMockSession(ctx, user.id);
    },

    async myProfile(token) {
      const { id } = resolveSessionUser(ctx, token);
      const found = records.read().find((u) => u.id === id);
      if (!found) throw new ApiError("unauthorized", "登录已失效，请重新登录");
      return profile(found);
    },

    async resubmit(token, resubmission) {
      const { id } = resolveSessionUser(ctx, token);
      const displayName = resubmission.displayName.trim();
      const team = resubmission.team.trim();
      if (!displayName || !team) throw new ApiError("invalid", "请填写显示名称和团队");
      return update(id, (record) => {
        if (record.status !== "rejected") throw new ApiError("invalid", "只有被拒绝的申请可以重新提交");
        return {
          ...record,
          displayName,
          team,
          note: resubmission.note.trim(),
          status: "pending",
          reason: undefined,
          appliedAt: new Date().toISOString(),
        };
      });
    },

    async list(token) {
      requireAdmin(token);
      return records
        .read()
        .map(profile)
        .sort((a, b) => b.appliedAt.localeCompare(a.appliedAt));
    },

    async approve(token, userId) {
      return decide(token, userId, "approve");
    },

    async reject(token, userId, reason) {
      return decide(token, userId, "reject", requireReason(token, reason, "请填写拒绝原因"));
    },

    async disable(token, userId, reason) {
      const trimmed = requireReason(token, reason, "请填写禁用原因");
      return administer(token, userId, "disable", (record, admin) => {
        if (record.status !== "active") throw new ApiError("invalid", "只能禁用已启用的账号");
        keepAdminSeat(record, admin);
        return { status: "disabled", reason: trimmed };
      });
    },

    async enable(token, userId) {
      return administer(token, userId, "enable", (record) => {
        if (record.status !== "disabled") throw new ApiError("invalid", "只能启用已禁用的账号");
        return { status: "active", reason: undefined };
      });
    },

    async promote(token, userId) {
      return administer(token, userId, "promote", (record) => {
        if (record.status !== "active" || record.role !== "user") throw new ApiError("invalid", "只能提升已启用的工程师");
        return { role: "admin" };
      });
    },

    async demote(token, userId) {
      return administer(token, userId, "demote", (record, admin) => {
        if (record.status !== "active" || record.role !== "admin") throw new ApiError("invalid", "只能降级已启用的管理员");
        keepAdminSeat(record, admin);
        return { role: "user" };
      });
    },

    async resetPassword(token, userId) {
      const temporaryPassword = makeTemporaryPassword();
      const reset = administer(token, userId, "reset", () => ({ password: temporaryPassword, mustChangePassword: true }));
      return { profile: reset, temporaryPassword };
    },

    async changePassword(token, newPassword, currentPassword) {
      const caller = resolveSessionUser(ctx, token);
      const forced = caller.mustChangePassword;
      if (!forced) requireSessionUser(ctx, token);
      return update(caller.id, (record) => {
        if (!forced && !currentPassword) throw new ApiError("invalid", "请输入当前密码");
        if (!forced && record.password !== currentPassword) throw new ApiError("invalid", "当前密码不正确");
        if (!newPassword.trim()) throw new ApiError("invalid", "请输入新密码");
        if (record.password === newPassword) throw new ApiError("invalid", "新密码不能与当前密码相同");
        return { ...record, password: newPassword, mustChangePassword: undefined };
      });
    },
  };
}
