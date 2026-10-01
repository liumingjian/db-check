import type { User } from "@/lib/auth-types";
import { requireSessionUser, startMockSession } from "@/lib/api/auth/mock";
import { ApiError } from "@/lib/api/errors";
import { mockCollection, mockId, type MockContext } from "@/lib/api/mock-storage";
import type { Account, AccountActionKind, UsersApi } from "@/lib/api/users/contract";
import { seedUsers, type MockUser } from "@/lib/api/users/seed";

/** The user records every mock domain reads, e.g. auth to check passwords. */
export function mockUserRecords(ctx: MockContext) {
  return mockCollection<MockUser[]>(ctx.storage, "users", seedUsers);
}

/** Strips the mock-only password before a record leaves the mock. */
export function publicUser({ id, username, displayName, role, status }: MockUser): User {
  return { id, username, displayName, role, status };
}

function account(record: MockUser): Account {
  const copy: Account & { password?: string } = { ...record };
  delete copy.password;
  return copy;
}

export function createMockUsers(ctx: MockContext): UsersApi {
  const records = mockUserRecords(ctx);

  function requireAdmin(token: string): User {
    const caller = requireSessionUser(ctx, token);
    if (caller.role !== "admin" || caller.status !== "active") throw new ApiError("forbidden", "仅管理员可操作");
    return caller;
  }

  /** Replaces one record with `change(record)` and returns the stored result. */
  function update(userId: string, change: (record: MockUser) => MockUser): Account {
    const all = records.read();
    const index = all.findIndex((u) => u.id === userId);
    if (index < 0) throw new ApiError("not_found", "用户不存在");
    const next = change(all[index]);
    records.write(all.map((u, i) => (i === index ? next : u)));
    return account(next);
  }

  /** An admin's decision on a pending application, stamped with the acting admin. */
  function decide(token: string, userId: string, action: AccountActionKind, reason?: string): Account {
    const admin = requireAdmin(token);
    return update(userId, (record) => {
      if (record.status !== "pending") throw new ApiError("invalid", "只能处理待审批的申请");
      return {
        ...record,
        status: action === "approve" ? "active" : "rejected",
        reason,
        lastAction: { action, by: admin.username, at: new Date().toISOString() },
      };
    });
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
      };
      records.write([...all, user]);
      return startMockSession(ctx, user.id);
    },

    async myAccount(token) {
      const { id } = requireSessionUser(ctx, token);
      const found = records.read().find((u) => u.id === id);
      if (!found) throw new ApiError("unauthorized", "登录已失效，请重新登录");
      return account(found);
    },

    async resubmit(token, resubmission) {
      const { id } = requireSessionUser(ctx, token);
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
        .map(account)
        .sort((a, b) => b.appliedAt.localeCompare(a.appliedAt));
    },

    async approve(token, userId) {
      return decide(token, userId, "approve");
    },

    async reject(token, userId, reason) {
      requireAdmin(token); // an engineer gets `forbidden`, not a hint about the reason
      const trimmed = reason.trim();
      if (!trimmed) throw new ApiError("invalid", "请填写拒绝原因");
      return decide(token, userId, "reject", trimmed);
    },
  };
}
