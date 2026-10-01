import type { UserProfile } from "@/lib/api/users/contract";

/** A mock user record; only the mock ever holds a password. */
export interface MockUser extends UserProfile {
  password: string;
}

/** Spec seed: one admin, one active engineer, one pending, one rejected and one disabled (both with a reason). */
export function seedUsers(): MockUser[] {
  return [
    {
      id: "u-admin-001",
      username: "admin",
      password: "admin",
      displayName: "系统管理员",
      role: "admin",
      status: "active",
      email: "admin@example.com",
      team: "DBA 平台组",
      note: "初始管理员（部署配置）",
      appliedAt: "2026-08-01T09:00:00.000Z",
      actions: [],
    },
    {
      id: "u-user-001",
      username: "user",
      password: "user",
      displayName: "巡检工程师",
      role: "user",
      status: "active",
      email: "user@example.com",
      team: "华东交付一部",
      note: "负责某银行 Oracle 巡检",
      appliedAt: "2026-08-15T10:12:00.000Z",
      actions: [{ action: "approve", by: "admin", at: "2026-08-15T11:00:00.000Z" }],
    },
    {
      id: "u-pending-001",
      username: "lisi",
      password: "lisi",
      displayName: "李四",
      role: "user",
      status: "pending",
      email: "lisi@example.com",
      team: "华南交付二部",
      note: "新入职，需要 GaussDB 巡检",
      appliedAt: "2026-09-30T16:40:00.000Z",
      actions: [],
    },
    {
      id: "u-rejected-001",
      username: "zhaoliu",
      password: "zhaoliu",
      displayName: "赵六",
      role: "user",
      status: "rejected",
      email: "zhaoliu@partner.com",
      team: "外部合作方",
      note: "协助客户巡检",
      appliedAt: "2026-09-20T14:05:00.000Z",
      reason: "外部合作方账号需由项目经理邮件确认后再申请",
      actions: [{ action: "reject", by: "admin", at: "2026-09-21T09:30:00.000Z" }],
    },
    {
      id: "u-disabled-001",
      username: "wangwu",
      password: "wangwu",
      displayName: "王五",
      role: "user",
      status: "disabled",
      email: "wangwu@example.com",
      team: "华北交付三部",
      note: "MySQL 巡检",
      appliedAt: "2026-08-05T08:20:00.000Z",
      reason: "已离职",
      actions: [
        { action: "approve", by: "admin", at: "2026-08-05T09:00:00.000Z" },
        { action: "disable", by: "admin", at: "2026-09-10T10:00:00.000Z" },
      ],
    },
  ];
}
