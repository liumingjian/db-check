"use client";

import { useEffect } from "react";
import { Check } from "lucide-react";
import { ApiError, type UserProfile } from "@/lib/api";
import { cn } from "@/lib/utils";
import { pendingUsers, useUsersStore } from "@/stores/users-store";
import { useAuthStore } from "@/stores/auth-store";
import { appliedAtLabel } from "@/components/console/account/form";
import { UserList } from "@/components/console/admin/user-list";
import { useDialogs } from "@/components/console/dialog-host";
import { PRESS, YellowButton } from "@/components/console/kit";

function ApplicantCard({ applicant, onApprove, onReject }: { applicant: UserProfile; onApprove: () => void; onReject: () => void }) {
  return (
    <div className="flex flex-col rounded-2xl bg-card p-7">
      <p className="text-sm text-muted-foreground tabular-nums">{appliedAtLabel(applicant.appliedAt)} 申请</p>
      <p className="mt-4 text-[32px] leading-none font-bold tracking-[-1px]">{applicant.displayName}</p>
      <p className="mt-2 text-sm text-muted-foreground">
        @{applicant.username} · {applicant.team} · {applicant.email}
      </p>
      {applicant.note && <p className="mt-5 text-base text-[#ccc]">“{applicant.note}”</p>}
      <div className="mt-8 flex items-center gap-6">
        <YellowButton onClick={onApprove}>
          批准 <Check className="h-4 w-4" />
        </YellowButton>
        <button type="button" onClick={onReject} className={cn("cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground", PRESS)}>
          拒绝…
        </button>
      </div>
    </div>
  );
}

/** 管理 → 用户: the pending count as headline, one card per applicant, then the user list. */
export default function AdminUsersPage() {
  const token = useAuthStore((s) => s.token);
  const users = useUsersStore((s) => s.users);
  const load = useUsersStore((s) => s.load);
  const approve = useUsersStore((s) => s.approve);
  const reject = useUsersStore((s) => s.reject);
  const { toast, ask } = useDialogs();

  useEffect(() => {
    if (token) load(token).catch(() => undefined);
  }, [token, load]);

  if (!token || users === null) return null;
  const pending = pendingUsers(users);

  async function decide(action: Promise<unknown>, done: string) {
    try {
      await action;
      toast(done);
    } catch (e) {
      if (!(e instanceof ApiError)) throw e;
      toast(e.message);
    }
  }

  const askReject = (applicant: UserProfile) =>
    ask({
      title: `拒绝 ${applicant.displayName} 的申请`,
      body: "对方登录后会看到原因，可以修改后重新申请。",
      input: "拒绝原因",
      confirm: "拒绝",
      danger: true,
      onConfirm: (reason) => void decide(reject(token, applicant.id, reason), `已拒绝 ${applicant.displayName}`),
    });

  return (
    <div>
      <h1 className="text-[72px] leading-[1.05] font-bold tracking-[-2.5px]">
        {pending.length > 0 ? (
          <>
            <span className="text-primary tabular-nums">{pending.length} 人</span>在等你批准。
          </>
        ) : (
          "没有待审批的申请。"
        )}
      </h1>
      {pending.length === 0 && <p className="mt-4 text-lg text-muted-foreground">新的注册申请会出现在这里。</p>}
      {pending.length > 0 && (
        <div className="mt-12 grid grid-cols-2 gap-4">
          {pending.map((applicant) => (
            <ApplicantCard
              key={applicant.id}
              applicant={applicant}
              onApprove={() => void decide(approve(token, applicant.id), `已批准 ${applicant.displayName}`)}
              onReject={() => askReject(applicant)}
            />
          ))}
        </div>
      )}
      <UserList users={users} />
    </div>
  );
}
