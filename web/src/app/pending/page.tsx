"use client";

import { useEffect, useState } from "react";
import { ArrowRight } from "lucide-react";
import { api, ApiError, type Account, type Resubmission } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";
import { appliedAtLabel, FormError, INPUT, TEXTAREA } from "@/components/console/account/form";
import { Brand, CAPTION, PRESS, YellowButton } from "@/components/console/kit";
import { SessionGuard } from "@/components/console/session-guard";

const HEADLINE = "mt-4 text-[88px] leading-[1.02] font-bold tracking-[-3px]";

function Waiting({ account }: { account: Account }) {
  const refresh = useAuthStore((s) => s.refresh);
  return (
    <div>
      <p className={CAPTION}>Pending</p>
      <h1 className={HEADLINE}>
        申请已提交，
        <br />
        <span className="text-primary">等管理员批准。</span>
      </h1>
      <p className="mt-8 max-w-md text-lg leading-relaxed text-[#ccc]">
        {account.displayName}，你在 {appliedAtLabel(account.appliedAt)} 提交了申请。批准后刷新状态就能开始用。
      </p>
      {/* An approved user leaves this page: the session guard sends active users home. */}
      <YellowButton onClick={() => void refresh()} className="mt-10">
        刷新状态
      </YellowButton>
    </div>
  );
}

function Rejected({ account }: { account: Account }) {
  const token = useAuthStore((s) => s.token);
  const refresh = useAuthStore((s) => s.refresh);
  const [form, setForm] = useState<Resubmission>({ displayName: account.displayName, team: account.team, note: account.note });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!token) return;
    setBusy(true);
    try {
      await api.users.resubmit(token, form);
      await refresh();
    } catch (err) {
      if (!(err instanceof ApiError)) throw err;
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <div>
        <p className={CAPTION}>Rejected</p>
        <h1 className={HEADLINE}>这次没通过。</h1>
        <p className="mt-8 max-w-md border-l-2 border-destructive pl-4 text-lg leading-relaxed text-[#ccc]">{account.reason}</p>
      </div>
      <form onSubmit={submit} className="flex flex-col gap-3">
        <p className="mb-2 text-base text-muted-foreground">
          改一下再申请，用户名（@{account.username}）和邮箱（{account.email}）不变。
        </p>
        <input aria-label="显示名称" placeholder="显示名称" value={form.displayName} onChange={(e) => setForm({ ...form, displayName: e.target.value })} className={INPUT} />
        <input aria-label="团队" placeholder="团队" value={form.team} onChange={(e) => setForm({ ...form, team: e.target.value })} className={INPUT} />
        <textarea aria-label="申请说明" placeholder="申请说明" rows={3} value={form.note} onChange={(e) => setForm({ ...form, note: e.target.value })} className={TEXTAREA} />
        <FormError message={error} />
        <YellowButton type="submit" disabled={busy} className="mt-2 h-14 w-full text-base">
          重新提交申请 <ArrowRight className="h-4 w-4" />
        </YellowButton>
      </form>
    </>
  );
}

/** `/pending`: the waiting page for pending users, the reason and resubmit form for rejected ones. */
function ApplicationGate() {
  const token = useAuthStore((s) => s.token);
  const status = useAuthStore((s) => s.user?.status);
  const logout = useAuthStore((s) => s.logout);
  const [account, setAccount] = useState<Account | null>(null);

  // Re-read the account whenever the status changes, e.g. after a resubmission.
  useEffect(() => {
    if (!token) return;
    let live = true;
    api.users
      .myAccount(token)
      .then((a) => live && setAccount(a))
      .catch(() => undefined);
    return () => {
      live = false;
    };
  }, [token, status]);

  return (
    <div className="flex min-h-screen flex-col">
      <div className="mx-auto flex h-20 w-full max-w-[1240px] items-center justify-between px-8">
        <Brand />
        <button type="button" onClick={() => void logout()} className={cn("cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground", PRESS)}>
          退出登录
        </button>
      </div>
      {account && (
        <div className="mx-auto grid w-full max-w-[1240px] flex-1 grid-cols-[1.3fr_1fr] items-center gap-16 px-8 pb-32">
          {account.status === "rejected" ? <Rejected key={account.appliedAt} account={account} /> : <Waiting account={account} />}
        </div>
      )}
    </div>
  );
}

export default function PendingPage() {
  return (
    <SessionGuard access="applicant">
      <ApplicationGate />
    </SessionGuard>
  );
}
