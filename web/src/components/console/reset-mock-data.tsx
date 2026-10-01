"use client";

import { resetMockData } from "@/lib/api";

/**
 * 重置 Mock 数据, mock mode only: renders nothing in real mode. Offered in the
 * account menu and on the pages outside the console (sign-in, waiting,
 * forced password change), so a stuck demo can always restart.
 */
export function ResetMockDataButton({ className }: { className?: string }) {
  if (!resetMockData) return null;
  return (
    <button type="button" onClick={resetToSeed} className={className}>
      重置 Mock 数据
    </button>
  );
}

/** Restarts from the seed: mock data and this tab's session both go. */
function resetToSeed() {
  resetMockData?.();
  sessionStorage.clear();
  window.location.assign("/login");
}
