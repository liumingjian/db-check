import type { ReportTask } from "@/lib/api";

/** What a report row flags when something is off; a plain successful task has none (spec #19). */
export type Marker = "processing" | "failed" | "partial" | "expired";

export const MARKER_LABEL: Record<Marker, string> = {
  processing: "生成中",
  failed: "失败",
  partial: "部分失败",
  expired: "已过期",
};

/**
 * The row's markers: its outcome marker (生成中, 失败 or 部分失败), then
 * 已过期. An expired task keeps its failure marker, so its reasons stay
 * one click away.
 */
export function markersFor(task: ReportTask): Marker[] {
  if (task.status === "processing") return ["processing"];
  const partlyFailed = task.items.some((item) => item.outcome.status === "failed");
  const outcome: Marker[] = task.status === "failed" ? ["failed"] : partlyFailed ? ["partial"] : [];
  return task.expired ? [...outcome, "expired"] : outcome;
}

/** The failed items with their reasons, in submission order. */
export function failedItems(task: ReportTask): Array<{ fileName: string; reason: string }> {
  return task.items.flatMap((item) =>
    item.outcome.status === "failed" ? [{ fileName: item.fileName, reason: item.outcome.reason }] : [],
  );
}
