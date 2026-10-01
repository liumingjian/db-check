/** The home page sections in priority order; each key is also the section's anchor id. */
export const SECTIONS = [
  { key: "new-report", label: "生成报告" },
  { key: "collectors", label: "采集器" },
  { key: "reports", label: "我的报告" },
] as const;

export type SectionKey = (typeof SECTIONS)[number]["key"];

/**
 * Jumps to a section on the home page without animation (frequent navigation
 * never animates) and records it in the URL hash. Off the home page, navigate
 * to `/#<key>` instead.
 */
export function scrollToSection(key: SectionKey): void {
  const el = document.getElementById(key);
  if (!el) return;
  el.scrollIntoView({ behavior: "instant", block: "start" });
  window.history.replaceState(null, "", `#${key}`);
}
