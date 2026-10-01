// PROTOTYPE — throwaway. Report generation flow shared by the round-3
// variants: picked files, simulated per-file progress, and a build log.
"use client";

import { useEffect, useState } from "react";
import { formatSize, type ConsoleState, type DbType, type ReportItem } from "./console-state";

export interface Picked {
  name: string;
  size: number;
  db: DbType;
  version: string;
  aux?: string;
}

export interface LogLine {
  t: string;
  text: string;
  tone?: "warn" | "danger" | "ok";
}

export const DEMO_FILES = [
  { name: "bank-ora-01.zip", size: 2_400_000 },
  { name: "mall-mysql.zip", size: 1_100_000 },
  { name: "legacy-ora.zip", size: 1_900_000 },
];

const STAGES = [
  { at: 1, text: (f: Picked) => `上传 ${f.name}（${formatSize(f.size)}）` },
  { at: 25, text: (f: Picked) => `识别 manifest：${f.db} · 采集器 v${f.version}${f.aux ? ` · 附 ${f.aux}` : ""}` },
  { at: 50, text: (f: Picked) => `分析 ${f.name}：${90 + f.name.length * 3} 项检查` },
  { at: 80, text: (f: Picked) => `渲染报告 ${f.name.replace(/\.zip$/, ".docx")}` },
  { at: 100, text: (f: Picked) => `${f.name} 完成` },
];

export function stageOf(p: number): string {
  if (p >= 100) return "完成";
  if (p >= 80) return "生成";
  if (p >= 50) return "分析";
  if (p >= 25) return "识别";
  return "上传";
}

function guessDb(name: string): DbType {
  if (/ora|awr/i.test(name)) return "oracle";
  if (/gauss|wdr/i.test(name)) return "gaussdb";
  return "mysql";
}

export function useReportFlow(state: ConsoleState) {
  const [files, setFiles] = useState<Picked[]>([]);
  const [progress, setProgress] = useState<number[] | null>(null);
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [tick, setTick] = useState(0);
  const [doneId, setDoneId] = useState<string | null>(null);
  const latest = state.latest?.version ?? "1.2.0";

  function add(list: { name: string; size: number }[]) {
    setFiles((fs) => [
      ...fs,
      ...list
        .filter((f) => !fs.some((x) => x.name === f.name))
        .map((f) => ({ ...f, db: guessDb(f.name), version: /legacy|old/i.test(f.name) ? "1.0.0" : latest })),
    ]);
  }

  useEffect(() => {
    if (!progress || doneId) return;
    if (progress.every((p) => p >= 100)) {
      const items: ReportItem[] = files.map((f) => ({
        name: f.name,
        dbType: f.db,
        collectorVersion: f.version,
        outcome: /broken/i.test(f.name) ? "failed" : "success",
      }));
      const t = setTimeout(() => setDoneId(state.addTask(items)), 400);
      return () => clearTimeout(t);
    }
    const t = setTimeout(() => {
      const next = progress.map((p, i) => Math.min(100, p + 3 + ((i * 7 + p) % 8)));
      const stamp = `00:${String(Math.floor((tick + 1) * 0.15)).padStart(2, "0")}.${String(((tick + 1) * 15) % 100).padStart(2, "0")}`;
      const lines: LogLine[] = [];
      files.forEach((f, i) => {
        for (const s of STAGES) {
          if (progress[i] < s.at && next[i] >= s.at) {
            lines.push({ t: stamp, text: s.text(f), tone: s.at === 100 ? "ok" : undefined });
            if (s.at === 25) {
              const n = state.versionNotice(f.version);
              if (n) lines.push({ t: stamp, text: n.text, tone: n.tone });
            }
          }
        }
      });
      setTick((x) => x + 1);
      setProgress(next);
      if (lines.length) setLogs((ls) => [...ls, ...lines]);
    }, 150);
    return () => clearTimeout(t);
  }, [progress, doneId, files, state, tick]);

  const failed = files.filter((f) => /broken/i.test(f.name)).length;

  return {
    files,
    progress,
    logs,
    doneId,
    running: progress !== null && !doneId,
    elapsed: (tick * 0.15).toFixed(1),
    okCount: files.length - failed,
    failedCount: failed,
    add,
    notice: (version: string) => state.versionNotice(version),
    remove: (name: string) => setFiles((fs) => fs.filter((x) => x.name !== name)),
    attach: (name: string) =>
      setFiles((fs) => fs.map((x) => (x.name === name ? { ...x, aux: x.db === "oracle" ? "AWR 报告" : "WDR 报告" } : x))),
    start: () => {
      setLogs([{ t: "00:00.00", text: `开始生成：${files.length} 个采集包` }]);
      setTick(0);
      setProgress(files.map(() => 0));
    },
    reset: () => {
      setFiles([]);
      setProgress(null);
      setLogs([]);
      setDoneId(null);
    },
  };
}

export type ReportFlow = ReturnType<typeof useReportFlow>;
