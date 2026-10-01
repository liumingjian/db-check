"use client";

import { useState } from "react";
import { Chip, CopyText } from "@/components/console/kit";
import { DB_LABEL, type DbType } from "@/lib/types";

/** The QUICKSTART commands shipped in each package (`scripts/build_release_packages.sh`). */
const USAGE: Record<DbType, string> = {
  mysql: "./db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password '***' --dbname dbcheck",
  oracle: "./db-collector --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password '***' --dbname ORCL",
  gaussdb: "./db-collector --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password '***' --dbname postgres",
};

const STEPS = ["上传到客户数据库主机并解压", "运行右侧的采集命令", "把 ZIP 拖回本页顶部"];
/** 三步用起来, with the collector command for each database type the release supports. */
export function UsageGuide({ dbTypes }: { dbTypes: DbType[] }) {
  const [picked, setPicked] = useState<DbType | null>(null);
  // A pick the shown release doesn't support falls back to its first type.
  const db = picked && dbTypes.includes(picked) ? picked : dbTypes[0];
  if (!db) return null;
  return (
    <div className="mt-20 grid grid-cols-[1fr_1.6fr] gap-16">
      <div>
        <h3 className="text-2xl font-bold tracking-[-0.3px]">三步用起来</h3>
        <ol className="mt-6 space-y-5">
          {STEPS.map((step, i) => (
            <li key={step} className="flex items-baseline gap-4">
              <span className="text-[32px] leading-none font-bold tracking-[-1px] text-primary">{i + 1}</span>
              <span className="text-base text-[#ccc]">{step}</span>
            </li>
          ))}
        </ol>
      </div>
      <div className="overflow-hidden rounded-2xl bg-card">
        <div className="flex gap-2 px-4 pt-4">
          {dbTypes.map((t) => (
            <Chip key={t} on={db === t} onClick={() => setPicked(t)}>
              {DB_LABEL[t]}
            </Chip>
          ))}
        </div>
        <div className="flex items-start gap-4 p-5">
          <code className="flex-1 font-mono text-sm leading-7 break-all text-[#e6e6e6]">{USAGE[db]}</code>
          <CopyText text={USAGE[db]} label="" />
        </div>
        <p className="px-5 pb-5 text-xs text-muted-foreground">Windows 上运行 db-collector.exe，参数相同。</p>
      </div>
    </div>
  );
}
