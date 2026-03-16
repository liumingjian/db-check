"use client";

import { useEffect, useRef } from "react";
import { cn } from "@/lib/utils";
import type { LogEntry, LogLevel } from "@/lib/types";

interface LogTerminalProps {
  logs: LogEntry[];
  className?: string;
}

const LEVEL_COLORS: Record<LogLevel, string> = {
  info: "text-foreground",
  success: "text-primary",
  error: "text-destructive",
  warn: "text-warning",
};

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString("zh-CN", { hour12: false });
  } catch {
    return "--:--:--";
  }
}

export function LogTerminal({ logs, className }: LogTerminalProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const isAutoScrollRef = useRef(true);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !isAutoScrollRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [logs]);

  function handleScroll() {
    const el = containerRef.current;
    if (!el) return;
    const gap = el.scrollHeight - el.scrollTop - el.clientHeight;
    isAutoScrollRef.current = gap < 40;
  }

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className={cn(
        "rounded-lg bg-[#0a0f1a] border border-border p-4",
        "font-mono text-xs leading-5 overflow-y-auto",
        "log-auto-scroll",
        className,
      )}
    >
      {logs.length === 0 && (
        <p className="text-muted-foreground animate-pulse">
          等待日志输出...
        </p>
      )}
      {logs.map((log) => (
        <div key={log.id} className="flex gap-2">
          <span className="shrink-0 text-muted-foreground select-none">
            $
          </span>
          <span className="shrink-0 text-muted-foreground">
            [{formatTime(log.timestamp)}]
          </span>
          <span className={LEVEL_COLORS[log.level]}>{log.message}</span>
        </div>
      ))}
    </div>
  );
}
