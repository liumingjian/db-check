"use client";

import { useState } from "react";
import {
  FileBarChart,
  Download,
  Trash2,
  CheckCircle2,
  XCircle,
  Clock,
  Plus,
  Search,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useHistoryStore, type HistoryTask } from "@/stores/history-store";
import { useNavStore } from "@/stores/nav-store";

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function HistoryPage() {
  const tasks = useHistoryStore((s) => s.tasks);
  const removeTask = useHistoryStore((s) => s.removeTask);
  const setActiveTab = useNavStore((s) => s.setActiveTab);
  const [search, setSearch] = useState("");

  const filtered = tasks.filter((t) => {
    if (!search.trim()) return true;
    const q = search.toLowerCase();
    return (
      t.id.toLowerCase().includes(q) ||
      t.fileNames.some((f) => f.toLowerCase().includes(q))
    );
  });

  function handleDownload(task: HistoryTask) {
    // Generate a mock report download blob
    const content = `DB-Check 巡检报告归档包\n任务ID: ${task.id}\n生成时间: ${task.createdAt}\n包含文件: ${task.fileNames.join(", ")}\n状态: ${task.status}\n`;
    const blob = new Blob([content], { type: "application/zip" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `reports-${task.id}.zip`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">任务记录</h1>
          <p className="text-sm text-muted-foreground">
            查看历史巡检分析任务，支持日志回放与报告包二次下载
          </p>
        </div>
        <button
          type="button"
          onClick={() => setActiveTab("report")}
          className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors cursor-pointer self-start sm:self-auto"
        >
          <Plus className="h-4 w-4" />
          新建巡检任务
        </button>
      </div>

      {/* Filter bar */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="搜索任务 ID 或文件名..."
            className="w-full rounded-lg border border-border bg-card pl-9 pr-3 py-2 text-sm outline-none focus:border-primary transition-colors"
          />
        </div>
        <span className="text-xs text-muted-foreground">
          共 {filtered.length} 个历史任务
        </span>
      </div>

      {/* Task list */}
      <div className="space-y-3">
        {filtered.map((task) => (
          <div
            key={task.id}
            className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 rounded-xl border border-border bg-card p-4 transition-colors hover:border-border/80"
          >
            <div className="flex items-start sm:items-center gap-3">
              <div
                className={cn(
                  "flex h-10 w-10 shrink-0 items-center justify-center rounded-lg",
                  task.status === "success"
                    ? "bg-primary/10 text-primary"
                    : task.status === "failed"
                      ? "bg-destructive/10 text-destructive"
                      : "bg-warning/10 text-warning",
                )}
              >
                {task.status === "success" && <CheckCircle2 className="h-5 w-5" />}
                {task.status === "failed" && <XCircle className="h-5 w-5" />}
                {task.status === "processing" && <Clock className="h-5 w-5 animate-spin" />}
              </div>

              <div className="space-y-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="font-mono text-sm font-semibold text-foreground">
                    {task.id}
                  </span>
                  <span
                    className={cn(
                      "rounded-full px-2 py-0.5 text-[10px] font-medium",
                      task.status === "success"
                        ? "bg-primary/10 text-primary"
                        : task.status === "failed"
                          ? "bg-destructive/10 text-destructive"
                          : "bg-warning/10 text-warning",
                    )}
                  >
                    {task.status === "success" ? "已完成" : task.status === "failed" ? "执行失败" : "处理中"}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs text-muted-foreground flex-wrap">
                  <span>{formatDate(task.createdAt)}</span>
                  <span>•</span>
                  <span>包含 {task.totalFiles} 份采集包: {task.fileNames.join(", ")}</span>
                </div>
              </div>
            </div>

            {/* Actions */}
            <div className="flex items-center gap-2 self-end sm:self-center">
              {task.status === "success" && (
                <button
                  type="button"
                  onClick={() => handleDownload(task)}
                  className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-muted hover:text-foreground transition-colors cursor-pointer"
                >
                  <Download className="h-3.5 w-3.5 text-primary" />
                  下载报告
                </button>
              )}
              <button
                type="button"
                onClick={() => removeTask(task.id)}
                className="rounded-md p-1.5 text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors cursor-pointer"
                aria-label="删除记录"
              >
                <Trash2 className="h-4 w-4" />
              </button>
            </div>
          </div>
        ))}

        {filtered.length === 0 && (
          <div className="flex flex-col items-center justify-center gap-3 rounded-xl border border-dashed border-border py-16 text-center">
            <FileBarChart className="h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground">
              {search ? "未找到匹配的历史任务" : "暂无历史巡检任务"}
            </p>
            {!search && (
              <button
                type="button"
                onClick={() => setActiveTab("report")}
                className="text-xs text-primary hover:underline cursor-pointer"
              >
                立即创建第一个巡检任务 →
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
