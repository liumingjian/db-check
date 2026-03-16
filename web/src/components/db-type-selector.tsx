"use client";

import { Database, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { DB_TYPE_OPTIONS, type DbType } from "@/lib/types";

interface DbTypeSelectorProps {
  selected: DbType | null;
  onSelect: (type: DbType) => void;
  onNext: () => void;
}

export function DbTypeSelector({
  selected,
  onSelect,
  onNext,
}: DbTypeSelectorProps) {
  return (
    <div className="flex flex-col items-center gap-10">
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-semibold tracking-tight">
          选择数据库类型
        </h2>
        <p className="text-muted-foreground text-sm">
          请选择本次巡检的数据库类型，同一批次仅支持单一类型
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 w-full max-w-2xl">
        {DB_TYPE_OPTIONS.map((opt) => {
          const isSelected = selected === opt.type;
          return (
            <button
              key={opt.type}
              type="button"
              onClick={() => onSelect(opt.type)}
              className={cn(
                "relative flex flex-col items-center gap-3 rounded-xl border-2 p-6",
                "cursor-pointer transition-all duration-200",
                "hover:border-primary/60 hover:bg-primary/5",
                isSelected
                  ? "border-primary bg-primary/10 shadow-[0_0_24px_-6px] shadow-primary/25"
                  : "border-border bg-card",
              )}
            >
              <Database
                className={cn(
                  "h-8 w-8 transition-colors duration-200",
                  isSelected ? "text-primary" : "text-muted-foreground",
                )}
              />
              <span className="text-lg font-semibold">{opt.label}</span>
              <span className="text-xs text-muted-foreground">
                {opt.versions}
              </span>
              <span className="text-xs text-muted-foreground text-center">
                {opt.description}
              </span>
            </button>
          );
        })}
      </div>

      {selected && (
        <div className="flex flex-col items-center gap-3">
          {DB_TYPE_OPTIONS.find((o) => o.type === selected)?.hasAwrWdr && (
            <p className="text-xs text-accent">
              下一步可为每个 ZIP 包关联{" "}
              {DB_TYPE_OPTIONS.find((o) => o.type === selected)?.awrWdrLabel}
            </p>
          )}
          <button
            type="button"
            onClick={onNext}
            className={cn(
              "inline-flex items-center gap-2 rounded-lg px-6 py-2.5",
              "bg-primary text-primary-foreground font-medium",
              "hover:bg-primary/90 transition-colors duration-200",
              "cursor-pointer",
            )}
          >
            下一步
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}
