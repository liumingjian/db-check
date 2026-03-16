"use client";

import { cn } from "@/lib/utils";

interface StepIndicatorProps {
  current: 1 | 2 | 3;
}

const STEPS = [
  { step: 1 as const, label: "选择类型" },
  { step: 2 as const, label: "上传文件" },
  { step: 3 as const, label: "生成报告" },
];

export function StepIndicator({ current }: StepIndicatorProps) {
  return (
    <div className="flex items-center justify-center gap-3">
      {STEPS.map(({ step, label }, idx) => (
        <div key={step} className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div
              className={cn(
                "flex h-7 w-7 items-center justify-center rounded-full text-xs font-semibold transition-colors duration-200",
                step === current
                  ? "bg-primary text-primary-foreground"
                  : step < current
                    ? "bg-primary/20 text-primary"
                    : "bg-muted text-muted-foreground",
              )}
            >
              {step}
            </div>
            <span
              className={cn(
                "text-sm transition-colors duration-200",
                step === current
                  ? "text-foreground font-medium"
                  : "text-muted-foreground",
              )}
            >
              {label}
            </span>
          </div>
          {idx < STEPS.length - 1 && (
            <div
              className={cn(
                "h-px w-8 sm:w-12 transition-colors duration-200",
                step < current ? "bg-primary/40" : "bg-border",
              )}
            />
          )}
        </div>
      ))}
    </div>
  );
}
