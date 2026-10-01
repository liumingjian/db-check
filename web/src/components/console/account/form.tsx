"use client";

import { cn } from "@/lib/utils";

/** The text input of the account screens (sign-in, register, resubmit). */
export const INPUT = "h-12 w-full rounded-lg bg-card px-4 text-[15px] ring-1 ring-transparent outline-none placeholder:text-[#5a5a5a] focus:ring-primary";

/** The multi-line variant, for the application note. */
export const TEXTAREA = cn(INPUT, "h-auto resize-none py-3");

/** When an application was submitted, e.g. "9/30 16:40". */
export function appliedAtLabel(iso: string): string {
  return new Date(iso).toLocaleString("zh-CN", { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

/** A form-level error line. */
export function FormError({ message }: { message: string }) {
  if (!message) return null;
  return (
    <p role="alert" className="text-sm text-destructive">
      {message}
    </p>
  );
}
