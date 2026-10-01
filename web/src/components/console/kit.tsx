/**
 * D3b primitives (spec #19, UI). Colours come from the theme tokens in
 * `globals.css`; motion follows the spec: popovers 150–200 ms with EASE_OUT
 * from scale 0.95, buttons press to 0.97, transitions name their properties.
 */
"use client";

import { useState } from "react";
import { Check, Copy, MoreHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";

export const EASE_OUT = "ease-[cubic-bezier(0.23,1,0.32,1)]";
export const PRESS = `transition-[transform,background-color,color,opacity] duration-150 ${EASE_OUT} active:scale-[0.97]`;
export const POP_IN = `transition-[opacity,transform] duration-150 starting:scale-95 starting:opacity-0 ${EASE_OUT}`;

/** Small uppercase English section word above a headline (Collector, My reports, Admin, ...). */
export const CAPTION = "text-[12px] font-semibold uppercase tracking-[1.5px] text-muted-foreground";

/** The brand mark: three yellow bars. */
export function Bars({ size = 20 }: { size?: number }) {
  const s = size / 20;
  return (
    <span className="flex items-end gap-[3px]" style={{ height: size }} aria-hidden>
      {[12, 20, 16].map((h, i) => (
        <span key={i} className="rounded-[1px] bg-primary" style={{ width: 4 * s, height: h * s }} />
      ))}
    </span>
  );
}

export function Brand() {
  return (
    <span className="flex items-center gap-2.5 text-[15px] font-bold tracking-[-0.3px]">
      <Bars />
      DB-Check
    </span>
  );
}

/** The primary action: solid yellow. */
export function YellowButton({ className, type = "button", ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type={type}
      {...props}
      className={cn(
        "inline-flex h-12 items-center justify-center gap-2 rounded-lg bg-primary px-6 text-[15px] font-semibold text-primary-foreground",
        "cursor-pointer hover:bg-[#e6eb52] disabled:pointer-events-none disabled:opacity-40",
        PRESS,
        className,
      )}
    />
  );
}

/** A filter pill; yellow when on. */
export function Chip({ on, onClick, children }: { on: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      aria-pressed={on}
      onClick={onClick}
      className={cn(
        "cursor-pointer rounded-full px-3 py-1 text-xs font-semibold",
        PRESS,
        on ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
      )}
    >
      {children}
    </button>
  );
}

export interface MenuItem {
  label: string;
  onSelect: () => void;
  danger?: boolean;
}

/** The `···` actions menu. Renders nothing without items. */
export function Menu({ items, label = "更多操作" }: { items: MenuItem[]; label?: string }) {
  const [open, setOpen] = useState(false);
  if (items.length === 0) return null;
  return (
    <div className="relative">
      <button
        type="button"
        aria-label={label}
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className={cn(
          "flex h-7 w-7 cursor-pointer items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground",
          PRESS,
        )}
      >
        <MoreHorizontal className="h-4 w-4" />
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div
            role="menu"
            className={cn(
              "absolute top-full right-0 z-50 mt-1 min-w-36 origin-top-right rounded-xl bg-popover p-1 shadow-lg ring-1 ring-border",
              POP_IN,
            )}
          >
            {items.map((it) => (
              <button
                key={it.label}
                type="button"
                role="menuitem"
                onClick={() => {
                  setOpen(false);
                  it.onSelect();
                }}
                className={cn(
                  "block w-full cursor-pointer rounded-lg px-3 py-1.5 text-left text-sm hover:bg-muted",
                  it.danger && "text-destructive",
                )}
              >
                {it.label}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

/** Mono text that copies itself, e.g. a SHA256 or a command. */
export function CopyText({ text, label, className }: { text: string; label?: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      title={text}
      onClick={() => {
        void navigator.clipboard?.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
      className={cn(
        "inline-flex cursor-pointer items-center gap-1.5 font-mono text-xs text-muted-foreground hover:text-foreground",
        className,
      )}
    >
      {label ?? text}
      {copied ? <Check className="h-3 w-3 text-success" /> : <Copy className="h-3 w-3" />}
    </button>
  );
}
