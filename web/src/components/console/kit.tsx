/**
 * D3b primitives (spec #19, UI). Colours come from the theme tokens in
 * `globals.css`; motion follows the spec: popovers 150–200 ms with EASE_OUT
 * from scale 0.95, buttons press to 0.97, transitions name their properties.
 * Tailwind v4 writes `scale-*` and `translate-*` to the standalone `scale` and
 * `translate` properties, so transitions name those, not `transform`.
 * Under reduced motion, entrances only fade.
 */
"use client";

import { useEffect, useRef, useState } from "react";
import { Menu as MenuPrimitive } from "@base-ui/react/menu";
import { Check, Copy, MoreHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";

export const EASE_OUT = "ease-[cubic-bezier(0.23,1,0.32,1)]";
export const PRESS = `transition-[scale,background-color,color,opacity] duration-150 ${EASE_OUT} active:scale-[0.97]`;

/** A Base UI popup: grows from its trigger (`--transform-origin`) and shrinks back on close. */
export const POPUP = cn(
  `origin-(--transform-origin) transition-[opacity,scale] duration-150 ${EASE_OUT}`,
  "data-starting-style:scale-95 data-starting-style:opacity-0 data-ending-style:scale-95 data-ending-style:opacity-0",
  "motion-reduce:data-starting-style:scale-100 motion-reduce:data-ending-style:scale-100",
);

/** Small uppercase English section word above a headline (Collector, My reports, Admin, ...). */
export const CAPTION = "text-[12px] font-semibold uppercase tracking-[1.5px] text-muted-foreground";

/** The plain text button of the top bars outside the console (退出登录, 重置 Mock 数据). */
export const TOP_BAR_ACTION = `cursor-pointer text-sm font-semibold text-muted-foreground hover:text-foreground ${PRESS}`;

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
        "cursor-pointer hover:bg-primary-hover disabled:pointer-events-none disabled:opacity-40",
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
  if (items.length === 0) return null;
  return (
    <MenuPrimitive.Root>
      <MenuPrimitive.Trigger
        aria-label={label}
        className={cn(
          "flex h-7 w-7 cursor-pointer items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground data-popup-open:bg-muted data-popup-open:text-foreground",
          PRESS,
        )}
      >
        <MoreHorizontal className="h-4 w-4" />
      </MenuPrimitive.Trigger>
      <MenuPrimitive.Portal>
        <MenuPrimitive.Positioner align="end" sideOffset={4} className="z-50">
          <MenuPrimitive.Popup className={cn("min-w-36 rounded-xl bg-popover p-1 shadow-lg ring-1 ring-border outline-none", POPUP)}>
            {items.map((it) => (
              <MenuPrimitive.Item
                key={it.label}
                onClick={it.onSelect}
                className={cn(
                  "block w-full cursor-pointer rounded-lg px-3 py-1.5 text-left text-sm outline-none data-highlighted:bg-muted",
                  it.danger && "text-destructive",
                )}
              >
                {it.label}
              </MenuPrimitive.Item>
            ))}
          </MenuPrimitive.Popup>
        </MenuPrimitive.Positioner>
      </MenuPrimitive.Portal>
    </MenuPrimitive.Root>
  );
}

const ICON_SWAP = `col-start-1 row-start-1 h-3 w-3 transition-[opacity,scale,filter] duration-150 ${EASE_OUT}`;
const ICON_OUT = "scale-[0.8] opacity-0 blur-[2px]";

/** Mono text that copies itself, e.g. a SHA256 or a command. */
export function CopyText({ text, label, className }: { text: string; label?: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  const reset = useRef<ReturnType<typeof setTimeout>>(undefined);
  useEffect(() => () => clearTimeout(reset.current), []);
  return (
    <button
      type="button"
      title={text}
      onClick={() => {
        void navigator.clipboard?.writeText(text);
        setCopied(true);
        // A repeat click restarts the 1.5 s rather than inheriting the first click's deadline.
        clearTimeout(reset.current);
        reset.current = setTimeout(() => setCopied(false), 1500);
      }}
      className={cn(
        "inline-flex cursor-pointer items-center gap-1.5 font-mono text-xs text-muted-foreground hover:text-foreground",
        PRESS,
        className,
      )}
    >
      {label ?? text}
      <span aria-hidden className="grid">
        <Copy className={cn(ICON_SWAP, copied && ICON_OUT)} />
        <Check className={cn(ICON_SWAP, "text-success", !copied && ICON_OUT)} />
      </span>
    </button>
  );
}
