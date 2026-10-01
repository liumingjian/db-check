"use client";

import { createContext, useContext, useEffect, useRef, useState } from "react";
import { Check } from "lucide-react";
import { cn } from "@/lib/utils";
import { EASE_OUT, PRESS } from "@/components/console/kit";

export interface AskOptions {
  title: string;
  body?: React.ReactNode;
  /** Placeholder of a required text input (e.g. a reason); confirm stays disabled while it is blank. */
  input?: string;
  confirm: string;
  danger?: boolean;
  /** Receives the trimmed input, or "" without one. Omit for a plain notice. */
  onConfirm?: (value: string) => void;
}

interface DialogHostApi {
  toast: (message: string) => void;
  ask: (options: AskOptions) => void;
}

const DialogHostContext = createContext<DialogHostApi | null>(null);

/** Opens dialogs and toasts from anywhere inside `DialogHost`. */
export function useDialogs(): DialogHostApi {
  const host = useContext(DialogHostContext);
  if (!host) throw new Error("useDialogs needs a DialogHost above it");
  return host;
}

function Dialog({ options, onClose }: { options: AskOptions; onClose: () => void }) {
  const [value, setValue] = useState("");
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const button = cn("inline-flex h-9 cursor-pointer items-center rounded-lg px-3.5 text-sm font-medium disabled:pointer-events-none disabled:opacity-40", PRESS);

  return (
    <div className="fixed inset-0 z-[90] flex items-center justify-center p-4">
      <div onClick={onClose} className="absolute inset-0 bg-black/50 transition-opacity duration-200 starting:opacity-0" />
      <div
        role="dialog"
        aria-modal
        aria-label={options.title}
        className={cn(
          "relative w-full max-w-sm rounded-2xl bg-popover p-5 shadow-2xl ring-1 ring-border",
          "transition-[opacity,transform] duration-200 starting:scale-95 starting:opacity-0",
          EASE_OUT,
        )}
      >
        <h2 className="text-[15px] font-semibold">{options.title}</h2>
        {options.body && <div className="mt-1.5 text-sm text-muted-foreground">{options.body}</div>}
        {options.input !== undefined && (
          <textarea
            autoFocus
            rows={3}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder={options.input}
            className="mt-4 w-full resize-none rounded-lg bg-muted px-3 py-2 text-sm ring-1 ring-transparent outline-none focus:ring-ring"
          />
        )}
        <div className="mt-5 flex justify-end gap-2">
          {options.onConfirm && (
            <button type="button" onClick={onClose} className={cn(button, "text-muted-foreground hover:bg-muted hover:text-foreground")}>
              取消
            </button>
          )}
          <button
            type="button"
            disabled={options.input !== undefined && !value.trim()}
            onClick={() => {
              options.onConfirm?.(value.trim());
              onClose();
            }}
            className={cn(
              button,
              options.danger ? "bg-destructive text-white hover:opacity-90" : "bg-primary text-primary-foreground hover:bg-[#e6eb52]",
            )}
          >
            {options.confirm}
          </button>
        </div>
      </div>
    </div>
  );
}

/** Hosts the one open dialog and a short toast stack. */
export function DialogHost({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<{ id: number; message: string }[]>([]);
  const [dialog, setDialog] = useState<AskOptions | null>(null);
  const seq = useRef(0);

  const [api] = useState<DialogHostApi>(() => ({
    toast(message) {
      const id = ++seq.current;
      setToasts((t) => [...t.slice(-2), { id, message }]);
      setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4000);
    },
    ask: setDialog,
  }));

  return (
    <DialogHostContext.Provider value={api}>
      {children}
      <div className="fixed right-5 bottom-5 z-[95] flex w-80 flex-col gap-2" aria-live="polite">
        {toasts.map((t) => (
          <div
            key={t.id}
            className={cn(
              "flex items-center gap-2 rounded-xl bg-popover px-4 py-3 text-sm shadow-lg ring-1 ring-border",
              "transition-[opacity,transform] duration-200 starting:translate-y-2 starting:opacity-0",
              EASE_OUT,
            )}
          >
            <Check className="h-4 w-4 shrink-0 text-success" />
            {t.message}
          </div>
        ))}
      </div>
      {dialog && <Dialog options={dialog} onClose={() => setDialog(null)} />}
    </DialogHostContext.Provider>
  );
}
