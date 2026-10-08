"use client";

import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import { Dialog as DialogPrimitive } from "@base-ui/react/dialog";
import { Check, TriangleAlert } from "lucide-react";
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

/** A failure toast carries a warning mark instead of the success check. */
export type ToastTone = "success" | "error";

interface DialogHostApi {
  toast: (message: string, tone?: ToastTone) => void;
  ask: (options: AskOptions) => void;
}

const DialogHostContext = createContext<DialogHostApi | null>(null);

/** Opens dialogs and toasts from anywhere inside `DialogHost`. */
export function useDialogs(): DialogHostApi {
  const host = useContext(DialogHostContext);
  if (!host) throw new Error("useDialogs needs a DialogHost above it");
  return host;
}

/** The open dialog's content; keyed per `ask`, so an input starts blank. */
function Dialog({ options, onClose }: { options: AskOptions; onClose: () => void }) {
  const [value, setValue] = useState("");
  const button = cn("inline-flex h-9 cursor-pointer items-center rounded-lg px-3.5 text-sm font-medium disabled:pointer-events-none disabled:opacity-40", PRESS);

  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Backdrop
        className={cn("fixed inset-0 z-[90] bg-black/50 transition-opacity duration-200 data-ending-style:duration-150", "data-starting-style:opacity-0 data-ending-style:opacity-0", EASE_OUT)}
      />
      {/* Centres the popup; a press here lands outside it and closes the dialog like the backdrop. */}
      <div className="fixed inset-0 z-[90] flex items-center justify-center p-4">
        <DialogPrimitive.Popup
          className={cn(
            "relative w-full max-w-sm rounded-2xl bg-popover p-5 shadow-2xl ring-1 ring-border outline-none",
            "transition-[opacity,scale] duration-200 data-ending-style:duration-150",
            "data-starting-style:scale-95 data-starting-style:opacity-0 data-ending-style:scale-95 data-ending-style:opacity-0",
            "motion-reduce:data-starting-style:scale-100 motion-reduce:data-ending-style:scale-100",
            EASE_OUT,
          )}
        >
          <DialogPrimitive.Title className="text-[15px] font-semibold">{options.title}</DialogPrimitive.Title>
          {options.body && (
            <DialogPrimitive.Description render={<div />} className="mt-1.5 text-sm text-muted-foreground">
              {options.body}
            </DialogPrimitive.Description>
          )}
          {options.input !== undefined && (
            <textarea
              rows={3}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder={options.input}
              className="mt-4 w-full resize-none rounded-lg bg-muted px-3 py-2 text-sm ring-1 ring-transparent outline-none focus:ring-ring"
            />
          )}
          <div className="mt-5 flex justify-end gap-2">
            {options.onConfirm && (
              <DialogPrimitive.Close className={cn(button, "text-muted-foreground hover:bg-muted hover:text-foreground")}>取消</DialogPrimitive.Close>
            )}
            <button
              type="button"
              disabled={options.input !== undefined && !value.trim()}
              onClick={() => {
                // Close first: a confirm that asks again at once (e.g. a follow-up notice) must stay open.
                onClose();
                options.onConfirm?.(value.trim());
              }}
              className={cn(
                button,
                options.danger ? "bg-destructive text-white hover:opacity-90" : "bg-primary text-primary-foreground hover:bg-primary-hover",
              )}
            >
              {options.confirm}
            </button>
          </div>
        </DialogPrimitive.Popup>
      </div>
    </DialogPrimitive.Portal>
  );
}

const TOAST_MS = 4000;

interface Toast {
  id: number;
  message: string;
  tone: ToastTone;
  leaving: boolean;
}

/** True while the tab is in the background. */
function useDocumentHidden(): boolean {
  const [hidden, setHidden] = useState(false);
  useEffect(() => {
    const sync = () => setHidden(document.visibilityState === "hidden");
    sync();
    document.addEventListener("visibilitychange", sync);
    return () => document.removeEventListener("visibilitychange", sync);
  }, []);
  return hidden;
}

/** One toast. Its 4 s count only runs while `paused` is false, then it fades out faster than it came in. */
function ToastItem({ toast, paused, onExpire }: { toast: Toast; paused: boolean; onExpire: (id: number) => void }) {
  const remaining = useRef(TOAST_MS);
  useEffect(() => {
    if (paused || toast.leaving) return;
    const started = Date.now();
    const timer = setTimeout(() => onExpire(toast.id), remaining.current);
    return () => {
      clearTimeout(timer);
      remaining.current -= Date.now() - started;
    };
  }, [paused, toast.leaving, toast.id, onExpire]);

  const error = toast.tone === "error";
  return (
    <div
      className={cn(
        "flex items-start gap-2 rounded-xl bg-popover px-4 py-3 text-sm shadow-lg ring-1 ring-border",
        "transition-[opacity,translate] duration-200 starting:translate-y-2 starting:opacity-0",
        toast.leaving && "translate-y-1 opacity-0 duration-150",
        "motion-reduce:translate-y-0 motion-reduce:starting:translate-y-0",
        EASE_OUT,
      )}
    >
      {error ? <TriangleAlert className="mt-0.5 h-4 w-4 shrink-0 text-destructive" /> : <Check className="mt-0.5 h-4 w-4 shrink-0 text-success" />}
      {toast.message}
    </div>
  );
}

let carriedToast: string | null = null;

/**
 * Shows a toast on the next screen that mounts a `DialogHost`, for screens
 * outside the console shell that end by navigating into it (e.g. /change-password).
 */
export function toastOnNextScreen(message: string) {
  carriedToast = message;
}

/**
 * Hosts the one open dialog and a short toast stack. Toast timers pause while
 * the stack is hovered or the tab is hidden, so no message expires unread.
 */
export function DialogHost({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [hovered, setHovered] = useState(false);
  const hidden = useDocumentHidden();
  // The closing dialog keeps its options until its exit animation ends; a new `ask` replaces them.
  const [dialog, setDialog] = useState<{ id: number; options: AskOptions } | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const seq = useRef(0);

  const [api] = useState<DialogHostApi>(() => ({
    toast(message, tone = "success") {
      const id = ++seq.current;
      setToasts((t) => [...t.slice(-2), { id, message, tone, leaving: false }]);
    },
    ask(options) {
      setDialog({ id: ++seq.current, options });
      setDialogOpen(true);
    },
  }));

  const expire = useCallback((id: number) => {
    setToasts((t) => t.map((x) => (x.id === id ? { ...x, leaving: true } : x)));
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 150);
  }, []);

  useEffect(() => {
    if (!carriedToast) return;
    api.toast(carriedToast);
    carriedToast = null;
  }, [api]);

  return (
    <DialogHostContext.Provider value={api}>
      {children}
      <div
        className="fixed right-5 bottom-5 z-[95] flex w-80 flex-col gap-2"
        aria-live="polite"
        onPointerEnter={() => setHovered(true)}
        onPointerLeave={() => setHovered(false)}
      >
        {toasts.map((t) => (
          <ToastItem key={t.id} toast={t} paused={hovered || hidden} onExpire={expire} />
        ))}
      </div>
      <DialogPrimitive.Root open={dialogOpen} onOpenChange={setDialogOpen}>
        {dialog && <Dialog key={dialog.id} options={dialog.options} onClose={() => setDialogOpen(false)} />}
      </DialogPrimitive.Root>
    </DialogHostContext.Provider>
  );
}
