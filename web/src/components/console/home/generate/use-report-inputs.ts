"use client";

import { useState } from "react";
import { useDialogs } from "@/components/console/dialog-host";
import { collectUnpaired, fileKey, inspectDrop, pairDiagnostic, toTaskInput, type InspectedZip } from "@/lib/report-input/inspect";

/**
 * What the drop zone holds before submission: one inspected row per ZIP, the
 * HTML files waiting in 待配对 (Unpaired), and how many drops are still being read.
 */
export function useReportInputs() {
  const { toast } = useDialogs();
  const [items, setItems] = useState<InspectedZip[]>([]);
  const [unpaired, setUnpaired] = useState<File[]>([]);
  const [inspecting, setInspecting] = useState(0);

  async function add(files: File[]) {
    if (files.length === 0) return;
    setUnpaired((current) => [...current, ...collectUnpaired(files, items, current)]);
    setInspecting((n) => n + 1);
    try {
      const fresh = await inspectDrop(files, items);
      // Filter again: another drop may have landed while this one was read.
      setItems((current) => [...current, ...fresh.filter((f) => !current.some((c) => c.file.name === f.file.name))]);
    } finally {
      setInspecting((n) => n - 1);
    }
  }

  /** Pairs the 待配对 file with this key onto the row, or toasts why the row refuses it. */
  function pair(item: InspectedZip, key: string) {
    const file = unpaired.find((f) => fileKey(f) === key);
    if (!file) return;
    const outcome = pairDiagnostic(item, file);
    if (!outcome.ok) {
      toast(outcome.reason);
      return;
    }
    setItems((current) => current.map((c) => (c === item ? outcome.item : c)));
    setUnpaired((current) => current.filter((f) => f !== file));
  }

  function unpair(item: InspectedZip, file: File) {
    setItems((current) => current.map((c) => (c === item ? { ...c, diagnostics: c.diagnostics.filter((d) => d !== file) } : c)));
    setUnpaired((current) => [...current, file]);
  }

  /** Drops the row; its paired files go back to 待配对. */
  function removeItem(item: InspectedZip) {
    setItems((current) => current.filter((c) => c !== item));
    setUnpaired((current) => [...current, ...item.diagnostics]);
  }

  function removeUnpaired(file: File) {
    setUnpaired((current) => current.filter((f) => f !== file));
  }

  function clear() {
    setItems([]);
    setUnpaired([]);
  }

  return {
    items,
    unpaired,
    inspecting: inspecting > 0,
    /** The submission, or `null` while it is blocked. */
    taskInput: toTaskInput(items, unpaired),
    add,
    pair,
    unpair,
    removeItem,
    removeUnpaired,
    clear,
  };
}
