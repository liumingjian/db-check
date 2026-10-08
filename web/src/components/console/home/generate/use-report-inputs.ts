"use client";

import { useState } from "react";
import { useDialogs } from "@/components/console/dialog-host";
import { addDiagnostics, inspectDrop, toTaskInput, type InspectedZip } from "@/lib/report-input/inspect";

/** Primary ZIPs and the optional diagnostics selected on each item. */
export function useReportInputs() {
  const { toast } = useDialogs();
  const [items, setItems] = useState<InspectedZip[]>([]);
  const [inspecting, setInspecting] = useState(0);

  async function add(files: File[]) {
    if (files.length === 0) return;
    if (files.some((file) => !/\.zip$/i.test(file.name))) {
      toast("这里只接受 ZIP 采集包。请先添加 ZIP，识别数据库类型后，在对应采集包上使用「添加 AWR」或「添加 WDR」。");
    }
    setInspecting((n) => n + 1);
    try {
      const fresh = await inspectDrop(files, items);
      setItems((current) => [...current, ...fresh.filter((f) => !current.some((c) => c.file.name === f.file.name))]);
    } finally {
      setInspecting((n) => n - 1);
    }
  }

  function attach(item: InspectedZip, files: File[]) {
    if (files.length === 0) return;
    const outcome = addDiagnostics(item, files);
    if (!outcome.ok) {
      toast(outcome.reason);
      return;
    }
    setItems((current) => current.map((c) => (c === item ? outcome.item : c)));
  }

  function removeDiagnostic(item: InspectedZip, file: File) {
    setItems((current) => current.map((c) => (c === item ? { ...c, diagnostics: c.diagnostics.filter((d) => d !== file) } : c)));
  }

  function removeItem(item: InspectedZip) {
    setItems((current) => current.filter((c) => c !== item));
  }

  function clear() {
    setItems([]);
  }

  const taskInput = toTaskInput(items);
  return { items, inspecting: inspecting > 0, taskInput, add, attach, removeDiagnostic, removeItem, clear };
}
