"use client";

import { useRef, useState } from "react";
import { useDialogs } from "@/components/console/dialog-host";
import { api, errorMessage, type DiagnosticValidation } from "@/lib/api";
import { addDiagnostics, inspectDrop, toTaskInput, type InspectedZip } from "@/lib/report-input/inspect";

/** Primary ZIPs and the optional diagnostics selected on each item. */
export function useReportInputs() {
  const { toast } = useDialogs();
  const [items, setItems] = useState<InspectedZip[]>([]);
  const [inspecting, setInspecting] = useState(0);
  const [validating, setValidating] = useState(false);
  const [checks, setChecks] = useState<Array<{ zip: File; file: File; check: DiagnosticValidation }>>([]);
  const revision = useRef(0);

  async function add(files: File[]) {
    if (files.length === 0) return;
    revision.current += 1;
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
    revision.current += 1;
    setItems((current) => current.map((c) => (c === item ? outcome.item : c)));
  }

  function removeDiagnostic(item: InspectedZip, file: File) {
    revision.current += 1;
    setChecks((current) => current.filter((entry) => entry.zip !== item.file || entry.file !== file));
    setItems((current) => current.map((c) => (c === item ? { ...c, diagnostics: c.diagnostics.filter((d) => d !== file) } : c)));
  }

  function removeItem(item: InspectedZip) {
    revision.current += 1;
    setChecks((current) => current.filter((entry) => entry.zip !== item.file));
    setItems((current) => current.filter((c) => c !== item));
  }

  function clear() {
    revision.current += 1;
    setItems([]);
    setChecks([]);
  }

  async function validate(token: string) {
    const input = toTaskInput(items);
    if (!input || validating) return null;
    const startedAt = revision.current;
    setValidating(true);
    try {
      const results = await api.reports.validate(token, input);
      if (revision.current !== startedAt) return null;
      const mapped = results.map((check) => {
        const item = input.items[check.itemPosition - 1];
        const file = item?.diagnostics[check.attachmentPosition - 1];
        if (!item || !file || file.name !== check.fileName) throw new Error("附件校验结果与所选文件不符，请重试。");
        return { zip: item.zip, file, check };
      });
      if (mapped.length !== input.items.reduce((n, item) => n + item.diagnostics.length, 0)) throw new Error("附件校验结果不完整，请重试。");
      setChecks(mapped);
      return results.some((check) => check.kind === "invalid") ? null : input;
    } catch (error) {
      if (revision.current === startedAt) toast(errorMessage(error));
      return null;
    } finally {
      setValidating(false);
    }
  }

  function diagnosticCheck(item: InspectedZip, file: File) {
    return checks.find((entry) => entry.zip === item.file && entry.file === file)?.check ?? null;
  }

  const taskInput = toTaskInput(items);
  return { items, inspecting: inspecting > 0, validating, taskInput, add, attach, removeDiagnostic, removeItem, clear, validate, diagnosticCheck };
}
