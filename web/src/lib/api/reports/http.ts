import type { HttpClient } from "@/lib/api/http-client";
import type { DiagnosticValidation, ReportEvent, ReportsApi, ReportTask, ReportTaskInput } from "@/lib/api/reports/contract";
import type { GenerateResponse } from "@/lib/types";

export function createHttpReports(client: HttpClient): ReportsApi {
  return {
    async validate(token, input) {
      const resp = await client.request("附件校验失败", "/api/reports/validate", token, { method: "POST", body: reportForm(input) });
      const body: unknown = await resp.json();
      if (!Array.isArray(body)) throw new Error("附件校验结果格式错误");
      return body.map(parseDiagnosticValidation);
    },
    async generate(token, { items }) {
      const form = reportForm({ items });
      const resp = await client.request("生成接口失败", "/api/reports/generate", token, { method: "POST", body: form });
      const body = (await resp.json()) as GenerateResponse;
      return { taskId: body.task_id, total: body.total };
    },

    async listOwn(token) {
      const resp = await client.request("读取我的报告失败", "/api/reports/mine", token);
      return (await resp.json()) as ReportTask[];
    },

    async listAll(token, { submitterId } = {}) {
      const query = submitterId ? `?${new URLSearchParams({ submitterId })}` : "";
      const resp = await client.request("读取全部报告失败", `/api/reports${query}`, token);
      return (await resp.json()) as ReportTask[];
    },

    async getTask(token, taskId) {
      const resp = await client.request("读取报告任务失败", `/api/reports/tasks/${encodeURIComponent(taskId)}`, token);
      return (await resp.json()) as ReportTask;
    },

    watch(token, taskId, onEvent) {
      let stopped = false;
      const emit = (event: ReportEvent) => {
        if (!stopped) onEvent(event);
      };
      // The backend authenticates WebSockets with the token as subprotocol.
      const ws = new WebSocket(client.wsUrl(`/api/reports/ws/${encodeURIComponent(taskId)}`), [token]);
      ws.onmessage = (ev) => {
        try {
          emit(JSON.parse(String(ev.data)) as ReportEvent);
        } catch {
          // Ignore frames that are not JSON events.
        }
      };
      ws.onerror = () => emit({ type: "error", seq: 0, message: "WebSocket 连接中断" });
      return () => {
        stopped = true;
        ws.close();
      };
    },

    async download(token, taskId) {
      const resp = await client.request("下载失败", `/api/reports/download/${encodeURIComponent(taskId)}`, token);
      return resp.blob();
    },
  };
}

function reportForm({ items }: ReportTaskInput): FormData {
  const form = new FormData();
  items.forEach(({ zip }) => form.append("zips", zip, zip.name));
  items.forEach(({ dbType, diagnostics }, idx) => {
    const field = dbType === "gaussdb" ? "wdr" : "awr";
    diagnostics.forEach((file) => form.append(`${field}_${idx + 1}`, file, file.name));
  });
  return form;
}

function parseDiagnosticValidation(value: unknown): DiagnosticValidation {
  if (typeof value !== "object" || value === null || !("itemPosition" in value) || !("attachmentPosition" in value) || !("fileName" in value) || !("kind" in value)) throw new Error("附件校验结果格式错误");
  const { itemPosition, attachmentPosition, fileName, kind } = value;
  if (typeof itemPosition !== "number" || !Number.isInteger(itemPosition) || itemPosition < 1 || typeof attachmentPosition !== "number" || !Number.isInteger(attachmentPosition) || attachmentPosition < 1 || typeof fileName !== "string") throw new Error("附件校验结果格式错误");
  if (kind === "invalid" && "message" in value && typeof value.message === "string") return { itemPosition, attachmentPosition, fileName, kind, message: value.message };
  if (kind === "checked" && "evidence" in value && (value.evidence === "database_name_dbid" || value.evidence === "database_name" || value.evidence === "unconfirmed")) return { itemPosition, attachmentPosition, fileName, kind, evidence: value.evidence };
  throw new Error("附件校验结果格式错误");
}
