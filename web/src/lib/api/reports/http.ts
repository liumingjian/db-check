import type { HttpClient } from "@/lib/api/http-client";
import type { ReportEvent, ReportsApi, ReportTask } from "@/lib/api/reports/contract";
import type { GenerateResponse } from "@/lib/types";

export function createHttpReports(client: HttpClient): ReportsApi {
  return {
    async generate(token, { items }) {
      const form = new FormData();
      items.forEach(({ zip }) => form.append("zips", zip, zip.name));
      items.forEach(({ dbType, diagnostics }, idx) => {
        // The backend reads each ZIP's type itself. Oracle takes one AWR per item, GaussDB any number of WDRs.
        const field = dbType === "gaussdb" ? "wdr" : "awr";
        const selected = dbType === "gaussdb" ? diagnostics : diagnostics.slice(0, 1);
        selected.forEach((file) => form.append(`${field}_${idx + 1}`, file, file.name));
      });
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
