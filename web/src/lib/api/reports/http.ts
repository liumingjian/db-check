import { httpRequest, wsUrl } from "@/lib/api/http-client";
import type { ReportEvent, ReportsApi } from "@/lib/api/reports/contract";
import type { GenerateResponse } from "@/lib/types";

export function createHttpReports(): ReportsApi {
  return {
    async generate(token, { dbType, items }) {
      const form = new FormData();
      items.forEach(({ zip }) => form.append("zips", zip, zip.name));
      items.forEach(({ diagnostics }, idx) => {
        // Oracle takes one AWR per item, GaussDB any number of WDRs.
        const field = dbType === "gaussdb" ? "wdr" : "awr";
        const selected = dbType === "gaussdb" ? diagnostics : diagnostics.slice(0, 1);
        selected.forEach((file) => form.append(`${field}_${idx + 1}`, file, file.name));
      });
      const resp = await httpRequest("生成接口失败", "/api/reports/generate", token, { method: "POST", body: form });
      const body = (await resp.json()) as GenerateResponse;
      return { taskId: body.task_id, total: body.total };
    },

    watch(token, taskId, onEvent) {
      let stopped = false;
      const emit = (event: ReportEvent) => {
        if (!stopped) onEvent(event);
      };
      // The backend authenticates WebSockets with the token as subprotocol.
      const ws = new WebSocket(wsUrl(`/api/reports/ws/${encodeURIComponent(taskId)}`), [token]);
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
      const resp = await httpRequest("下载失败", `/api/reports/download/${encodeURIComponent(taskId)}`, token);
      return resp.blob();
    },
  };
}
