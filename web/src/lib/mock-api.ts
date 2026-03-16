import type {
  GenerateResponse,
  WsMessage,
  LogLevel,
} from "@/lib/types";

const MOCK_LOGS: Array<{ level: LogLevel; template: string }> = [
  { level: "info", template: "解析 manifest.json..." },
  { level: "info", template: "校验 result.json schema..." },
  { level: "success", template: "Schema 校验通过" },
  { level: "info", template: "加载规则文件 rule.json..." },
  { level: "info", template: "执行规则分析..." },
  { level: "success", template: "生成 summary.json ✓" },
  { level: "info", template: "构建 ReportView..." },
  { level: "info", template: "渲染 report.docx..." },
  { level: "success", template: "报告生成完成 ✓" },
];

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function timestamp(): string {
  return new Date().toISOString();
}

/**
 * Simulates POST /api/reports/generate.
 * Returns a mock task_id and starts a simulated WebSocket session.
 */
export async function mockGenerate(
  total: number,
): Promise<GenerateResponse> {
  await delay(400);
  const taskId = `mock-${Date.now()}`;
  return {
    task_id: taskId,
    status: "processing",
    total,
    ws_url: `/api/reports/ws/${taskId}`,
  };
}

/**
 * Simulates a WebSocket connection by calling onMessage repeatedly.
 * Returns a cleanup function.
 */
export function mockWebSocket(
  total: number,
  fileNames: string[],
  onMessage: (msg: WsMessage) => void,
): () => void {
  let cancelled = false;

  (async () => {
    for (let i = 0; i < total && !cancelled; i++) {
      const fileName = fileNames[i] ?? `file-${i + 1}.zip`;

      onMessage({
        type: "log",
        timestamp: timestamp(),
        level: "info",
        message: `开始处理 ${fileName}`,
      });

      for (const log of MOCK_LOGS) {
        if (cancelled) return;
        await delay(250);
        onMessage({
          type: "log",
          timestamp: timestamp(),
          level: log.level,
          message: `[${fileName}] ${log.template}`,
        });
      }

      onMessage({
        type: "progress",
        completed: i + 1,
        total,
        current_file: fileName,
      });

      await delay(200);
    }

    if (!cancelled) {
      onMessage({
        type: "log",
        timestamp: timestamp(),
        level: "success",
        message:
          total > 1
            ? `全部 ${total} 份报告生成完成，正在打包...`
            : "报告生成完成",
      });
      await delay(500);
      onMessage({
        type: "done",
        download_url: `/api/reports/download/mock-result`,
      });
    }
  })();

  return () => {
    cancelled = true;
  };
}
