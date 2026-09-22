import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";

export interface HistoryTask {
  id: string;
  createdAt: string;
  totalFiles: number;
  fileNames: string[];
  status: "success" | "failed" | "processing";
  downloadUrl?: string | null;
}

interface HistoryStore {
  tasks: HistoryTask[];
  addTask: (task: HistoryTask) => void;
  updateTask: (id: string, partial: Partial<HistoryTask>) => void;
  removeTask: (id: string) => void;
  clearTasks: () => void;
}

const SEED_HISTORY_TASKS: HistoryTask[] = [
  {
    id: "task-seed-001",
    createdAt: "2026-09-20T14:32:00Z",
    totalFiles: 2,
    fileNames: ["mysql-prod-01.zip", "mysql-prod-02.zip"],
    status: "success",
    downloadUrl: "/api/reports/download/task-seed-001",
  },
  {
    id: "task-seed-002",
    createdAt: "2026-09-18T09:15:00Z",
    totalFiles: 1,
    fileNames: ["oracle-core-rac.zip"],
    status: "success",
    downloadUrl: "/api/reports/download/task-seed-002",
  },
];

export const useHistoryStore = create<HistoryStore>()(
  persist(
    (set) => ({
      tasks: SEED_HISTORY_TASKS,

      addTask: (task) =>
        set((state) => ({
          tasks: [task, ...state.tasks.filter((t) => t.id !== task.id)],
        })),

      updateTask: (id, partial) =>
        set((state) => ({
          tasks: state.tasks.map((t) => (t.id === id ? { ...t, ...partial } : t)),
        })),

      removeTask: (id) =>
        set((state) => ({
          tasks: state.tasks.filter((t) => t.id !== id),
        })),

      clearTasks: () => set({ tasks: [] }),
    }),
    {
      name: "dbcheck_history_tasks",
      storage: createJSONStorage(() => sessionStorage),
    },
  ),
);
