import { create } from "zustand";
import type {
  DbType,
  ZipFileEntry,
  LogEntry,
  ProgressState,
} from "@/lib/types";

const MAX_LOG_LINES = 1000;

interface ReportStore {
  token: string | null;
  setToken: (token: string | null) => void;

  /* Step 1 */
  dbType: DbType | null;
  setDbType: (type: DbType) => void;

  /* Step 2 */
  zipFiles: ZipFileEntry[];
  awrFiles: Record<string, File>; // zipId → awrFile
  addZipFiles: (files: File[]) => void;
  removeZipFile: (id: string) => void;
  setAwrFile: (zipId: string, file: File | null) => void;

  /* Step 3 */
  currentStep: 1 | 2 | 3;
  taskId: string | null;
  progress: ProgressState;
  logs: LogEntry[];
  downloadUrl: string | null;
  isGenerating: boolean;
  isComplete: boolean;
  hasError: boolean;

  /* Navigation */
  nextStep: () => void;
  prevStep: () => void;

  /* Generation */
  setTaskId: (id: string) => void;
  setProgress: (p: Partial<ProgressState>) => void;
  addLog: (entry: LogEntry) => void;
  setDownloadUrl: (url: string) => void;
  setGenerating: (v: boolean) => void;
  setComplete: (v: boolean) => void;
  setHasError: (v: boolean) => void;

  /* Reset */
  reset: () => void;
}

function generateId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

const INITIAL_STATE = {
  token: null,
  dbType: null,
  zipFiles: [],
  awrFiles: {},
  currentStep: 1 as const,
  taskId: null,
  progress: { completed: 0, total: 0, currentFile: "" },
  logs: [],
  downloadUrl: null,
  isGenerating: false,
  isComplete: false,
  hasError: false,
};

export const useReportStore = create<ReportStore>((set) => ({
  ...INITIAL_STATE,

  setToken: (token) => set({ token }),

  setDbType: (type) => set({ dbType: type }),

  addZipFiles: (files) =>
    set((state) => ({
      zipFiles: [
        ...state.zipFiles,
        ...files.map((file) => ({
          id: generateId(),
          file,
          name: file.name,
          size: file.size,
        })),
      ],
    })),

  removeZipFile: (id) =>
    set((state) => {
      const remainingAwrs = { ...state.awrFiles };
      delete remainingAwrs[id];
      return {
        zipFiles: state.zipFiles.filter((z) => z.id !== id),
        awrFiles: remainingAwrs,
      };
    }),

  setAwrFile: (zipId, file) =>
    set((state) => {
      if (file === null) {
        const rest = { ...state.awrFiles };
        delete rest[zipId];
        return { awrFiles: rest };
      }
      return { awrFiles: { ...state.awrFiles, [zipId]: file } };
    }),

  nextStep: () =>
    set((state) => ({
      currentStep: Math.min(state.currentStep + 1, 3) as 1 | 2 | 3,
    })),

  prevStep: () =>
    set((state) => ({
      currentStep: Math.max(state.currentStep - 1, 1) as 1 | 2 | 3,
    })),

  setTaskId: (id) => set({ taskId: id }),

  setProgress: (p) =>
    set((state) => ({
      progress: { ...state.progress, ...p },
    })),

  addLog: (entry) =>
    set((state) => {
      const logs =
        state.logs.length >= MAX_LOG_LINES
          ? [...state.logs.slice(1), entry]
          : [...state.logs, entry];
      return { logs };
    }),

  setDownloadUrl: (url) => set({ downloadUrl: url }),
  setGenerating: (v) => set({ isGenerating: v }),
  setComplete: (v) => set({ isComplete: v }),
  setHasError: (v) => set({ hasError: v }),

  reset: () => set((state) => ({ ...INITIAL_STATE, token: state.token })),
}));
