import { create } from "zustand";
import type {
  DbType,
  ZipFileEntry,
  LogEntry,
  ProgressState,
} from "@/lib/types";

const MAX_LOG_LINES = 1000;
const SUBMISSION_KEY_STORAGE_KEY = "dbcheck_submission_key";

type SubmissionKeyState = {
  key: string;
  fingerprint: string;
};

interface ReportStore {
  token: string | null;
  setToken: (token: string | null) => void;

  /* Step 1 */
  dbType: DbType | null;
  setDbType: (type: DbType) => void;

  /* Step 2 */
  zipFiles: ZipFileEntry[];
  awrFiles: Record<string, File[]>;
  addZipFiles: (files: File[]) => void;
  removeZipFile: (id: string) => void;
  setAwrFile: (zipId: string, files: File[] | null) => void;

  /* Step 3 */
  currentStep: 1 | 2 | 3;
  taskId: string | null;
  submissionKey: string | null;
  submissionFingerprint: string | null;
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
  restoreTask: (id: string) => void;
  beginNewTask: () => void;
  ensureSubmissionKey: () => string;
  clearSubmissionKey: () => void;
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
  submissionKey: null,
  submissionFingerprint: null,
  progress: { completed: 0, total: 0, currentFile: "" },
  logs: [],
  downloadUrl: null,
  isGenerating: false,
  isComplete: false,
  hasError: false,
};

function submissionKeyInvalidationPatch(): Pick<
  ReportStore,
  "submissionKey" | "submissionFingerprint"
> {
  clearSubmissionKeyFromSession();
  return { submissionKey: null, submissionFingerprint: null };
}

export const useReportStore = create<ReportStore>((set, get) => ({
  ...INITIAL_STATE,

  setToken: (token) => set({ token }),

  setDbType: (type) => {
    set({ dbType: type, ...submissionKeyInvalidationPatch() });
  },

  addZipFiles: (files) => {
    const invalidation = submissionKeyInvalidationPatch();
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
      ...invalidation,
    }));
  },

  removeZipFile: (id) => {
    const invalidation = submissionKeyInvalidationPatch();
    set((state) => {
      const remainingAwrs = { ...state.awrFiles };
      delete remainingAwrs[id];
      return {
        zipFiles: state.zipFiles.filter((z) => z.id !== id),
        awrFiles: remainingAwrs,
        ...invalidation,
      };
    });
  },

  setAwrFile: (zipId, files) => {
    const invalidation = submissionKeyInvalidationPatch();
    set((state) => {
      if (files === null || files.length === 0) {
        const rest = { ...state.awrFiles };
        delete rest[zipId];
        return { awrFiles: rest, ...invalidation };
      }
      return {
        awrFiles: { ...state.awrFiles, [zipId]: files },
        ...invalidation,
      };
    });
  },

  nextStep: () =>
    set((state) => ({
      currentStep: Math.min(state.currentStep + 1, 3) as 1 | 2 | 3,
    })),

  prevStep: () =>
    set((state) => ({
      currentStep: Math.max(state.currentStep - 1, 1) as 1 | 2 | 3,
    })),

  setTaskId: (id) => {
    set({ taskId: id, ...submissionKeyInvalidationPatch() });
  },

  restoreTask: (id) => set({ currentStep: 3, taskId: id }),

  beginNewTask: () => {
    const invalidation = submissionKeyInvalidationPatch();
    set((state) => ({
      ...INITIAL_STATE,
      ...invalidation,
      token: state.token,
      dbType: state.dbType,
      currentStep: state.dbType ? 2 : 1,
    }));
  },

  ensureSubmissionKey: () => {
    const state = get();
    const fingerprint = submissionFingerprint(state);
    if (state.submissionKey && state.submissionFingerprint === fingerprint) {
      return state.submissionKey;
    }
    const stored = readSubmissionKeyFromSession();
    if (stored?.fingerprint === fingerprint) {
      set({ submissionKey: stored.key, submissionFingerprint: fingerprint });
      return stored.key;
    }
    const key = createSubmissionKey();
    writeSubmissionKeyToSession({ key, fingerprint });
    set({ submissionKey: key, submissionFingerprint: fingerprint });
    return key;
  },

  clearSubmissionKey: () => {
    set(submissionKeyInvalidationPatch());
  },

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

  reset: () => {
    const invalidation = submissionKeyInvalidationPatch();
    set((state) => ({ ...INITIAL_STATE, ...invalidation, token: state.token }));
  },
}));

function submissionFingerprint(state: Pick<ReportStore, "dbType" | "zipFiles" | "awrFiles">): string {
  return JSON.stringify({
    version: 1,
    dbType: state.dbType,
    items: state.zipFiles.map((zip) => ({
      zip: fileFingerprint(zip.file, zip.name),
      optional: (state.awrFiles[zip.id] ?? []).map((file) => fileFingerprint(file, file.name)),
    })),
  });
}

function fileFingerprint(file: File, name: string) {
  return {
    name,
    size: file.size,
    lastModified: file.lastModified,
    type: file.type,
  };
}

function createSubmissionKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 14)}`;
}

function readSubmissionKeyFromSession(): SubmissionKeyState | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(SUBMISSION_KEY_STORAGE_KEY);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      typeof (parsed as SubmissionKeyState).key !== "string" ||
      typeof (parsed as SubmissionKeyState).fingerprint !== "string"
    ) {
      return null;
    }
    return parsed as SubmissionKeyState;
  } catch {
    return null;
  }
}

function writeSubmissionKeyToSession(value: SubmissionKeyState): void {
  if (typeof window === "undefined") return;
  try {
    sessionStorage.setItem(SUBMISSION_KEY_STORAGE_KEY, JSON.stringify(value));
  } catch {
    // Session storage is optional; the in-memory store still supports retries.
  }
}

function clearSubmissionKeyFromSession(): void {
  if (typeof window === "undefined") return;
  try {
    sessionStorage.removeItem(SUBMISSION_KEY_STORAGE_KEY);
  } catch {
    // Session storage failures must not block a reset or input mutation.
  }
}
