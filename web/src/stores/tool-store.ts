import { create } from "zustand";
import type { ToolRelease, Platform } from "@/lib/tool-types";
import {
  mockGetReleases,
  mockPublishRelease,
  mockRemoveRelease,
} from "@/lib/mock/mock-tools";

interface ToolStore {
  releases: ToolRelease[];
  selectedPlatform: Platform;
  isPublishing: boolean;

  loadReleases: () => void;
  setSelectedPlatform: (p: Platform) => void;
  publishRelease: (version: string, changelog: string) => void;
  removeRelease: (id: string) => void;
}

export const useToolStore = create<ToolStore>((set) => ({
  releases: [],
  selectedPlatform: "linux-amd64",
  isPublishing: false,

  loadReleases: () => {
    set({ releases: mockGetReleases() });
  },

  setSelectedPlatform: (p) => set({ selectedPlatform: p }),

  publishRelease: (version, changelog) => {
    set({ isPublishing: true });
    // Simulate network delay.
    setTimeout(() => {
      mockPublishRelease(version, changelog);
      set({ releases: mockGetReleases(), isPublishing: false });
    }, 800);
  },

  removeRelease: (id) => {
    mockRemoveRelease(id);
    set({ releases: mockGetReleases() });
  },
}));
