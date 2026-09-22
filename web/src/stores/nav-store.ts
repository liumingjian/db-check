import { create } from "zustand";

export type NavTab = "report" | "tools" | "history";

interface NavStore {
  activeTab: NavTab;
  setActiveTab: (tab: NavTab) => void;
}

export const useNavStore = create<NavStore>((set) => ({
  activeTab: "report",
  setActiveTab: (tab) => set({ activeTab: tab }),
}));
