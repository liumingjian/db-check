import { describe, expect, it } from "vitest";
import { parseApiMode } from "@/lib/api/mode";

describe("API mode", () => {
  it("runs against the real backend when the variable is unset or blank", () => {
    expect(parseApiMode(undefined)).toBe("real");
    expect(parseApiMode("  ")).toBe("real");
  });

  it("uses the mock only on explicit opt-in", () => {
    expect(parseApiMode("mock")).toBe("mock");
    expect(parseApiMode(" real ")).toBe("real");
  });

  it("refuses any other value instead of guessing", () => {
    expect(() => parseApiMode("Mock")).toThrow(/NEXT_PUBLIC_API_MODE/);
  });
});
