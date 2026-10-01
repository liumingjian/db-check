import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    // Builds and starts the Go contract test server for the "real" entry.
    globalSetup: ["./src/lib/api/test-server.global-setup.ts"],
    // Suites on the real entry share that one server and reset it per test.
    fileParallelism: false,
  },
});
