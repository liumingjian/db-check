/**
 * Vitest global setup: builds the Go contract test server
 * (reporter/cmd/db-web-testserver) from this checkout, starts it on a free
 * port, and provides its URL to the "real" contract entry as `testServerUrl`.
 * Needs Go on the machine running `npm test`.
 */
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    testServerUrl: string;
  }
}

const repoRoot = fileURLToPath(new URL("../../../../", import.meta.url));

export default async function setup(project: TestProject) {
  const binDir = mkdtempSync(join(tmpdir(), "db-web-testserver-"));
  const bin = join(binDir, "db-web-testserver");
  execFileSync("go", ["build", "-o", bin, "./reporter/cmd/db-web-testserver"], { cwd: repoRoot, stdio: "inherit" });

  const server = spawn(bin, ["--seed", join(repoRoot, "tests/fixtures/console-seed.json")], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  server.stderr.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  const url = await new Promise<string>((resolve, reject) => {
    server.stdout.on("data", (chunk: Buffer) => {
      const match = /listening on (http:\/\/\S+)/.exec(chunk.toString());
      if (match) resolve(match[1]);
    });
    server.on("exit", (code) => reject(new Error(`db-web-testserver exited with ${code} before listening:\n${stderr}`)));
  });
  project.provide("testServerUrl", url);

  return () => {
    server.kill();
    rmSync(binDir, { recursive: true, force: true });
  };
}
