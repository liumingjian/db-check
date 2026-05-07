/* eslint-env node */

const fs = require("fs");
const path = require("path");

const ROOT = __dirname;

function cleanEnvValue(value) {
  if (value.length < 2) return value;
  const first = value[0];
  const last = value[value.length - 1];
  if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
    return value.slice(1, -1);
  }
  return value;
}

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const content = fs.readFileSync(filePath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const entry = line.startsWith("export ") ? line.slice(7).trim() : line;
    const eq = entry.indexOf("=");
    if (eq <= 0) continue;
    const key = entry.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;
    if (process.env[key] != null) continue;
    process.env[key] = cleanEnvValue(entry.slice(eq + 1).trim());
  }
}

loadDotEnv(path.join(ROOT, ".env"));

function envOr(name, fallback) {
  const v = process.env[name];
  if (v == null) return fallback;
  const trimmed = String(v).trim();
  return trimmed === "" ? fallback : trimmed;
}

module.exports = {
  apps: [
    {
      name: "dbcheck-api",
      cwd: ROOT,
      script: path.join(ROOT, "scripts", "pm2", "run_api.sh"),
      exec_interpreter: "bash",
      env: {
        DBCHECK_MODE: "dev",
        DBCHECK_ADDR: envOr("DBCHECK_ADDR", "127.0.0.1:8080"),
        DBCHECK_DATA_DIR: envOr("DBCHECK_DATA_DIR", "/tmp/dbcheck-data"),
        ALLOWED_ORIGINS: envOr(
          "ALLOWED_ORIGINS",
          "http://127.0.0.1:3000,http://localhost:3000",
        ),
        DBCHECK_API_TOKEN: envOr("DBCHECK_API_TOKEN", "ATI"),
        // Prefer venv Python if present; can override via env DBCHECK_PYTHON_BIN.
        DBCHECK_PYTHON_BIN: envOr(
          "DBCHECK_PYTHON_BIN",
          path.join(ROOT, ".venv", "bin", "python3"),
        ),
        GOCACHE: envOr("GOCACHE", "/tmp/go-cache"),
      },
      env_production: {
        DBCHECK_MODE: "production",
        NODE_ENV: "production",
      },
      autorestart: true,
      max_restarts: 10,
      restart_delay: 1000,
    },
    {
      name: "dbcheck-web",
      cwd: path.join(ROOT, "web"),
      script: path.join(ROOT, "scripts", "pm2", "run_web.sh"),
      exec_interpreter: "bash",
      env: {
        DBCHECK_MODE: "dev",
        PORT: envOr("PORT", "3000"),
        NEXT_ALLOWED_DEV_ORIGINS: envOr(
          "NEXT_ALLOWED_DEV_ORIGINS",
          envOr("ALLOWED_ORIGINS", ""),
        ),
        // Used by Next.js in dev (and at build-time in prod if you run `npm run build` with it set).
        NEXT_PUBLIC_API_BASE: envOr(
          "NEXT_PUBLIC_API_BASE",
          "",
        ),
      },
      env_production: {
        DBCHECK_MODE: "production",
        NODE_ENV: "production",
      },
      autorestart: true,
      max_restarts: 10,
      restart_delay: 1000,
    },
  ],
};
