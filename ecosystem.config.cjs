/* eslint-env node */

const path = require("path");

const ROOT = __dirname;

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
        DBCHECK_API_TOKEN: envOr("DBCHECK_API_TOKEN", "secret"),
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
        // Used by Next.js in dev (and at build-time in prod if you run `npm run build` with it set).
        NEXT_PUBLIC_API_BASE: envOr(
          "NEXT_PUBLIC_API_BASE",
          "http://127.0.0.1:8080",
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

