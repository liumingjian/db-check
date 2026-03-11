#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2
  exit 1
fi

python3 -m pip install --upgrade pip
python3 -m pip install -r "$ROOT_DIR/requirements.txt"
