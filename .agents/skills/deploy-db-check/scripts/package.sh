#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <new-output-directory>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]]; then
  echo "Tracked source has uncommitted changes. Commit the intended deployment first." >&2
  exit 1
fi
if [[ -e "$1" || -L "$1" ]]; then
  echo "Output path already exists. Choose a new directory." >&2
  exit 1
fi

REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD)"
STAGING="$(mktemp -d "${TMPDIR:-/tmp}/dbcheck-package.XXXXXX")"
trap 'rm -rf "$STAGING"' EXIT
mkdir "$STAGING/source" "$STAGING/artifacts"
git -C "$REPO_ROOT" archive --format=tar.gz "$REVISION" > "$STAGING/artifacts/source.tar.gz"
tar -xzf "$STAGING/artifacts/source.tar.gz" -C "$STAGING/source"
(
  cd "$STAGING/source"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$STAGING/artifacts/db-web" ./reporter/cmd/db-web
)
printf '%s\n' "$REVISION" > "$STAGING/artifacts/REVISION"
go version > "$STAGING/artifacts/BUILD_TOOLCHAIN"
(
  cd "$STAGING/artifacts"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum source.tar.gz db-web REVISION BUILD_TOOLCHAIN > SHA256SUMS
  else
    shasum -a 256 source.tar.gz db-web REVISION BUILD_TOOLCHAIN > SHA256SUMS
  fi
)
# Atomic mkdir refuses a concurrent writer and keeps existing paths intact.
mkdir -p "$(dirname "$1")"
mkdir "$1"
cp "$STAGING/artifacts/"* "$1/"
echo "Packaged revision $REVISION into $1"
