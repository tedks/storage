#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/install-bd.sh [options]

Installs the Beads issue tracker CLI (`bd`) on the host (outside Docker/devcontainer).

Options:
  --version <ver>   NPM dist-tag or version (default: latest)
  -h, --help        Show this help

Examples:
  scripts/install-bd.sh
  scripts/install-bd.sh --version 0.30.0
EOF
}

VERSION="latest"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      if [[ -z "$VERSION" ]]; then
        echo "[install-bd][error] --version requires a value" >&2
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[install-bd][error] Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v npm >/dev/null 2>&1; then
  echo "[install-bd][error] npm is required (install Node.js first)" >&2
  exit 1
fi

echo "[install-bd] Installing @beads/bd@$VERSION..."
npm install -g "@beads/bd@$VERSION"

if ! command -v bd >/dev/null 2>&1; then
  echo "[install-bd][error] bd not found in PATH after install" >&2
  exit 1
fi

echo "[install-bd] Installed:"
bd --version
