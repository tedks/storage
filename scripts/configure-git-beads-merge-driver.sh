#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/configure-git-beads-merge-driver.sh [options]

Configures the repo-local Git merge driver used for Beads issue data:
  .beads/issues.jsonl merge=beads

This installs (or updates) the following git config values in the current repo:
  merge.beads.name
  merge.beads.driver

Options:
  --repo <path>   Path within the repo (default: current directory)
  --force         Overwrite existing merge.beads.* config
  -h, --help      Show this help

Example:
  scripts/configure-git-beads-merge-driver.sh
EOF
}

REPO="."
FORCE="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      REPO="${2:-}"
      if [[ -z "$REPO" ]]; then
        echo "[beads-merge-driver][error] --repo requires a value" >&2
        exit 2
      fi
      shift 2
      ;;
    --force)
      FORCE="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[beads-merge-driver][error] Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v git >/dev/null 2>&1; then
  echo "[beads-merge-driver][error] git is required" >&2
  exit 1
fi

if ! command -v bd >/dev/null 2>&1; then
  echo "[beads-merge-driver][error] bd is required (install Beads first)" >&2
  exit 1
fi

ROOT="$(git -C "$REPO" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$ROOT" ]]; then
  echo "[beads-merge-driver][error] Not in a git repository: $REPO" >&2
  exit 1
fi

EXISTING_DRIVER="$(git -C "$ROOT" config --get merge.beads.driver || true)"
EXISTING_NAME="$(git -C "$ROOT" config --get merge.beads.name || true)"

if [[ "$FORCE" != "1" && -n "$EXISTING_DRIVER" && -n "$EXISTING_NAME" ]]; then
  echo "[beads-merge-driver] Already configured:"
  echo "  merge.beads.name=$EXISTING_NAME"
  echo "  merge.beads.driver=$EXISTING_DRIVER"
  exit 0
fi

git -C "$ROOT" config merge.beads.name "bd JSONL merge driver"
git -C "$ROOT" config merge.beads.driver "bd merge %A %O %A %B"

if [[ -f "$ROOT/.gitattributes" ]] && ! grep -Fx ".beads/issues.jsonl merge=beads" "$ROOT/.gitattributes" >/dev/null 2>&1; then
  echo "[beads-merge-driver][warn] Missing .gitattributes entry for .beads/issues.jsonl (expected merge=beads)" >&2
fi

echo "[beads-merge-driver] Configured:"
git -C "$ROOT" config --get merge.beads.name
git -C "$ROOT" config --get merge.beads.driver
