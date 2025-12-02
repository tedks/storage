#!/usr/bin/env bash
set -euo pipefail

# Refresh vendored submodules, respecting the branch set in .gitmodules
# (expected to be <project>_master).
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ $# -eq 0 ]]; then
  git submodule update --remote --merge --recursive
else
  git submodule update --remote --merge --recursive "$@"
fi
