#!/usr/bin/env bash
set -euo pipefail

# Ensure we are running from repository root
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

NIX_BIN="${NIX_BIN:-}"

have_working_nix() {
  command -v "${NIX_BIN:-nix}" >/dev/null 2>&1 || return 1
  "${NIX_BIN:-nix}" --version >/dev/null 2>&1 || return 1
  # Quick sanity check: can we read flake metadata (fails fast if daemon permissions are wrong)
  "${NIX_BIN:-nix}" --extra-experimental-features 'nix-command flakes' flake metadata "${repo_root}" --no-warn-dirty >/dev/null 2>&1
}

install_nix_portable() {
  local url="https://github.com/DavHau/nix-portable/releases/download/v021/nix-portable-Linux-x86_64"
  local dest="${repo_root}/.nix-portable"
  echo "Downloading nix-portable..."
  curl -L "$url" -o "$dest"
  chmod +x "$dest"
  NIX_BIN="$dest"
}

echo "Checking for Nix..."
if ! have_working_nix; then
  echo "Nix is not available."
  read -rp "Download and use nix-portable locally? [y/N] " reply
  case "$reply" in
    [yY][eE][sS]|[yY])
      install_nix_portable
      ;;
    *)
      echo "Cannot continue without Nix. Exiting."
      exit 1
      ;;
  esac
else
  NIX_BIN="${NIX_BIN:-nix}"
  echo "Found Nix: $($NIX_BIN --version)"
fi

echo "Entering Nix development shell..."
exec "${NIX_BIN}" --extra-experimental-features "nix-command flakes" develop "${repo_root}"
