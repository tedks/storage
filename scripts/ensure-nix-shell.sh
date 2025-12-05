#!/usr/bin/env bash
set -euo pipefail

DEBUG=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug)
      DEBUG=1
      shift
      ;;
    *)
      echo "Usage: $0 [--debug]" >&2
      exit 1
      ;;
  esac
done

if [[ "${DEBUG}" -eq 1 ]]; then
  set -x
fi

# Ensure we are running from repository root
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

NIX_BIN="${NIX_BIN:-}"
NIX_ARGS=()
NIX_PORTABLE_VERSION="${NIX_PORTABLE_VERSION:-v012}"
NIX_PORTABLE_URL="${NIX_PORTABLE_URL:-}"
NIX_INSTALLER_URL="${NIX_INSTALLER_URL:-https://nixos.org/nix/install}"

if [[ -x "${repo_root}/.nix-portable" && ! -e "${repo_root}/nix-portable" ]]; then
  echo "Renaming legacy .nix-portable to nix-portable..."
  mv "${repo_root}/.nix-portable" "${repo_root}/nix-portable"
fi

have_working_nix() {
  local nix_bin="$1"
  shift
  local nix_args=("$@")
  local resolved_bin=""

  if [[ -x "${nix_bin}" ]]; then
    resolved_bin="${nix_bin}"
  else
    resolved_bin="$(command -v "${nix_bin}" 2>/dev/null || true)"
  fi

  if [[ -z "${resolved_bin}" ]]; then
    return 1
  fi

  "${resolved_bin}" "${nix_args[@]}" --version >/dev/null 2>&1 || return 1
  # Quick sanity check: can we read flake metadata (fails fast if daemon permissions are wrong)
  "${resolved_bin}" "${nix_args[@]}" --extra-experimental-features 'nix-command flakes' flake metadata "${repo_root}" --no-warn-dirty >/dev/null 2>&1
}

set_nix_from_candidates() {
  local candidates=()
  if [[ -n "${NIX_BIN}" ]]; then
    candidates+=("${NIX_BIN}")
  fi

  candidates+=("nix")
  candidates+=("${repo_root}/nix-portable")

  for candidate in "${candidates[@]}"; do
    local resolved_candidate="${candidate}"
    if [[ "${candidate}" != /* ]]; then
      resolved_candidate="$(command -v "${candidate}" 2>/dev/null || true)"
    fi

    [[ -z "${resolved_candidate}" ]] && continue

    local candidate_args=()
    if [[ "$(basename "${resolved_candidate}")" == nix-portable* ]]; then
      candidate_args=("nix")
    fi

    if have_working_nix "${resolved_candidate}" "${candidate_args[@]}"; then
      NIX_BIN="${resolved_candidate}"
      NIX_ARGS=("${candidate_args[@]}")
      return 0
    fi
  done

  return 1
}

install_nix_portable() {
  local arch
  arch="$(uname -m)"
  case "${arch}" in
    x86_64) arch="x86_64" ;;
    aarch64) arch="aarch64" ;;
    *)
      echo "Unsupported architecture for nix-portable: ${arch}" >&2
      return 1
      ;;
  esac

  local url="${NIX_PORTABLE_URL:-https://github.com/DavHau/nix-portable/releases/download/${NIX_PORTABLE_VERSION}/nix-portable-${arch}}"
  local dest="${repo_root}/nix-portable"
  local tmp="${dest}.tmp"

  echo "Downloading nix-portable..."
  rm -f "${tmp}"
  if ! curl -fL --retry 3 --retry-delay 1 -o "${tmp}" "${url}"; then
    echo "Failed to download nix-portable from ${url}" >&2
    rm -f "${tmp}"
    return 1
  fi

  mv "${tmp}" "${dest}"
  chmod +x "$dest"

  NIX_BIN="$dest"
  NIX_ARGS=("nix")

  if ! have_working_nix "${NIX_BIN}" "${NIX_ARGS[@]}"; then
    echo "Downloaded nix-portable is not working; please try again." >&2
    rm -f "${dest}"
    return 1
  fi
}

install_nix_global() {
  echo "Installing Nix globally (requires root/sudo)..."
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to install Nix." >&2
    return 1
  fi

  local runner=(sh -s -- --daemon)
  if [[ "${EUID}" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      runner=(sudo "${runner[@]}")
    else
      echo "sudo is required for a global Nix install when not running as root." >&2
      return 1
    fi
  fi

  if ! curl -fL "${NIX_INSTALLER_URL}" | "${runner[@]}"; then
    echo "Nix installation failed." >&2
    return 1
  fi

  local profile="/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"
  if [[ -f "${profile}" ]]; then
    # shellcheck disable=SC1090
    . "${profile}"
  fi

  local nix_cmd
  nix_cmd="$(command -v nix 2>/dev/null || true)"
  if [[ -z "${nix_cmd}" ]]; then
    echo "Nix installed but not yet available in PATH. Please restart your shell." >&2
    return 1
  fi

  NIX_ARGS=()

  if ! have_working_nix "${nix_cmd}" "${NIX_ARGS[@]}"; then
    echo "Nix installation completed, but nix is not working correctly." >&2
    return 1
  fi

  NIX_BIN="${nix_cmd}"
}

echo "Checking for Nix..."
if ! set_nix_from_candidates; then
  echo "Nix is not available."

  echo "Select installation method:"
  echo "  1) Install Nix globally (requires root/sudo, uses ${NIX_INSTALLER_URL})"
  echo "  2) Download nix-portable locally to ${repo_root}/nix-portable"
  echo "  q) Quit"
  read -rp "Choice [1/2/q]: " reply
  case "$reply" in
    1)
      if ! install_nix_global; then
        echo "Could not install Nix globally; exiting." >&2
        exit 1
      fi
      ;;
    2)
      if ! install_nix_portable; then
        echo "Could not install nix-portable; exiting." >&2
        exit 1
      fi
      ;;
    q|Q|"")
      echo "Cannot continue without Nix. Exiting."
      exit 1
      ;;
    *)
      echo "Unrecognized choice. Exiting."
      exit 1
      ;;
  esac
fi

if ! set_nix_from_candidates; then
  echo "Nix is still not available after installation. Exiting." >&2
  exit 1
fi

echo "Found Nix: $("${NIX_BIN}" "${NIX_ARGS[@]}" --version)"

echo "Entering Nix development shell..."
DEV_SHELL_CMD=()
if [[ -n "${NIX_SHELL_COMMAND:-}" ]]; then
  DEV_SHELL_CMD=(--command "${NIX_SHELL_COMMAND}")
fi

exec "${NIX_BIN}" "${NIX_ARGS[@]}" --extra-experimental-features "nix-command flakes" develop "${repo_root}" "${DEV_SHELL_CMD[@]}"
