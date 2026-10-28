#!/usr/bin/env bash
# Run every quality gate (fast, mounted, resilience and remote) before a push.
#
#   scripts/run-gates.sh [extra bazel test flags]
#
# With a usable Docker daemon everything runs here, through the all-gates
# suite. Without one, the remote gate (which needs Docker) runs over ssh on
# the host named by STORAGE_REMOTE_GATE_HOST, at the exact commit you pushed,
# in a scratch checkout that is removed afterwards; the other three gates
# still run here. With neither, this fails: a skipped gate must be a
# decision, not an accident.
#
#   STORAGE_REMOTE_GATE_HOST  ssh destination of a host with Docker and the
#                             dev shell. Unset by default.
#   STORAGE_REMOTE_GATE_URL   where that host fetches the commit from.
#                             Default: this checkout's origin URL, which the
#                             host must be able to read (a public https URL
#                             always works).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

die() {
  echo "run-gates: $*" >&2
  exit 1
}

bazel_test() {
  nix develop --command bazel test "$@" --test_output=errors
}

docker_usable() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

# Runs on the remote host, fed to `bash -l -s` so nix is on PATH. Everything
# that reads stdin gets /dev/null: stdin is this script.
remote_script() {
  cat <<'REMOTE'
set -euo pipefail
url=$1
sha=$2
shift 2
scratch=$(mktemp -d "${TMPDIR:-/tmp}/storage-remote-gate.XXXXXX")
cleanup() {
  # Free the bazel output base this checkout created, then the checkout.
  if [ -f "$scratch/MODULE.bazel" ]; then
    (cd "$scratch" && nix develop --command bazel clean --expunge </dev/null) || true
  fi
  case $scratch in
    */storage-remote-gate.??????)
      find "$scratch" -mindepth 1 -delete
      rmdir "$scratch"
      ;;
  esac
}
trap cleanup EXIT
cd "$scratch"
git init -q . </dev/null
git fetch -q --depth 1 "$url" "$sha" </dev/null
git checkout -q --detach FETCH_HEAD </dev/null
[ "$(git rev-parse HEAD)" = "$sha" ] || { echo "fetched commit is not $sha" >&2; exit 1; }
nix develop --command bazel test //:storage_tests_remote_gate --test_output=errors "$@" </dev/null
REMOTE
}

run_remote_gate() {
  local host=$1 sha url cmd arg
  shift
  [ -z "$(git status --porcelain)" ] ||
    die "uncommitted changes: the remote gate tests the pushed commit, not this tree"
  sha=$(git rev-parse HEAD)
  [ -n "$(git branch -r --contains "$sha")" ] ||
    die "$sha is not on any remote-tracking branch: push it first"
  url=${STORAGE_REMOTE_GATE_URL:-$(git remote get-url origin)}
  echo "run-gates: remote gate on $host at $sha"
  # ssh joins its arguments into one remote shell command line: quote them.
  cmd=$(printf 'bash -l -s -- %q %q' "$url" "$sha")
  for arg in "$@"; do cmd+=$(printf ' %q' "$arg"); done
  remote_script | ssh -o BatchMode=yes "$host" "$cmd"
}

if docker_usable; then
  echo "run-gates: Docker is usable; running all gates here"
  bazel_test //:storage_tests_all_gates "$@"
elif [ -n "${STORAGE_REMOTE_GATE_HOST:-}" ]; then
  echo "run-gates: no usable Docker here; local gates first"
  bazel_test //:storage_tests_fast_gate //:storage_tests_mounted_system_gate \
    //:storage_tests_resilience_gate "$@"
  run_remote_gate "$STORAGE_REMOTE_GATE_HOST" "$@"
else
  die "the remote gate needs Docker, and none is usable here. Start Docker, or set STORAGE_REMOTE_GATE_HOST to an ssh host that has it (the gate then runs there at your pushed commit)."
fi
