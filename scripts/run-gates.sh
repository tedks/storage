#!/usr/bin/env bash
# Run every quality gate (fast, mounted, resilience and remote) before merging.
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
#                             usually works: the host fetches the commit by
#                             SHA, which GitHub allows).
#
# On success, with a clean tree and no extra arguments, it writes a receipt,
# .gates/<full sha>.json (git-ignored): the sha, its tree hash, the targets
# run, a hash of this host's name (the repo is public) and a timestamp.
# scripts/merge-gated.sh refuses to merge a head with no receipt. A dirty
# tree, or extra arguments (which can narrow a gate), still run but get none.
#
# Extra arguments go to every bazel test, local and remote; a pytest flag such
# as --test_arg=-k fails any module it selects nothing in, so avoid it here.
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

# Writes .gates/<HEAD sha>.json for the targets given. Only called after a run
# that passed; refuses (returns 1, says why) unless the tree is clean, HEAD is
# still the commit the run started on ($1), and no extra arguments narrowed it.
write_receipt() {
  local started=$1 sha tree host stamp dir targets t first=1 tmp
  shift
  sha=$(git rev-parse HEAD)
  if [ -n "$(git status --porcelain)" ]; then
    echo "run-gates: gates passed on $sha plus uncommitted changes; no receipt written (commit and rerun to attest the commit)" >&2
    return 1
  fi
  [ "$sha" = "$started" ] || {
    echo "run-gates: HEAD moved during the run; no receipt written" >&2
    return 1
  }
  if [ "$extra_args" -ne 0 ]; then
    echo "run-gates: extra arguments were given, so this run may not have been the full gates; no receipt written" >&2
    return 1
  fi
  tree=$(git rev-parse 'HEAD^{tree}')
  host=$(hostname | sha256sum | cut -c1-12)
  stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  targets=
  for t in "$@"; do
    [ "$first" = 1 ] || targets+=", "
    targets+="\"$t\""
    first=0
  done
  dir=$root/.gates
  mkdir -p "$dir"
  tmp=$(mktemp "$dir/.receipt.XXXXXX")
  printf '{\n  "sha": "%s",\n  "tree": "%s",\n  "targets": [%s],\n  "host": "%s",\n  "timestamp": "%s"\n}\n' \
    "$sha" "$tree" "$targets" "$host" "$stamp" >"$tmp"
  mv -f "$tmp" "$dir/$sha.json"
  echo "run-gates: receipt $dir/$sha.json"
}

docker_usable() {
  command -v docker >/dev/null 2>&1 && timeout 30 docker info >/dev/null 2>&1
}

# Runs on the remote host, fed to `bash -l -s` so nix is on PATH. Everything
# that reads stdin gets /dev/null: stdin is this script.
remote_script() {
  cat <<'REMOTE'
set -euo pipefail
url=$1
sha=$2
shift 2
case ${TMPDIR:-} in
  /*) tmp=$TMPDIR ;;
  *) tmp=/tmp ;;
esac
scratch=$(mktemp -d "$tmp/storage-remote-gate.XXXXXX")
cleanup() {
  cd /
  # Free the bazel output base this checkout created, then the checkout.
  if [ -f "$scratch/MODULE.bazel" ]; then
    (cd "$scratch" && nix develop --command bazel clean --expunge </dev/null >/dev/null 2>&1) || true
  fi
  case $scratch in
    /*/storage-remote-gate.??????)
      find "$scratch" -mindepth 1 -delete || true
      rmdir "$scratch" || true
      ;;
  esac
}
trap cleanup EXIT
# A dropped ssh connection usually shows up as PIPE on the next write: leave
# through the EXIT trap.
trap 'exit 1' HUP INT TERM PIPE
cd "$scratch"
git init -q . </dev/null
git fetch -q --depth 1 -- "$url" "$sha" </dev/null
git checkout -q --detach FETCH_HEAD </dev/null
[ "$(git rev-parse HEAD)" = "$sha" ] || { echo "fetched commit is not $sha" >&2; exit 1; }
nix develop --command bazel test //:storage_tests_remote_gate --test_output=errors "$@" </dev/null
REMOTE
}

# Prints the commit to test. Fails unless the tree is clean and that commit is
# on origin, since the host fetches it from there.
remote_preflight() {
  local sha
  [ -z "$(git status --porcelain)" ] ||
    die "uncommitted changes: the remote gate tests the pushed commit, not this tree"
  sha=$(git rev-parse HEAD)
  if [ -n "${STORAGE_REMOTE_GATE_URL:-}" ]; then
    [ -n "$(git branch -r --contains "$sha")" ]
  else
    [ -n "$(git branch -r --contains "$sha" --list 'origin/*')" ]
  fi || die "$sha is not on a remote-tracking branch of the fetch source: push it first"
  echo "$sha"
}

run_remote_gate() {
  local host=$1 sha=$2 url cmd arg
  shift 2
  [ "$(git rev-parse HEAD)" = "$sha" ] && [ -z "$(git status --porcelain)" ] ||
    die "HEAD or the tree changed while the local gates ran; run again"
  url=${STORAGE_REMOTE_GATE_URL:-$(git remote get-url origin)}
  echo "run-gates: remote gate on $host at $sha"
  # ssh joins its arguments into one remote shell command line: quote them
  # (bash %q; the remote login shell must accept its quoting, including $'...'
  # for control characters).
  cmd=$(printf 'bash -l -s -- %q %q' "$url" "$sha")
  for arg in "$@"; do cmd+=$(printf ' %q' "$arg"); done
  remote_script | ssh -o BatchMode=yes -- "$host" "$cmd"
}

extra_args=$#
started=$(git rev-parse HEAD)

if docker_usable; then
  echo "run-gates: Docker is usable; running all gates here"
  [ -z "$(git status --porcelain)" ] ||
    echo "run-gates: warning: uncommitted changes are included in this run" >&2
  bazel_test //:storage_tests_all_gates "$@"
  write_receipt "$started" //:storage_tests_all_gates || true
elif [ -n "${STORAGE_REMOTE_GATE_HOST:-}" ]; then
  case $STORAGE_REMOTE_GATE_HOST in
    -*) die "STORAGE_REMOTE_GATE_HOST must be an ssh destination, not an option" ;;
  esac
  echo "run-gates: no usable Docker here; local gates first"
  sha=$(remote_preflight)
  bazel_test //:storage_tests_fast_gate //:storage_tests_mounted_system_gate \
    //:storage_tests_resilience_gate "$@"
  run_remote_gate "$STORAGE_REMOTE_GATE_HOST" "$sha" "$@"
  write_receipt "$started" //:storage_tests_fast_gate //:storage_tests_mounted_system_gate \
    //:storage_tests_resilience_gate //:storage_tests_remote_gate || true
else
  die "the remote gate needs Docker, and none is usable here. Start Docker, or set STORAGE_REMOTE_GATE_HOST to an ssh host that has it (the gate then runs there at your pushed commit)."
fi
