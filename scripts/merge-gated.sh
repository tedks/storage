#!/usr/bin/env bash
# Merge a PR only if this checkout is the exact, rebased, gated head.
#
#   scripts/merge-gated.sh <pr-number>
#
# Run it from the worktree of the PR's branch. It refuses, with a one-line
# reason and a distinct exit code, unless:
#
#   12  the working tree is clean (no uncommitted or untracked files)
#   11  HEAD is the PR's head commit on GitHub (pushed, and nothing newer)
#   13  scripts/run-gates.sh left a receipt for HEAD's sha and tree
#   10  the PR's base branch (origin/master for ordinary PRs) is an ancestor
#       of HEAD: the head is rebased, 0 commits behind
#
# Then it prints the council-record line for the PR comment and runs
#
#   gh pr merge <pr> --merge --match-head-commit <sha>
#
# (a normal merge, never squash or rebase). GitHub itself rejects the merge
# if the PR head moved after the check. The base branch is fetched last, right
# before the merge, to keep the window in which master can move small; a
# merge in that window is not detected (only GitHub's "require branches to be
# up to date" rule would close it; see ditz open-questions), so rerun the
# script if you notice one. A receipt is a guard against mistakes, not a
# defence against forgery: anyone can write one.
#
# Other exits: 64 usage, 65 cannot verify (fetch or gh failed), 20 gh refused
# the merge.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

refuse() {
  local code=$1
  shift
  echo "merge-gated: refusing: $*" >&2
  exit "$code"
}

[ $# -eq 1 ] || refuse 64 "usage: scripts/merge-gated.sh <pr-number>"
pr=$1
case $pr in
  '' | *[!0-9]*) refuse 64 "PR number must be digits, got '$pr'" ;;
esac

sha=$(git rev-parse HEAD)
tree=$(git rev-parse 'HEAD^{tree}')

[ -z "$(git status --porcelain)" ] ||
  refuse 12 "the tree is not clean; commit or remove changes and rerun the gates"

pr_info=$(gh pr view "$pr" --json headRefOid,baseRefName \
  --jq '.headRefOid + " " + .baseRefName') ||
  refuse 65 "could not read PR #$pr from GitHub"
pr_head=${pr_info%% *}
case $pr_head in
  '' | *[!0-9a-f]*) refuse 65 "unusable head sha from GitHub for PR #$pr" ;;
esac
base=${pr_info#* }
case $base in
  '' | -* | *[!A-Za-z0-9._/-]*) refuse 65 "unusable base branch name for PR #$pr" ;;
esac

[ "$sha" = "$pr_head" ] ||
  refuse 11 "HEAD $sha is not PR #$pr's head $pr_head; push, or check out the PR branch"

receipt=$root/.gates/$sha.json
if [ ! -f "$receipt" ] ||
  ! grep -qxF "  \"sha\": \"$sha\"," "$receipt" ||
  ! grep -qxF "  \"tree\": \"$tree\"," "$receipt"; then
  refuse 13 "no gate receipt for $sha (tree $tree); run scripts/run-gates.sh on this commit"
fi

git fetch -q origin || refuse 65 "git fetch origin failed"
base_sha=$(git rev-parse --verify -q "refs/remotes/origin/$base^{commit}") ||
  refuse 65 "origin/$base not found after fetch"
git merge-base --is-ancestor "$base_sha" "$sha" ||
  refuse 10 "origin/$base ($base_sha) is not an ancestor of HEAD: $(git rev-list --count "$sha..$base_sha") commit(s) behind; rebase, rerun the gates, push"

targets=$(sed -n 's/^  "targets": \[\(.*\)\],\{0,1\}$/\1/p' "$receipt" | tr -d '"')
echo "merge-gated: council record: rebased on $base_sha, 0 behind; gates ${targets:-?} on $sha"

gh pr merge "$pr" --merge --match-head-commit "$sha" ||
  refuse 20 "gh pr merge failed for PR #$pr"
