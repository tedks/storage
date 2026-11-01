"""scripts/merge-gated.sh and the receipt scripts/run-gates.sh writes.

A local bare origin and a fake gh (and fake nix, docker) on PATH; nothing here
touches GitHub, Docker or nix. The fake gh logs its argv to $FAKE_LOG and
answers `pr view` from $FAKE_PR_HEAD / $FAKE_PR_BASE.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).parent.parent / "scripts"

GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.invalid",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
}

FAKE_GH = """#!/bin/sh
printf 'gh' >> "$FAKE_LOG"
for a in "$@"; do printf ' [%s]' "$a" >> "$FAKE_LOG"; done
printf '\\n' >> "$FAKE_LOG"
case "$1 $2" in
  "pr view") printf '%s %s\\n' "$FAKE_PR_HEAD" "${FAKE_PR_BASE:-main}" ;;
  "pr merge") exit "${FAKE_GH_MERGE_EXIT:-0}" ;;
  *) exit 2 ;;
esac
"""

REFUSAL_CODES = {"dirty": 12, "head": 11, "receipt": 13, "behind": 10}


def _git(cwd, *args):
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, **GIT_ENV},
    ).stdout.strip()


def _commit(repo, name):
    (repo / name).write_text(name)
    _git(repo, "add", name)
    _git(repo, "commit", "-q", "-m", name)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def env(tmp_path):
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    for name in ("run-gates.sh", "merge-gated.sh"):
        shutil.copy(SCRIPTS / name, repo / "scripts" / name)
    (repo / "MODULE.bazel").write_text("")
    (repo / ".gitignore").write_text("/.gates\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    _git(repo, "remote", "add", "origin", str(origin))
    _git(repo, "push", "-q", "-u", "origin", "main")
    _git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature.txt")
    _git(repo, "push", "-q", "-u", "origin", "feature")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "fake.log"
    log.touch()
    (bin_dir / "gh").write_text(FAKE_GH)
    (bin_dir / "docker").write_text("#!/bin/sh\nexit 0\n")
    (bin_dir / "nix").write_text('#!/bin/sh\ncase "$*" in *"${FAKE_NIX_FAIL:-@@none@@}"*) exit 1;; esac\n')
    for name in ("gh", "docker", "nix"):
        (bin_dir / name).chmod(0o755)
    environ = {
        **os.environ,
        **GIT_ENV,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_LOG": str(log),
        "FAKE_PR_HEAD": head,
    }
    for var in ("STORAGE_REMOTE_GATE_HOST", "STORAGE_REMOTE_GATE_URL"):
        environ.pop(var, None)
    return repo, log, environ


def _gates(repo, environ, *args):
    return subprocess.run(
        [str(repo / "scripts" / "run-gates.sh"), *args],
        cwd=repo, env=environ, capture_output=True, text=True,
        stdin=subprocess.DEVNULL,
    )


def _merge(repo, environ, pr="7"):
    return subprocess.run(
        [str(repo / "scripts" / "merge-gated.sh"), *([pr] if pr else [])],
        cwd=repo, env=environ, capture_output=True, text=True,
        stdin=subprocess.DEVNULL,
    )


def _receipt(repo):
    return repo / ".gates" / f"{_git(repo, 'rev-parse', 'HEAD')}.json"


# --- receipt writer ---------------------------------------------------------


def test_success_writes_receipt(env):
    repo, _, environ = env
    assert _gates(repo, environ).returncode == 0
    receipt = json.loads(_receipt(repo).read_text())
    assert receipt["sha"] == _git(repo, "rev-parse", "HEAD")
    assert receipt["tree"] == _git(repo, "rev-parse", "HEAD^{tree}")
    assert receipt["targets"] == ["//:storage_tests_all_gates"]
    assert re.fullmatch(r"[0-9a-f]{12}", receipt["host"])
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", receipt["timestamp"])
    assert subprocess.run(
        ["hostname"], capture_output=True, text=True
    ).stdout.strip() not in _receipt(repo).read_text()
    # the receipt itself must not dirty the tree
    assert _git(repo, "status", "--porcelain") == ""


def test_dirty_tree_runs_but_writes_no_receipt(env):
    repo, _, environ = env
    (repo / "untracked.txt").write_text("x")
    result = _gates(repo, environ)
    assert result.returncode == 0
    assert "no receipt" in result.stderr
    assert not (repo / ".gates").exists() or not list((repo / ".gates").iterdir())


def test_failed_gates_write_no_receipt(env):
    repo, _, environ = env
    environ["FAKE_NIX_FAIL"] = "all_gates"
    assert _gates(repo, environ).returncode != 0
    assert not _receipt(repo).exists()


def test_extra_args_write_no_receipt(env):
    repo, _, environ = env
    result = _gates(repo, environ, "--config=x")
    assert result.returncode == 0
    assert "extra arguments" in result.stderr
    assert not _receipt(repo).exists()


def test_remote_host_path_records_all_four_gates(env, tmp_path):
    repo, _, environ = env
    (tmp_path / "bin" / "docker").write_text("#!/bin/sh\nexit 1\n")
    (tmp_path / "bin" / "ssh").write_text("#!/bin/sh\ncat >/dev/null\n")
    (tmp_path / "bin" / "ssh").chmod(0o755)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    _git(repo, "push", "-q", "origin", "feature")
    assert _gates(repo, environ).returncode == 0
    targets = json.loads(_receipt(repo).read_text())["targets"]
    assert targets[-1] == "//:storage_tests_remote_gate"
    assert len(targets) == 4


# --- merge-gated.sh ---------------------------------------------------------


def test_happy_path_merges_with_match_head_commit(env):
    repo, log, environ = env
    assert _gates(repo, environ).returncode == 0
    sha = _git(repo, "rev-parse", "HEAD")
    main = _git(repo, "rev-parse", "origin/main")
    result = _merge(repo, environ)
    assert result.returncode == 0, result.stderr
    assert (
        f"rebased on {main}, 0 behind; gates //:storage_tests_all_gates on {sha}"
        in result.stdout
    )
    assert f"gh [pr] [merge] [7] [--merge] [--match-head-commit] [{sha}]" in log.read_text()


@pytest.mark.parametrize("pr", ["", "x", "7;ls", "-1"])
def test_bad_usage(env, pr):
    repo, log, environ = env
    assert _merge(repo, environ, pr).returncode == 64
    assert "merge" not in log.read_text()


def test_refuses_dirty_tree(env):
    repo, log, environ = env
    _gates(repo, environ)
    (repo / "scratch.txt").write_text("x")
    result = _merge(repo, environ)
    assert result.returncode == REFUSAL_CODES["dirty"]
    assert "not clean" in result.stderr
    assert "pr] [merge" not in log.read_text()


def test_refuses_head_that_is_not_the_pr_head(env):
    repo, log, environ = env
    _gates(repo, environ)
    environ["FAKE_PR_HEAD"] = "0" * 40
    result = _merge(repo, environ)
    assert result.returncode == REFUSAL_CODES["head"]
    assert "pr] [merge" not in log.read_text()


def test_refuses_without_receipt(env):
    repo, log, environ = env
    result = _merge(repo, environ)
    assert result.returncode == REFUSAL_CODES["receipt"]
    assert "pr] [merge" not in log.read_text()


def test_refuses_receipt_for_a_different_tree(env):
    repo, log, environ = env
    _gates(repo, environ)
    path = _receipt(repo)
    path.write_text(path.read_text().replace('"tree": "', '"tree": "0'))
    assert _merge(repo, environ).returncode == REFUSAL_CODES["receipt"]


def test_refuses_receipt_of_an_earlier_commit(env):
    repo, log, environ = env
    _gates(repo, environ)
    environ["FAKE_PR_HEAD"] = _commit(repo, "more.txt")
    assert _merge(repo, environ).returncode == REFUSAL_CODES["receipt"]


def test_refuses_head_behind_master_even_with_receipt(env):
    repo, log, environ = env
    _gates(repo, environ)
    # master moves on origin after the gates ran
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "other.txt")
    _git(repo, "push", "-q", "origin", "main")
    _git(repo, "checkout", "-q", "feature")
    result = _merge(repo, environ)
    assert result.returncode == REFUSAL_CODES["behind"]
    assert "1 commit(s) behind" in result.stderr
    assert "pr] [merge" not in log.read_text()


def test_unreadable_pr_cannot_be_verified(env):
    repo, log, environ = env
    _gates(repo, environ)
    (Path(environ["PATH"].split(":")[0]) / "gh").write_text("#!/bin/sh\nexit 1\n")
    assert _merge(repo, environ).returncode == 65


def test_gh_merge_failure_is_distinct(env):
    repo, log, environ = env
    _gates(repo, environ)
    environ["FAKE_GH_MERGE_EXIT"] = "1"
    assert _merge(repo, environ).returncode == 20


def test_run_that_started_dirty_gets_no_receipt_even_if_clean_at_the_end(env, tmp_path):
    repo, _, environ = env
    (repo / "feature.txt").write_text("edited")
    # the gates "test" the edit, which is gone before the run ends
    (tmp_path / "bin" / "nix").write_text(
        f'#!/bin/sh\ngit -C "{repo}" checkout -- feature.txt\n'
    )
    result = _gates(repo, environ)
    assert result.returncode == 0
    assert "dirty when the run started" in result.stderr
    assert not _receipt(repo).exists()


def test_head_moving_during_the_run_gets_no_receipt(env, tmp_path):
    repo, _, environ = env
    (tmp_path / "bin" / "nix").write_text(
        f'#!/bin/sh\ngit -C "{repo}" commit -q --allow-empty -m moved\n'
    )
    result = _gates(repo, environ)
    assert result.returncode == 0
    assert "HEAD moved" in result.stderr
    assert not _receipt(repo).exists()


def test_unwritable_receipt_dir_says_no_receipt_not_success(env):
    repo, _, environ = env
    (repo / ".gates").write_text("a file, so mkdir -p fails")
    result = _gates(repo, environ)
    assert "dirty when the run started" not in result.stderr
    assert "run-gates: receipt" not in result.stdout


@pytest.mark.parametrize("base", ["-x", "a b"])
def test_unusable_base_branch_name_cannot_be_verified(env, base):
    repo, log, environ = env
    _gates(repo, environ)
    environ["FAKE_PR_BASE"] = base
    assert _merge(repo, environ).returncode == 65
    assert "pr] [merge" not in log.read_text()


def test_missing_base_branch_cannot_be_verified(env):
    repo, log, environ = env
    _gates(repo, environ)
    environ["FAKE_PR_BASE"] = "nonexistent"
    assert _merge(repo, environ).returncode == 65


def test_receipt_whose_tree_line_is_not_exact_is_refused(env):
    repo, log, environ = env
    _gates(repo, environ)
    path = _receipt(repo)
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    # the right text, on a line that is not exactly the tree line
    path.write_text(path.read_text().replace('  "tree"', '  "x-tree"'))
    assert f'"x-tree": "{tree}",' in path.read_text()
    assert _merge(repo, environ).returncode == REFUSAL_CODES["receipt"]
