"""Branch selection of scripts/run-gates.sh, against fake docker, nix and ssh.

The fakes log their argv to $FAKE_LOG; nothing here touches a real Docker
daemon, nix, or the network.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "run-gates.sh"

FAKE_LOGGER = """#!/bin/sh
printf '%s' "$(basename "$0")" >> "$FAKE_LOG"
for a in "$@"; do printf ' [%s]' "$a" >> "$FAKE_LOG"; done
printf '\\n' >> "$FAKE_LOG"
"""


def _git(cwd, *args):
    subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
        },
    )


@pytest.fixture
def env(tmp_path):
    """A repo with the script, a pushed commit, and fakes first on PATH."""
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", str(origin))
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy(SCRIPT, repo / "scripts" / "run-gates.sh")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    _git(repo, "remote", "add", "origin", str(origin))
    _git(repo, "push", "-q", "-u", "origin", "main")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "fake.log"
    log.touch()
    for name in ("nix", "ssh"):
        (bin_dir / name).write_text(FAKE_LOGGER + "cat >/dev/null 2>&1 || true\n")
        (bin_dir / name).chmod(0o755)
    environ = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_LOG": str(log),
    }
    environ.pop("STORAGE_REMOTE_GATE_HOST", None)
    environ.pop("STORAGE_REMOTE_GATE_URL", None)
    return repo, bin_dir, log, environ


def _fake_docker(bin_dir, exit_code):
    docker = bin_dir / "docker"
    docker.write_text(FAKE_LOGGER + f"exit {exit_code}\n")
    docker.chmod(0o755)


def _run(repo, environ, *args):
    return subprocess.run(
        [str(repo / "scripts" / "run-gates.sh"), *args],
        cwd=repo,
        env=environ,
        capture_output=True,
        text=True,
    )


def test_docker_usable_runs_all_gates_locally(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 0)
    result = _run(repo, environ, "--config=x")
    assert result.returncode == 0, result.stderr
    lines = log.read_text().splitlines()
    assert lines[-1] == (
        "nix [develop] [--command] [bazel] [test] [//:storage_tests_all_gates]"
        " [--config=x] [--test_output=errors]"
    )
    assert not any(line.startswith("ssh") for line in lines)


def test_docker_ignored_host_when_usable(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 0)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    assert _run(repo, environ).returncode == 0
    assert "ssh" not in log.read_text()


def test_no_docker_and_no_host_fails_naming_both(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "Docker" in result.stderr
    assert "STORAGE_REMOTE_GATE_HOST" in result.stderr
    assert "nix" not in log.read_text()


def test_docker_missing_entirely_is_unusable(env):
    repo, bin_dir, log, environ = env
    # Only the fakes plus a minimal PATH that has git and sh but no docker.
    for tool in ("git", "dirname", "cat", "bash", "env"):
        found = shutil.which(tool)
        if found and not (bin_dir / tool).exists():
            (bin_dir / tool).symlink_to(found)
    environ["PATH"] = str(bin_dir)
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "STORAGE_REMOTE_GATE_HOST" in result.stderr


def test_no_docker_with_host_runs_local_gates_then_remote_over_ssh(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    result = _run(repo, environ)
    assert result.returncode == 0, result.stderr
    lines = log.read_text().splitlines()
    nix_line = next(line for line in lines if line.startswith("nix"))
    assert "storage_tests_fast_gate" in nix_line
    assert "storage_tests_mounted_system_gate" in nix_line
    assert "storage_tests_resilience_gate" in nix_line
    assert "all_gates" not in nix_line
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()
    ssh_line = next(line for line in lines if line.startswith("ssh"))
    assert "[gatehost]" in ssh_line
    assert sha in ssh_line
    assert lines.index(nix_line) < lines.index(ssh_line)


def test_remote_gate_refuses_unpushed_commit(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    (repo / "new.txt").write_text("x")
    _git(repo, "add", "new.txt")
    _git(repo, "commit", "-q", "-m", "unpushed")
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "push it first" in result.stderr
    assert "ssh" not in log.read_text()


def test_remote_gate_refuses_dirty_tree(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    (repo / "untracked.txt").write_text("x")
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "uncommitted" in result.stderr
    assert "ssh" not in log.read_text()
