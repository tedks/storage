"""Branch selection of scripts/run-gates.sh, against fake docker, nix and ssh.

The fakes log their argv to $FAKE_LOG; nothing here touches a real Docker
daemon, nix, or the network.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / "scripts" / "run-gates.sh"

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
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
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
    (repo / "MODULE.bazel").write_text("")
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
    # nix fails when its argv contains $FAKE_NIX_FAIL, and records the commit
    # checked out where it ran.
    (bin_dir / "nix").write_text(
        FAKE_LOGGER
        + 'printf "  at %s\\n" "$(git rev-parse HEAD 2>/dev/null)" >> "$FAKE_LOG"\n'
        + 'case "$*" in *"${FAKE_NIX_FAIL:-@@none@@}"*) exit 1;; esac\n'
    )
    (bin_dir / "nix").chmod(0o755)
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
        stdin=subprocess.DEVNULL,
    )


def _ssh_runs_remote_script(bin_dir):
    """An ssh that runs the command it is given locally, as the remote would."""
    ssh = bin_dir / "ssh"
    # `bash -l` would reset PATH from the login profile and lose the fakes.
    ssh.write_text(
        FAKE_LOGGER.replace("#!/bin/sh", "#!/bin/bash")
        + 'shift 3; shift; cmd=$1; exec bash -c "${cmd/bash -l/bash}"\n'
    )
    ssh.chmod(0o755)


def test_docker_usable_runs_all_gates_locally(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 0)
    result = _run(repo, environ, "--config=x")
    assert result.returncode == 0, result.stderr
    lines = log.read_text().splitlines()
    assert [line for line in lines if line.startswith("nix")][-1] == (
        "nix [develop] [--command] [bazel] [test] [//:storage_tests_all_gates]"
        " [--config=x] [--test_output=errors]"
    )
    assert not any(line.startswith("ssh") for line in lines)


def test_dirty_tree_warns_but_runs_when_docker_is_usable(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 0)
    (repo / "untracked.txt").write_text("x")
    result = _run(repo, environ)
    assert result.returncode == 0
    assert "uncommitted changes" in result.stderr


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


def test_option_shaped_host_is_rejected(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    environ["STORAGE_REMOTE_GATE_HOST"] = "-oProxyCommand=touch pwned"
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "not an option" in result.stderr
    assert log.read_text() == "docker [info]\n"


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
    assert "nix" not in log.read_text()


def test_remote_gate_refuses_dirty_tree(env):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    (repo / "untracked.txt").write_text("x")
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "uncommitted" in result.stderr
    assert "ssh" not in log.read_text()
    assert "nix" not in log.read_text()


def _remote_env(env, tmp_path):
    repo, bin_dir, log, environ = env
    _fake_docker(bin_dir, 1)
    _ssh_runs_remote_script(bin_dir)
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    environ["STORAGE_REMOTE_GATE_HOST"] = "gatehost"
    environ["STORAGE_REMOTE_GATE_URL"] = f"file://{tmp_path / 'origin.git'}"
    environ["TMPDIR"] = str(scratch_parent)
    return repo, log, environ, scratch_parent


def test_remote_script_checks_out_the_sha_and_cleans_up(env, tmp_path):
    repo, log, environ, scratch_parent = _remote_env(env, tmp_path)
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()
    result = _run(repo, environ, "--test_arg=a b", "--x='y'")
    assert result.returncode == 0, result.stderr
    lines = log.read_text().splitlines()
    remote = [
        i
        for i, line in enumerate(lines)
        if "//:storage_tests_remote_gate" in line
    ]
    assert len(remote) == 1
    assert "[--test_arg=a b] [--x='y']" in lines[remote[0]]
    assert lines[remote[0] + 1] == f"  at {sha}"
    assert any("[clean] [--expunge]" in line for line in lines)
    assert list(scratch_parent.iterdir()) == []


def test_remote_gate_failure_fails_the_script_and_cleans_up(env, tmp_path):
    repo, log, environ, scratch_parent = _remote_env(env, tmp_path)
    environ["FAKE_NIX_FAIL"] = "storage_tests_remote_gate"
    result = _run(repo, environ)
    assert result.returncode != 0
    assert list(scratch_parent.iterdir()) == []


def test_failing_local_gates_never_reach_ssh(env, tmp_path):
    repo, log, environ, scratch_parent = _remote_env(env, tmp_path)
    environ["FAKE_NIX_FAIL"] = "storage_tests_fast_gate"
    result = _run(repo, environ)
    assert result.returncode != 0
    assert "ssh" not in log.read_text()


def test_relative_tmpdir_still_cleans_up(env, tmp_path):
    repo, log, environ, scratch_parent = _remote_env(env, tmp_path)
    environ["TMPDIR"] = "."
    before = set(Path("/tmp").glob("storage-remote-gate.*"))
    result = _run(repo, environ)
    assert result.returncode == 0, result.stderr
    assert set(Path("/tmp").glob("storage-remote-gate.*")) == before
