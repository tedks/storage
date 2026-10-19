"""
Real-remote system tests: the SSH backend, rsync over SSH and the remote
watcher against sshd running in Docker containers (issue #105).

The fixtures build the image in tests/remote/ (tagged by a hash of that
directory, so an edited Dockerfile is never tested stale), generate a
test-only ed25519 client key and one host key per member under the test's
tmp dir, and start two members on a user-defined Docker network. Everything
they create in Docker is named ``storage-test-<run id>-*`` and removed by
exact name at teardown.

RsyncBackend and RemoteWatcher run the system ``ssh`` with
``StrictHostKeyChecking=accept-new`` and no way to pass a config or
known-hosts file, so left alone they would write these throwaway host keys
into ~/.ssh/known_hosts and fail on the next run that reuses an IP. The
session fixture puts an ``ssh`` shim first on PATH that runs the real ssh
with ``-F`` pointing at a per-run config whose known_hosts already holds the
members' host keys, and logs every invocation; tests read that log to prove
which commands went over ssh.

Docker is a hard requirement of this gate: without it every test fails with
a message naming Docker. It never skips.
"""

import hashlib
import os
import secrets
import shlex
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pytest

from storage.backends.rsync import RsyncBackend
from storage.backends.ssh import SSHBackend
from storage.checksums import fast_checksum_bytes
from storage.watcher import RemoteWatcher

IMAGE_DIR = Path(__file__).resolve().parent / "remote"
IMAGE_REPO = "storage-test-sshd"
REMOTE_ROOT = "/srv/storage"
REMOTE_USER = "storage"
MEMBER_COUNT = 2
# Set to 1 to connect through ports published on 127.0.0.1 even when the
# container IPs are routable (the fallback for hosts that cannot route to
# the bridge network).
FORCE_PUBLISHED_ENV = "STORAGE_TEST_REMOTE_PUBLISHED"


def _docker(*args, check=True, timeout=120.0, input=None):
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
        input=input,
    )


def _require_docker():
    if shutil.which("docker") is None:
        pytest.fail(
            "Docker is unavailable: no docker CLI on PATH. "
            "//:storage_tests_remote_gate needs Docker; see AGENTS.md.",
            pytrace=False,
        )
    try:
        info = _docker("info", "--format", "{{.ServerVersion}}", check=False, timeout=30.0)
    except subprocess.TimeoutExpired:
        pytest.fail("Docker is unavailable: `docker info` timed out.", pytrace=False)
    if info.returncode != 0:
        pytest.fail(
            "Docker is unavailable: `docker info` failed: "
            f"{(info.stderr or info.stdout).strip()}",
            pytrace=False,
        )


def _image_tag() -> str:
    digest = hashlib.sha256()
    for path in sorted(IMAGE_DIR.iterdir()):
        digest.update(path.name.encode() + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return f"{IMAGE_REPO}:{digest.hexdigest()[:16]}"


def _ensure_image(build_root: Path) -> str:
    tag = _image_tag()
    if _docker("image", "inspect", tag, check=False).returncode == 0:
        return tag
    # Runfiles are symlinks, which a build context sends as symlinks;
    # copy the real files into a context of our own.
    context = build_root / "image-context"
    context.mkdir()
    for path in IMAGE_DIR.iterdir():
        shutil.copyfile(path, context / path.name)
    result = _docker("build", "--label", "storage-test=1", "-t", tag, str(context),
                     check=False, timeout=900.0)
    if result.returncode != 0:
        pytest.fail(f"Building {tag} failed:\n{result.stderr[-4000:]}", pytrace=False)
    # Drop tags this fixture built from older contents of tests/remote/.
    # Without -f, Docker refuses to remove one a running container uses.
    stale = _docker("image", "ls", "--filter", "label=storage-test=1",
                    "--format", "{{.Repository}}:{{.Tag}}", IMAGE_REPO).stdout.split()
    for old_tag in stale:
        if old_tag != tag:
            _docker("rmi", old_tag, check=False)
    return tag


def _ssh_keygen(path: Path, comment: str):
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", comment, "-f", str(path)],
        check=True, capture_output=True, timeout=30.0,
    )


def _ssh_banner(host: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            return sock.recv(64).startswith(b"SSH-2.0-")
    except OSError:
        return False


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@dataclass
class Member:
    name: str
    container: str
    ip: str
    host: str
    port: int
    host_pubkey: str


class RemoteCluster:
    """The running members and the client-side files the tests connect with."""

    def __init__(self, run_id: str, members: List[Member], key_path: Path, ssh_log: Path):
        self.run_id = run_id
        self.members = members
        self.key_path = key_path
        self.ssh_log = ssh_log

    def exec(self, member: Member, command: str, user: str = REMOTE_USER,
             check: bool = True, input=None) -> subprocess.CompletedProcess:
        """Run a shell command inside the member's container."""
        return _docker("exec", "-i", "-u", user, member.container, "sh", "-c", command,
                       check=check, timeout=60.0, input=input)

    def read_remote(self, member: Member, rel_path: str) -> bytes:
        result = subprocess.run(
            ["docker", "exec", "-u", REMOTE_USER, member.container,
             "cat", f"{REMOTE_ROOT}/{rel_path.lstrip('/')}"],
            capture_output=True, check=True, timeout=60.0,
        )
        return result.stdout

    def write_remote(self, member: Member, rel_path: str, data: bytes):
        path = f"{REMOTE_ROOT}/{rel_path.lstrip('/')}"
        subprocess.run(
            ["docker", "exec", "-i", "-u", REMOTE_USER, member.container, "sh", "-c",
             f"mkdir -p \"$(dirname {shlex.quote(path)})\" && cat > {shlex.quote(path)}"],
            input=data, capture_output=True, check=True, timeout=60.0,
        )

    def ssh_log_lines(self) -> List[str]:
        if not self.ssh_log.exists():
            return []
        return self.ssh_log.read_text().splitlines()

    def backend_kwargs(self, member: Member) -> dict:
        return dict(
            name=member.name,
            host=member.host,
            remote_path=REMOTE_ROOT,
            username=REMOTE_USER,
            key_filename=str(self.key_path),
            port=member.port,
        )

    def wipe(self):
        """Empty every exported directory and end any inotifywait left over.

        A stopped watcher's remote inotifywait outlives it (#106); without
        this, the next test's check that one is running would pass early.
        """
        for member in self.members:
            self.exec(member, "pkill -u storage -x inotifywait", check=False)
            self.exec(member, f"find {REMOTE_ROOT} -mindepth 1 -delete")


def _install_ssh_shim(work: Path, known_hosts: Path) -> tuple:
    real_ssh = shutil.which("ssh")
    if real_ssh is None:
        pytest.fail("No ssh client on PATH; the remote gate needs one.", pytrace=False)
    config = work / "ssh_config"
    config.write_text(
        f"UserKnownHostsFile {known_hosts}\n"
        "GlobalKnownHostsFile /dev/null\n"
        "IdentitiesOnly yes\n"
        "IdentityAgent none\n"
        "LogLevel ERROR\n"
    )
    log = work / "ssh_invocations.log"
    shim_dir = work / "bin"
    shim_dir.mkdir()
    shim = shim_dir / "ssh"
    shim.write_text(
        "#!/bin/sh\n"
        "# Test-only: see tests/test_remote_system.py.\n"
        f"printf '%s\\n' \"$*\" >> {shlex.quote(str(log))}\n"
        f"exec {shlex.quote(real_ssh)} -F {shlex.quote(str(config))} \"$@\"\n"
    )
    shim.chmod(0o755)
    return shim_dir, log


@pytest.fixture(scope="session")
def cluster(tmp_path_factory):
    _require_docker()
    work = tmp_path_factory.mktemp("remote-gate")
    run_id = f"storage-test-{os.getpid()}-{secrets.token_hex(3)}"
    created_containers: List[str] = []
    created_network = None
    old_path = os.environ.get("PATH", "")
    try:
        image = _ensure_image(work)

        key_path = work / "client_ed25519"
        _ssh_keygen(key_path, f"{run_id}-client")
        client_pub = (work / "client_ed25519.pub").read_text()

        _docker("network", "create", "--label", "storage-test=1", run_id)
        created_network = run_id

        force_published = os.environ.get(FORCE_PUBLISHED_ENV) == "1"
        members: List[Member] = []
        for index in range(MEMBER_COUNT):
            name = f"remote{index}"
            keys = work / f"keys-{name}"
            keys.mkdir()
            _ssh_keygen(keys / "ssh_host_ed25519_key", f"{run_id}-{name}")
            (keys / "authorized_keys").write_text(client_pub)
            container = f"{run_id}-{name}"
            created_containers.append(container)
            _docker("run", "-d", "--name", container, "--network", run_id,
                    "--label", "storage-test=1", "-p", "127.0.0.1::22",
                    "-v", f"{keys}:/keys:ro", image)
            ip = _docker("inspect", "-f",
                         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                         container).stdout.strip()
            published = _docker("port", container, "22/tcp").stdout.split()[0]
            published_port = int(published.rsplit(":", 1)[1])
            if not _wait_for(lambda: _ssh_banner("127.0.0.1", published_port, 1.0),
                             timeout=30.0, interval=0.2):
                logs = _docker("logs", container, check=False)
                pytest.fail(f"sshd in {container} never answered:\n"
                            f"{logs.stdout}{logs.stderr}", pytrace=False)
            if not force_published and _ssh_banner(ip, 22, 2.0):
                host, port = ip, 22
            else:
                host, port = "127.0.0.1", published_port
            host_pubkey = (keys / "ssh_host_ed25519_key.pub").read_text().split()
            members.append(Member(name, container, ip, host, port,
                                  " ".join(host_pubkey[:2])))

        known_hosts = work / "known_hosts"
        known_hosts.write_text("".join(
            f"{m.host if m.port == 22 else f'[{m.host}]:{m.port}'} {m.host_pubkey}\n"
            for m in members
        ))
        shim_dir, ssh_log = _install_ssh_shim(work, known_hosts)
        os.environ["PATH"] = f"{shim_dir}{os.pathsep}{old_path}"

        yield RemoteCluster(run_id, members, key_path, ssh_log)
    finally:
        os.environ["PATH"] = old_path
        for container in created_containers:
            _docker("rm", "-f", "-v", container, check=False)
        if created_network is not None:
            _docker("network", "rm", created_network, check=False)
        shutil.rmtree(work, ignore_errors=True)


@pytest.fixture
def remote(cluster):
    """The cluster, with every member's exported directory emptied first."""
    cluster.wipe()
    return cluster


@pytest.fixture
def ssh_backend(remote):
    backend = SSHBackend(**remote.backend_kwargs(remote.members[0]))
    yield backend
    backend.close()


def test_members_are_distinct_containers(remote):
    """The fixture is live: each member's sshd is its own container."""
    hostnames = {remote.exec(m, "hostname").stdout.strip() for m in remote.members}
    assert len(hostnames) == MEMBER_COUNT
    for member in remote.members:
        assert _ssh_banner(member.host, member.port, 2.0)


def test_ssh_backend_operations(remote, ssh_backend, tmp_path):
    member = remote.members[0]
    payload = secrets.token_bytes(200_000)
    source = tmp_path / "upload.bin"
    source.write_bytes(payload)

    ssh_backend.mkdir("/dir")
    ssh_backend.copy_from_local(source, "/dir/upload.bin")

    # The bytes landed inside the container, read back through docker exec.
    assert remote.read_remote(member, "dir/upload.bin") == payload
    assert remote.exec(member, f"stat -c %U {REMOTE_ROOT}/dir/upload.bin").stdout.strip() \
        == REMOTE_USER

    assert ssh_backend.exists("/dir/upload.bin")
    assert not ssh_backend.exists("/dir/absent.bin")
    assert ssh_backend.isdir("/dir")
    assert ssh_backend.listdir("/dir") == ["upload.bin"]
    assert ssh_backend.stat("/dir/upload.bin").st_size == len(payload)
    assert ssh_backend.get_size("/dir/upload.bin") == len(payload)
    assert ssh_backend.read_file("/dir/upload.bin", 1000, 64) == payload[1000:1064]

    # A file made inside the container is visible and downloads intact.
    made_remotely = secrets.token_bytes(50_000)
    remote.write_remote(member, "dir/made-remotely.bin", made_remotely)
    assert sorted(ssh_backend.listdir("/dir")) == ["made-remotely.bin", "upload.bin"]
    download = tmp_path / "download.bin"
    ssh_backend.copy_to_local("/dir/made-remotely.bin", download)
    assert download.read_bytes() == made_remotely


def _count_downloads(backend, monkeypatch) -> list:
    calls = []
    original = backend.copy_to_local

    def counting(virtual_path, local_path):
        calls.append(virtual_path)
        return original(virtual_path, local_path)

    monkeypatch.setattr(backend, "copy_to_local", counting)
    return calls


def test_fast_checksum_uses_remote_xxh128sum(remote, ssh_backend, monkeypatch):
    member = remote.members[0]
    payload = secrets.token_bytes(300_000)
    remote.write_remote(member, "hashed.bin", payload)
    remote_digest = remote.exec(member, f"xxh128sum {REMOTE_ROOT}/hashed.bin").stdout.split()[0]
    assert remote_digest == fast_checksum_bytes(payload)

    downloads = _count_downloads(ssh_backend, monkeypatch)
    assert ssh_backend.get_fast_checksum("/hashed.bin") == remote_digest
    assert downloads == [], "the fast path must hash on the remote, not download"


def test_fast_checksum_falls_back_to_download(remote, ssh_backend, monkeypatch):
    member = remote.members[0]
    payload = secrets.token_bytes(300_000)
    remote.write_remote(member, "hashed.bin", payload)
    xxh = remote.exec(member, "command -v xxh128sum").stdout.strip()
    assert xxh, "the image must ship xxh128sum for the fast-path test to mean anything"

    remote.exec(member, f"mv {xxh} {xxh}.hidden", user="root")
    try:
        assert remote.exec(member, "command -v xxh128sum", check=False).returncode != 0
        downloads = _count_downloads(ssh_backend, monkeypatch)
        assert ssh_backend.get_fast_checksum("/hashed.bin") == fast_checksum_bytes(payload)
        assert downloads == ["/hashed.bin"]
    finally:
        remote.exec(member, f"mv {xxh}.hidden {xxh}", user="root")


def test_rsync_write_over_ssh(remote, tmp_path):
    member = remote.members[1]
    backend = RsyncBackend(**remote.backend_kwargs(member))
    try:
        payload = secrets.token_bytes(400_000)
        source = tmp_path / "rsynced.bin"
        source.write_bytes(payload)
        log_before = len(remote.ssh_log_lines())

        backend.copy_from_local(source, "/sub/rsynced.bin")

        assert remote.read_remote(member, "sub/rsynced.bin") == payload
        rsync_calls = [line for line in remote.ssh_log_lines()[log_before:]
                       if "rsync --server" in line]
        assert rsync_calls, "the write must go through rsync over the ssh shim"
        assert all(member.host in line for line in rsync_calls)

        download = tmp_path / "back.bin"
        backend.copy_to_local("/sub/rsynced.bin", download)
        assert download.read_bytes() == payload
    finally:
        backend.close()


def _watcher_for(remote, member, on_change) -> RemoteWatcher:
    return RemoteWatcher(
        backend_name=member.name,
        ssh_host=member.host,
        ssh_port=member.port,
        remote_path=REMOTE_ROOT,
        on_change=on_change,
        username=REMOTE_USER,
        key_filename=str(remote.key_path),
    )


def _remote_inotifywait_running(remote, member) -> bool:
    return remote.exec(member, "pgrep -u storage -x inotifywait",
                       check=False).returncode == 0


def _wait_for_remote_inotifywait(remote, member):
    assert _wait_for(lambda: _remote_inotifywait_running(remote, member),
                     timeout=20.0, interval=0.2), \
        "inotifywait never started in the container"
    # inotifywait -r sets its watches after it starts; give it a moment.
    time.sleep(1.0)


def test_watcher_reports_change_made_in_container(remote):
    member = remote.members[0]
    remote.exec(member, f"mkdir -p {REMOTE_ROOT}/watched")
    token = secrets.token_hex(4)
    events: List[str] = []
    seen = threading.Event()

    def on_change(virtual_path):
        events.append(virtual_path)
        if virtual_path == f"/watched/new-{token}":
            seen.set()

    log_before = len(remote.ssh_log_lines())
    watcher = _watcher_for(remote, member, on_change)
    watcher.start()
    try:
        _wait_for_remote_inotifywait(remote, member)
        assert not seen.is_set()

        remote.exec(member, f"echo changed > {REMOTE_ROOT}/watched/new-{token}")

        assert seen.wait(15.0), f"no watcher event for the docker exec change; saw {events}"
        assert "/watched" in events
        assert any("inotifywait" in line and member.host in line
                   for line in remote.ssh_log_lines()[log_before:])
    finally:
        watcher.stop()


# Known failure, #106: stop() kills only the local ssh; without a pty the
# remote inotifywait is reparented to init and lives until its next event.
# strict, so the fix turns this red and the marker comes off.
@pytest.mark.xfail(strict=True, reason="#106: remote inotifywait outlives RemoteWatcher.stop()")
def test_watcher_stop_ends_remote_inotifywait(remote):
    member = remote.members[1]
    watcher = _watcher_for(remote, member, lambda _path: None)
    watcher.start()
    try:
        _wait_for_remote_inotifywait(remote, member)
    finally:
        watcher.stop()
    assert _wait_for(lambda: not _remote_inotifywait_running(remote, member),
                     timeout=10.0, interval=0.2), \
        "inotifywait still runs in the container after RemoteWatcher.stop()"
