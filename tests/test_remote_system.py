"""
Real-remote system tests: the SSH backend, rsync over SSH and the remote
watcher against sshd running in Docker containers, then a real FUSE mount of
StorageFS with both containers as SSH members (issue #105).

The fixtures build the image in tests/remote/ (tagged by a hash of that
directory, so an edited Dockerfile is never tested stale), generate a
test-only ed25519 client key and one host key per member under the test's
tmp dir, and start two members on a user-defined Docker network. Everything
they create in Docker is named ``storage-test-<run id>-*`` and removed at
teardown: containers by the id ``docker create`` returned, the network by
its exact name.

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
import warnings
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
    result = subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        input=input,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"docker {' '.join(args)} exited {result.returncode}: {result.stderr.strip()}"
        )
    return result


def _docker_bytes(*args, input: bytes = None) -> bytes:
    """Like _docker, for commands whose stdin or stdout is binary."""
    result = subprocess.run(["docker", *args], input=input, capture_output=True,
                            timeout=60.0)
    if result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:4])} ... exited {result.returncode}: "
                           f"{result.stderr.decode(errors='replace').strip()}")
    return result.stdout


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


def _image_files() -> List[Path]:
    # Plain files only, so a stray subdirectory cannot break the copy below.
    return sorted(p for p in IMAGE_DIR.iterdir() if p.is_file())


def _image_tag() -> str:
    digest = hashlib.sha256()
    for path in _image_files():
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
    for path in _image_files():
        shutil.copyfile(path, context / path.name)
    # Under Bazel's "long" (900s) test timeout, so a slow build still
    # reports why it failed.
    result = _docker("build", "--label", "storage-test=1", "-t", tag, str(context),
                     check=False, timeout=600.0)
    if result.returncode != 0:
        pytest.fail(f"Building {tag} failed:\n{result.stderr[-4000:]}", pytrace=False)
    # Older tags are left alone: another run may be about to start one.
    return tag


def _create_network(run_id: str) -> str:
    """Create the run's network, or fall back to Docker's default bridge.

    A host whose other projects hold most of Docker's predefined address
    pools can refuse a new network; the tests need only host-to-container
    traffic, which the default bridge also carries.
    """
    result = _docker("network", "create", "--label", "storage-test=1", run_id, check=False)
    if result.returncode == 0:
        return run_id
    # Docker 29.8's wording for pool exhaustion; other wordings hard-fail.
    if "address pools have been fully subnetted" in result.stderr:
        warnings.warn(f"docker network create {run_id}: {result.stderr.strip()}; "
                      "using the default bridge network")
        return "bridge"
    raise RuntimeError(f"docker network create {run_id} failed: {result.stderr.strip()}")


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
        return _docker_bytes("exec", "-u", REMOTE_USER, member.container,
                             "cat", f"{REMOTE_ROOT}/{rel_path.lstrip('/')}")

    def write_remote(self, member: Member, rel_path: str, data: bytes):
        path = f"{REMOTE_ROOT}/{rel_path.lstrip('/')}"
        _docker_bytes("exec", "-i", "-u", REMOTE_USER, member.container, "sh", "-c",
                      f"mkdir -p \"$(dirname {shlex.quote(path)})\" && cat > {shlex.quote(path)}",
                      input=data)

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
            if not _wait_for(lambda member=member: self.exec(
                    member, "pgrep -u storage -x inotifywait", check=False).returncode != 0,
                    timeout=10.0, interval=0.1):
                raise RuntimeError(f"inotifywait in {member.container} survived pkill")
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

        network = _create_network(run_id)
        if network != "bridge":
            created_network = network

        force_published = os.environ.get(FORCE_PUBLISHED_ENV) == "1"
        members: List[Member] = []
        for index in range(MEMBER_COUNT):
            name = f"remote{index}"
            keys = work / f"keys-{name}"
            keys.mkdir()
            _ssh_keygen(keys / "ssh_host_ed25519_key", f"{run_id}-{name}")
            (keys / "authorized_keys").write_text(client_pub)
            container = f"{run_id}-{name}"
            # create fails on a name clash, and teardown removes by the id it
            # returns, so only a container this run made is ever removed.
            container_id = _docker(
                "create", "--pull=never", "--name", container, "--network", network,
                "--label", "storage-test=1", "-p", "127.0.0.1::22",
                "-v", f"{keys}:/keys:ro", image).stdout.strip()
            created_containers.append(container_id)
            _docker("start", container)
            ip = _docker("inspect", "-f",
                         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                         container).stdout.strip()
            published = _docker("port", container, "22/tcp").stdout.split()[0]
            published_port = int(published.rsplit(":", 1)[1])
            candidates = [("127.0.0.1", published_port)]
            if not force_published:
                candidates.insert(0, (ip, 22))
            answered: List[tuple] = []

            def sshd_answers(candidates=candidates, answered=answered):
                for candidate in candidates:
                    if _ssh_banner(*candidate, 1.0):
                        answered.append(candidate)
                        return True
                return False

            if not _wait_for(sshd_answers, timeout=30.0, interval=0.2):
                logs = _docker("logs", container, check=False)
                pytest.fail(f"sshd in {container} never answered on {candidates} "
                            "(the Docker daemon must be on this host):\n"
                            f"{logs.stdout}{logs.stderr}", pytrace=False)
            host, port = answered[0]
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
        removals = [("rm", "-f", "-v", c) for c in created_containers]
        if created_network is not None:
            removals.append(("network", "rm", created_network))
        for removal in removals:
            try:
                _docker(*removal, check=False, timeout=60.0)
            except subprocess.TimeoutExpired:
                warnings.warn(f"docker {' '.join(removal)} timed out; remove it by hand")
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


def _ssh_to(line: str, member: Member) -> bool:
    """Whether a shim log line is an ssh invocation aimed at this member."""
    args = line.split()
    pairs = list(zip(args, args[1:]))
    # The watcher passes user@host; rsync passes -l user and the bare host.
    target = (f"{REMOTE_USER}@{member.host}" in args
              or (("-l", REMOTE_USER) in pairs and member.host in args))
    return target and ("-p", str(member.port)) in pairs


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
        assert all(_ssh_to(line, member) for line in rsync_calls)

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
    # RuntimeError, not an assertion: the #106 xfail expects only
    # AssertionError, and a watcher that never starts must not pass as it.
    if not _wait_for(lambda: _remote_inotifywait_running(remote, member),
                     timeout=20.0, interval=0.2):
        raise RuntimeError(f"inotifywait never started in {member.container}")


def _wait_for_watch(remote, member, rel_dir: str, events: List[str]):
    """Block until a change under rel_dir reaches the watcher.

    inotifywait -r sets its watches after it starts, so a running process
    is not a ready one: touch probe files until one is reported.
    """
    _wait_for_remote_inotifywait(remote, member)
    deadline = time.monotonic() + 20.0
    index = 0
    while time.monotonic() < deadline:
        probe = "/" + "/".join(part for part in (rel_dir, f".probe-{index}") if part)
        remote.exec(member, f"touch {REMOTE_ROOT}{probe}")
        if _wait_for(lambda: probe in events, timeout=0.5):
            return
        index += 1
    raise RuntimeError(f"the watcher on {member.container} never reported a probe")


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
        _wait_for_watch(remote, member, "watched", events)
        assert not seen.is_set()

        remote.exec(member, f"echo changed > {REMOTE_ROOT}/watched/new-{token}")

        assert seen.wait(15.0), f"no watcher event for the docker exec change; saw {events}"
        # The parent directory is invalidated too. The probes also did that,
        # so look only after the change's own first event.
        first = events.index(f"/watched/new-{token}")
        assert _wait_for(lambda: "/watched" in events[first + 1:], timeout=5.0), events
        assert any("inotifywait" in line and _ssh_to(line, member)
                   for line in remote.ssh_log_lines()[log_before:])
    finally:
        watcher.stop()


# Known failure, #106: stop() kills only the local ssh; without a pty the
# remote inotifywait is reparented to init and lives until its next event.
# strict, so the fix turns this red and the marker comes off; raises, so a
# setup error (no Docker) is still an error and not an expected failure.
@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="#106: remote inotifywait outlives RemoteWatcher.stop()")
def test_watcher_stop_ends_remote_inotifywait(remote):
    member = remote.members[1]
    events: List[str] = []
    watcher = _watcher_for(remote, member, events.append)
    watcher.start()
    try:
        # Stop a fully established watch, not one still setting up.
        _wait_for_watch(remote, member, "", events)
    finally:
        watcher.stop()
    assert _wait_for(lambda: not _remote_inotifywait_running(remote, member),
                     timeout=10.0, interval=0.2), \
        "inotifywait still runs in the container after RemoteWatcher.stop()"


# --- Mounted half: a real FUSE mount of StorageFS over two SSH members ----

MOUNT_TIMEOUT = 30.0


@dataclass
class Mounted:
    fs: object
    mount: Path
    cache: Path
    backends: dict


def _fusermount_unmount(mount: Path) -> List[str]:
    """Unmount; return what went wrong, falling back to a lazy unmount."""
    problems: List[str] = []
    for flags in ("-u", "-uz"):
        for tool in ("fusermount", "fusermount3"):
            if shutil.which(tool) is None:
                continue
            try:
                result = subprocess.run([tool, flags, str(mount)], capture_output=True,
                                        text=True, timeout=30.0)
            except subprocess.TimeoutExpired:
                problems.append(f"{tool} {flags} timed out")
                continue
            if result.returncode == 0:
                return problems
            problems.append(f"{tool} {flags}: {result.stderr.strip()}")
    problems.append(f"{mount} could not be unmounted")
    return problems


@pytest.fixture
def mounted_factory(remote, tmp_path):
    """Factory mounting StorageFS over remote0 and remote1 (both SSHBackend).

    Unlike the other mounted suites this never skips: a mount that does not
    come up is a failure.
    """
    from fuse import FUSE

    from storage.fs import StorageFS

    created: List[tuple] = []

    def make(**fs_kwargs) -> Mounted:
        assert not created, "one mount per test"
        mount = tmp_path / "mount"
        cache = tmp_path / "cache"
        mount.mkdir()
        cache.mkdir()
        backends = {m.name: SSHBackend(**remote.backend_kwargs(m)) for m in remote.members}
        fs_kwargs.setdefault("num_sync_workers", 2)
        fs = StorageFS(cache_root=cache, state_db=tmp_path / "state.db",
                       backends=backends, cache_size=256 * 1024 * 1024, **fs_kwargs)
        errors: List[BaseException] = []

        def run():
            try:
                FUSE(fs, str(mount), nothreads=True, foreground=True, allow_other=False)
            except BaseException as exc:  # reported by the failure below
                errors.append(exc)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        handle = Mounted(fs, mount, cache, backends)
        created.append((handle, thread, errors))
        if not _wait_for(lambda: os.path.ismount(mount) or bool(errors),
                         timeout=MOUNT_TIMEOUT, interval=0.1) or errors:
            pytest.fail(f"FUSE mount did not come up: {errors or 'timed out'}", pytrace=False)
        return handle

    yield make

    problems: List[str] = []
    for handle, thread, errors in created:
        mounted = os.path.ismount(handle.mount)
        if mounted:
            problems += _fusermount_unmount(handle.mount)
        # On unmount libfuse calls StorageFS.destroy, which stops the
        # watchers and sync workers; a hang there leaves this thread alive.
        thread.join(timeout=30.0)
        if thread.is_alive():
            problems.append("the FUSE thread did not finish after unmount "
                            "(StorageFS.destroy hung?)")
        elif not mounted:
            # The mount never came up, so libfuse never ran destroy.
            handle.fs.destroy("/")
        if errors:
            problems.append(f"the FUSE thread raised: {errors}")
        for backend in handle.backends.values():
            backend.close()
    if problems:
        pytest.fail("mount teardown: " + "; ".join(problems), pytrace=False)


def test_mounted_write_syncs_to_both_members(remote, mounted_factory):
    from storage.file_config import FileConfig, set_file_config

    m0, m1 = remote.members
    for member in (m0, m1):
        assert remote.exec(member, f"ls -A {REMOTE_ROOT}").stdout.strip() == "", \
            "members must start empty or the replica check proves nothing"
    mnt = mounted_factory()
    # Replica count is read from the xattr on the cache path, inherited down.
    set_file_config(mnt.cache, FileConfig(replica_count=2))

    payload = secrets.token_bytes(300_000)
    (mnt.mount / "replicated.bin").write_bytes(payload)

    for member in (m0, m1):
        assert _wait_for(
            lambda member=member: remote.exec(
                member, f"cat {REMOTE_ROOT}/replicated.bin 2>/dev/null | wc -c"
            ).stdout.strip() == str(len(payload)), timeout=30.0), \
            f"{member.name} never received the replica"
        assert remote.read_remote(member, "replicated.bin") == payload


def test_mounted_watcher_refreshes_listing(remote, mounted_factory):
    m1 = remote.members[1]
    log_before = len(remote.ssh_log_lines())
    mnt = mounted_factory(enable_watchers=True)
    assert mnt.fs._readdir_snapshot_ttl == float("inf"), \
        "both SSH members must be watchable, else a TTL would mask the watcher"
    os.listdir(mnt.mount)  # primes the snapshot; only the watcher can refresh it

    # A running inotifywait is not a ready one (its watches are set after it
    # starts): create probe files in the member until one shows up in the
    # mount's listing. Not dot-named, so no listing filter can hide them.
    deadline = time.monotonic() + 20.0
    index = 0
    while True:
        probe = f"probe-{index}"
        remote.write_remote(m1, probe, b"")
        if _wait_for(lambda: probe in os.listdir(mnt.mount), timeout=0.5):
            break
        index += 1
        if time.monotonic() >= deadline:
            pytest.fail(f"the mount's watcher on {m1.container} never refreshed the listing",
                        pytrace=False)

    token = secrets.token_hex(4)
    name = f"from-container-{token}.txt"
    body = f"hello {token}\n".encode()
    assert name not in os.listdir(mnt.mount)

    remote.write_remote(m1, name, body)

    assert _wait_for(lambda: name in os.listdir(mnt.mount), timeout=20.0), \
        "the container's new file never appeared in the mount listing"
    assert (mnt.mount / name).read_bytes() == body
    assert any("inotifywait" in line and _ssh_to(line, m1)
               for line in remote.ssh_log_lines()[log_before:]), \
        "the mount's watcher must have run inotifywait over ssh to remote1"


def _streaming_read_case(remote, mounted_factory, monkeypatch, streaming: bool):
    chunk = 64 * 1024
    member = remote.members[0]
    payload = secrets.token_bytes(3 * 1024 * 1024)
    remote.write_remote(member, "stream.bin", payload)

    mnt = mounted_factory(streaming_reads=streaming, streaming_chunk_size=chunk,
                          num_sync_workers=0, start_sync=False)
    ranges: List[tuple] = []
    backend = mnt.backends[member.name]
    original = backend.read_file

    def recording(virtual_path, offset, size):
        ranges.append((virtual_path, offset, size))
        return original(virtual_path, offset, size)

    monkeypatch.setattr(backend, "read_file", recording)
    downloads = _count_downloads(backend, monkeypatch)

    assert "stream.bin" in os.listdir(mnt.mount)
    assert (mnt.mount / "stream.bin").read_bytes() == payload
    return ranges, downloads, chunk


def test_mounted_streaming_range_reads_reach_ssh_member(remote, mounted_factory, monkeypatch):
    ranges, downloads, chunk = _streaming_read_case(remote, mounted_factory, monkeypatch,
                                                    streaming=True)
    assert downloads == [], "a streaming read must not download the whole file"
    assert len(ranges) > 1, f"expected several range reads, saw {ranges}"
    assert all(size <= chunk for _p, _o, size in ranges), ranges
    assert len({offset for _p, offset, _s in ranges}) > 1


def test_mounted_nonstreaming_read_makes_no_range_reads(remote, mounted_factory, monkeypatch):
    """Twin: without streaming the whole file is downloaded, not ranged."""
    ranges, downloads, _chunk = _streaming_read_case(remote, mounted_factory, monkeypatch,
                                                     streaming=False)
    assert ranges == []
    assert downloads == ["/stream.bin"], "the whole file must be downloaded exactly once"
