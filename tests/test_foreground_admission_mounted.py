"""Closes through a real mount and its unmount, with the callback cap on.

release and releasedir are not admitted (storage/foreground_admission.py),
so each close the kernel sends reaches StorageFS exactly once, including a
close still in its member upload when the unmount starts: libfuse joins it
before destroy closes the gate. (A close made after a lazy unmount can fail
to reach the daemon at all, with or without the cap; ditz
lazy-unmount-late-release.) The single-threaded mode, without a cap, is the
baseline the capped threaded mount must match.
"""

import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from storage.fs import StorageFS

from tests.fuse_mount import FUSE_THREAD_MODES, fuse_mode_id, run_fuse, stop_fuse

WAIT = 10.0


@pytest.fixture(params=FUSE_THREAD_MODES, ids=fuse_mode_id)
def mounted(request):
    fuse_threads = request.param
    root = Path(tempfile.mkdtemp())
    member_dir, cache_dir, mount_dir = root / "member", root / "cache", root / "mount"
    for path in (member_dir, cache_dir, mount_dir):
        path.mkdir()
    member = LocalBackend("member", str(member_dir))
    fs = StorageFS(
        cache_dir, root / "state.db", {"member": member}, 10 * 1024 * 1024,
        num_sync_workers=0, start_sync=False,
        max_foreground_operations=4 if fuse_threads else None,
    )
    record = {
        "opened": Counter(), "released": Counter(), "releasedir": 0, "opendir": 0,
        "queued": [], "uploads": [], "gate_closed_before_release": [],
    }
    lock = threading.Lock()
    real_open, real_create, real_release = fs.open, fs.create, fs._release
    real_opendir, real_releasedir = fs.opendir, fs.releasedir
    real_queue, real_upload = fs.sync_manager.queue_sync, member.copy_from_local

    def opened(fn):
        def wrapper(*args):
            fh = fn(*args)
            with lock:
                record["opened"][fh] += 1
            return fh
        return wrapper

    def release(path, fh):
        with lock:
            record["released"][fh] += 1
            gate = fs._foreground_admission
            if gate is not None:
                record["gate_closed_before_release"].append(gate._closed)
        return real_release(path, fh)

    def opendir(path):
        with lock:
            record["opendir"] += 1
        return real_opendir(path)

    def releasedir(path, fh):
        with lock:
            record["releasedir"] += 1
        return real_releasedir(path, fh)

    def queue_sync(path, *args, **kwargs):
        with lock:
            record["queued"].append(path)
        return real_queue(path, *args, **kwargs)

    def upload(local_path, virtual_path):
        with lock:
            record["uploads"].append(virtual_path)
        hold = record.get("hold_upload")
        if hold is not None:
            hold[0].set()
            assert hold[1].wait(WAIT), "upload never released"
        return real_upload(local_path, virtual_path)

    fs.open, fs.create, fs._release = opened(real_open), opened(real_create), release
    fs.opendir, fs.releasedir = opendir, releasedir
    fs.sync_manager.queue_sync = queue_sync
    member.copy_from_local = upload

    thread = threading.Thread(
        target=run_fuse, args=(fs, mount_dir), kwargs={"fuse_threads": fuse_threads},
        daemon=True,
    )
    thread.start()
    deadline = time.monotonic() + WAIT
    while not os.path.ismount(mount_dir):
        assert time.monotonic() < deadline, "mount did not come up"
        time.sleep(0.05)
    try:
        yield fs, member_dir, mount_dir, thread, record, fuse_threads
    finally:
        problems = stop_fuse(mount_dir, thread, fs=fs)
        shutil.rmtree(root, ignore_errors=True)
        assert not problems, problems


def test_each_close_runs_once_through_unmount(mounted):
    fs, member_dir, mount_dir, thread, record, fuse_threads = mounted
    (member_dir / "f.txt").write_bytes(b"canonical")
    assert (mount_dir / "f.txt").read_bytes() == b"canonical"
    assert sorted(os.listdir(mount_dir))  # opendir, readdir, releasedir
    (mount_dir / "new.txt").write_bytes(b"new bytes")
    # The member's copy changes; its conflict-qualified name is a member copy.
    (member_dir / "f.txt").write_bytes(b"member copy")

    # Hold that copy's close in its member upload, then unmount: libfuse
    # joins the handler before destroy, so the close finishes exactly once
    # and before destroy closes the gate.
    upload_started, upload_go = threading.Event(), threading.Event()
    record["hold_upload"] = (upload_started, upload_go)
    with open(mount_dir / "f.txt.member", "r+b") as handle:
        handle.write(b"edited")
    assert upload_started.wait(WAIT), "the member-copy close never reached release"
    result = subprocess.run(["fusermount", "-u", str(mount_dir)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert not os.path.ismount(mount_dir)
    assert thread.is_alive() and not fs.destroy_entered.is_set()
    upload_go.set()
    thread.join(WAIT)
    assert not thread.is_alive()
    assert fs.destroy_entered.is_set()

    opened, released = record["opened"], record["released"]
    assert opened and set(opened) == set(released)
    assert all(count == 1 for count in opened.values())
    assert all(count == 1 for count in released.values()), released
    assert record["opendir"] >= 1 and record["releasedir"] == record["opendir"]
    assert record["uploads"] == ["/f.txt"]
    assert (member_dir / "f.txt").read_bytes() == b"edited copy"
    assert "/new.txt" in record["queued"]
    assert (fs.cache_manager.cache_root / "new.txt").read_bytes() == b"new bytes"
    if fuse_threads:
        gate = fs._foreground_admission
        assert gate is not None and gate._closed
        assert gate.active_count == 0
        assert 1 <= gate.peak_active <= 4
        assert not any(record["gate_closed_before_release"])
    else:
        assert fs._foreground_admission is None


if __name__ == "__main__":
    unittest.main()
