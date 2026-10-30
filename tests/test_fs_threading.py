"""Races between FUSE handlers run from several threads (threading stage 2).

StorageFS is driven directly from threads, with Event gates patched into
member calls so each interleaving is forced rather than hoped for. Every
wait has a timeout, so a regression fails instead of hanging.
"""

import dataclasses
import errno
import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
from storage.models import SyncStatus

WAIT = 5.0
# How long a thread that should be blocked is given to (wrongly) finish.
BLOCKED = 0.3


class Gate:
    """Pause the first call of a wrapped function until opened.

    Later calls (and every call when `only` rejects the arguments) run
    straight through. `started` is set when the paused call arrives.
    """

    def __init__(self, real, only=None):
        self.real = real
        self.only = only
        self.started = threading.Event()
        self.opened = threading.Event()
        self.calls = []
        self._lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        with self._lock:
            self.calls.append(args)
            pause = not self.started.is_set() and (self.only is None or self.only(*args))
            if pause:
                self.started.set()
        if pause:
            assert self.opened.wait(WAIT), "gate never opened"
        return self.real(*args, **kwargs)


class Worker(threading.Thread):
    """Run fn in a thread; keep its result or exception."""

    def __init__(self, fn, *args):
        super().__init__(daemon=True)
        self.fn, self.args = fn, args
        self.result = self.error = None

    def run(self):
        try:
            self.result = self.fn(*self.args)
        except BaseException as exc:  # noqa: BLE001 - reported by the test
            self.error = exc

    def finish(self):
        self.join(WAIT)
        assert not self.is_alive(), "worker did not finish"
        if self.error is not None:
            raise self.error
        return self.result


class ThreadingTestBase(unittest.TestCase):
    revalidate_after = 60.0

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.member_dir = self.temp_dir / "member"
        self.cache_dir.mkdir()
        self.member_dir.mkdir()
        self.member = LocalBackend("member", str(self.member_dir))
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=False,
            revalidate_after=self.revalidate_after,
        )
        self.gates = []

    def tearDown(self):
        for gate in self.gates:
            gate.opened.set()
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def gate(self, target, name, only=None):
        gate = Gate(getattr(target, name), only)
        self.gates.append(gate)
        patcher = patch.object(target, name, side_effect=gate)
        patcher.start()
        self.addCleanup(patcher.stop)
        return gate

    def read_all(self, path, fd):
        return self.fs.read(path, 1 << 20, 0, fd)

    def open_read_release(self, path):
        fd = self.fs.open(path, os.O_RDONLY)
        try:
            return self.read_all(path, fd)
        finally:
            self.fs.release(path, fd)

    def cache(self, name, content):
        (self.member_dir / name).write_bytes(content)
        self.assertEqual(content, self.open_read_release("/" + name))
        self.assertEqual(
            SyncStatus.CLEAN, self.fs.state_manager.get_file_metadata("/" + name).sync_status,
        )


class TestOpenRaces(ThreadingTestBase):
    def test_two_opens_of_one_uncached_path_fetch_once(self):
        payload = os.urandom(256 * 1024)
        (self.member_dir / "f.bin").write_bytes(payload)
        copy = self.gate(self.member, "copy_to_local")

        first = Worker(self.open_read_release, "/f.bin")
        first.start()
        self.assertTrue(copy.started.wait(WAIT))
        second = Worker(self.open_read_release, "/f.bin")
        second.start()
        # The second open waits for the first's fetch instead of starting
        # its own (or seeing a half-fetched file).
        second.join(BLOCKED)
        self.assertTrue(second.is_alive())
        copy.opened.set()

        self.assertEqual(payload, first.finish())
        self.assertEqual(payload, second.finish())
        self.assertEqual(1, len(copy.calls))
        self.assertEqual(0, len(self.fs._path_locks))

    def test_listing_and_other_paths_proceed_while_a_fetch_is_gated(self):
        # The headline property: one slow member transfer holds only its
        # own path.
        (self.member_dir / "slow.bin").write_bytes(b"s" * 4096)
        self.cache("other.txt", b"other")
        (self.member_dir / "uncached.txt").write_bytes(b"uncached")
        copy = self.gate(self.member, "copy_to_local",
                         only=lambda path, local: path == "/slow.bin")

        slow = Worker(self.open_read_release, "/slow.bin")
        slow.start()
        self.assertTrue(copy.started.wait(WAIT))

        def browse():
            names = set(self.fs.readdir("/", None))
            attrs = self.fs.getattr("/other.txt")
            return (names, attrs["st_size"],
                    self.open_read_release("/other.txt"),
                    self.open_read_release("/uncached.txt"))

        browser = Worker(browse)
        browser.start()
        names, size, other, uncached = browser.finish()
        self.assertTrue(slow.is_alive())
        self.assertLessEqual({"slow.bin", "other.txt", "uncached.txt"}, names)
        self.assertEqual(5, size)
        self.assertEqual(b"other", other)
        self.assertEqual(b"uncached", uncached)

        copy.opened.set()
        self.assertEqual(b"s" * 4096, slow.finish())

    def test_pin_survives_a_concurrent_revalidate(self):
        self.cache("f.txt", b"original")
        (self.member_dir / "f.txt").write_bytes(b"changed and longer")
        with self.fs._member_checks_lock:
            for path, check in list(self.fs._member_checks.items()):
                self.fs._member_checks[path] = dataclasses.replace(
                    check, verified_at=check.verified_at - 3600,
                )
        # Pause the first opener after its check pinned the refetch and
        # before its fetch consumed the pin.
        invalidate = self.gate(self.fs, "_invalidate_file_cache")
        pins_at_select = []
        real_select = self.fs._select_backend_for_read

        def select(canonical_path):
            with self.fs._member_checks_lock:
                pins_at_select.append(self.fs._refetch_pins.get(canonical_path))
            return real_select(canonical_path)

        copies = self.gate(self.member, "copy_to_local", only=lambda *a: False)
        with patch.object(self.fs, "_select_backend_for_read", side_effect=select):
            first = Worker(self.open_read_release, "/f.txt")
            first.start()
            self.assertTrue(invalidate.started.wait(WAIT))
            second = Worker(self.open_read_release, "/f.txt")
            second.start()
            second.join(BLOCKED)
            self.assertTrue(second.is_alive())
            invalidate.opened.set()
            self.assertEqual(b"changed and longer", first.finish())
            self.assertEqual(b"changed and longer", second.finish())
        # The first opener's fetch found its pin (the second opener's
        # revalidate did not pop it), and the second did not fetch again.
        self.assertEqual([self.member], pins_at_select)
        self.assertEqual(1, len(copies.calls))
        self.assertEqual({}, self.fs._refetch_pins)

    def test_open_of_a_cached_file_is_not_undercut_by_eviction(self):
        self.cache("f.txt", b"cached")
        # Pause the open after it found the file cached, before os.open.
        heal = self.gate(self.fs.cache_manager, "heal_stale_clean")
        opener = Worker(self.open_read_release, "/f.txt")
        opener.start()
        self.assertTrue(heal.started.wait(WAIT))
        # Eviction wants everything, but skips the busy path.
        self.fs.cache_manager.evict_lru_files(10 * 1024 * 1024)
        self.assertTrue((self.cache_dir / "f.txt").exists())
        heal.opened.set()
        self.assertEqual(b"cached", opener.finish())
        # Once the open is done the path is evictable again.
        self.assertTrue(self.fs.cache_manager.evict_lru_files(1))
        self.assertFalse((self.cache_dir / "f.txt").exists())


class TestRenameRaces(ThreadingTestBase):
    def _rename_gated_at_member(self, old, new):
        member_rename = self.gate(self.member, "rename")
        renamer = Worker(self.fs.rename, old, new)
        renamer.start()
        self.assertTrue(member_rename.started.wait(WAIT))
        return member_rename, renamer

    def test_open_of_the_old_name_does_not_rebuild_a_stale_copy(self):
        self.cache("old.txt", b"content")
        gate, renamer = self._rename_gated_at_member("/old.txt", "/new.txt")
        # The cache has moved; the member still has the old name.
        self.assertTrue((self.member_dir / "old.txt").exists())
        opener = Worker(self.fs.open, "/old.txt", os.O_RDONLY)
        opener.start()
        opener.join(BLOCKED)
        self.assertTrue(opener.is_alive())
        gate.opened.set()
        renamer.finish()
        with self.assertRaises(FuseOSError) as ctx:
            opener.finish()
        self.assertEqual(errno.ENOENT, ctx.exception.errno)
        self.assertFalse((self.cache_dir / "old.txt").exists())
        self.assertEqual(b"content", self.open_read_release("/new.txt"))

    def test_write_during_a_rename_is_recorded_at_the_new_path(self):
        self.cache("w.txt", b"before")
        fd = self.fs.open("/w.txt", os.O_RDWR)
        try:
            generation = self.fs.state_manager.get_file_metadata("/w.txt").dirty_generation
            gate, renamer = self._rename_gated_at_member("/w.txt", "/moved.txt")
            # write takes no path lock: it runs inside the rename.
            self.assertEqual(5, self.fs.write("/w.txt", b"after", 0, fd))
            gate.opened.set()
            renamer.finish()
        finally:
            self.fs.release("/moved.txt", fd)
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/w.txt"))
        moved = self.fs.state_manager.get_file_metadata("/moved.txt")
        self.assertEqual(SyncStatus.DIRTY, moved.sync_status)
        self.assertGreater(moved.dirty_generation, generation)
        self.assertEqual(b"after", (self.cache_dir / "moved.txt").read_bytes()[:5])

    def test_directory_rename_waits_for_an_open_beneath_it(self):
        (self.member_dir / "d").mkdir()
        (self.member_dir / "d" / "f.txt").write_bytes(b"inside")
        copy = self.gate(self.member, "copy_to_local")
        opener = Worker(self.open_read_release, "/d/f.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        renamer = Worker(self.fs.rename, "/d", "/e")
        renamer.start()
        renamer.join(BLOCKED)
        self.assertTrue(renamer.is_alive())
        copy.opened.set()
        self.assertEqual(b"inside", opener.finish())
        renamer.finish()
        self.assertEqual(b"inside", self.open_read_release("/e/f.txt"))


class TestWatcherInvalidation(ThreadingTestBase):
    def test_remote_change_waits_for_an_open_of_the_path(self):
        self.cache("f.txt", b"cached")
        heal = self.gate(self.fs.cache_manager, "heal_stale_clean")
        opener = Worker(self.open_read_release, "/f.txt")
        opener.start()
        self.assertTrue(heal.started.wait(WAIT))
        watcher = Worker(self.fs._on_remote_change, "/f.txt")
        watcher.start()
        watcher.join(BLOCKED)
        self.assertTrue(watcher.is_alive())
        heal.opened.set()
        self.assertEqual(b"cached", opener.finish())
        watcher.finish()
        self.assertFalse((self.cache_dir / "f.txt").exists())


if __name__ == "__main__":
    unittest.main()
