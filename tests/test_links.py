"""Links made through the mount follow their target (ditz symlink-replication)."""

import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.cache import CacheManager
from storage.links import (
    LinkRefused, find_holders, is_link_checksum, link_checksum, resolve_link_target,
)
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.replication import ReplicationManager
from storage.state import StateManager
from storage.sync_executor import SyncExecutor
from storage.sync_tasks import SyncConfig, SyncTask


class TestResolveLinkTarget(unittest.TestCase):

    def test_relative_targets_resolve_from_the_links_directory(self):
        self.assertEqual(resolve_link_target("/a/b/link", "c.txt"), "/a/b/c.txt")
        self.assertEqual(resolve_link_target("/a/b/link", "../c.txt"), "/a/c.txt")
        self.assertEqual(resolve_link_target("/a/b/link", "./x/./y/"), "/a/b/x/y")
        self.assertEqual(resolve_link_target("/link", "dir/../file"), "/file")
        self.assertEqual(resolve_link_target("/a/link", ".."), "/")

    def test_refused_targets(self):
        for link, target in (("/link", "/etc/passwd"),   # absolute
                             ("/link", "../x"),          # escapes the root
                             ("/a/link", "../../x"),
                             ("/a/link", "b/../../../x"),
                             ("/link", ""),
                             ("/link", "a\0b")):
            with self.assertRaises(LinkRefused, msg=(link, target)):
                resolve_link_target(link, target)

    def test_link_checksum_compares_target_strings(self):
        self.assertEqual(link_checksum("a"), link_checksum("a"))
        self.assertNotEqual(link_checksum("a"), link_checksum("b"))
        self.assertTrue(is_link_checksum(link_checksum("a")))
        self.assertFalse(is_link_checksum("0123abcd"))
        self.assertFalse(is_link_checksum(None))


class _Members(unittest.TestCase):

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.roots = {}
        self.backends = {}
        for name in ("m0", "m1"):
            root = self.temp_dir / name
            root.mkdir()
            self.roots[name] = root
            self.backends[name] = LocalBackend(name, str(root))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)


class TestFindHolders(_Members):

    def test_members_holding_the_target(self):
        (self.roots["m1"] / "t.txt").write_text("t")
        holders, unsure = find_holders("/t.txt", self.backends)
        self.assertEqual([b.name for b in holders], ["m1"])
        self.assertEqual(unsure, [])

    def test_a_link_or_directory_target_counts(self):
        (self.roots["m0"] / "d").mkdir()
        (self.roots["m1"] / "d").symlink_to("elsewhere")
        holders, _ = find_holders("/d", self.backends)
        self.assertEqual(sorted(b.name for b in holders), ["m0", "m1"])

    def test_an_unavailable_member_with_a_location_is_unsure(self):
        self.backends["m0"].set_available(False)
        holders, unsure = find_holders("/t.txt", self.backends, located={"m0"})
        self.assertEqual((holders, unsure), ([], ["m0"]))
        holders, unsure = find_holders("/t.txt", self.backends, located=set())
        self.assertEqual((holders, unsure), ([], []))


class TestLinkSync(_Members):
    """SyncExecutor._process_link_task against two local members."""

    def setUp(self):
        super().setUp()
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.state = StateManager(str(self.temp_dir / "state.db"))
        self.cache = CacheManager(self.cache_dir, self.state, 1024 * 1024)
        self.executor = SyncExecutor(
            worker_id=0, state_manager=self.state, cache_manager=self.cache,
            replication_manager=ReplicationManager(self.state, self.backends),
            config=SyncConfig(monitor_interval=1, max_retries=1, retry_delay=0),
        )

    def tearDown(self):
        self.state.close()
        super().tearDown()

    def _link_row(self, path, target, *, cache=True):
        if cache:
            cache_path = self.cache_dir / path.lstrip("/")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.symlink_to(target)
        now = time.time()
        self.state.set_file_metadata(FileMetadata(
            virtual_path=path, size=len(target), checksum=None, last_access=now,
            last_modified=now, sync_status=SyncStatus.DIRTY, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False,
            encryption_key_id=None, link_target=target,
        ))

    def _sync(self, path):
        self.executor._process_file_task(SyncTask(virtual_path=path))
        return self.state.get_file_metadata(path)

    def _located(self, path):
        return sorted(loc.backend_name for loc in self.state.get_file_locations(path))

    def test_link_goes_only_to_the_members_holding_the_target(self):
        (self.roots["m1"] / "data").mkdir()
        (self.roots["m1"] / "data" / "t.txt").write_text("t")
        self._link_row("/links/l", "../data/t.txt")

        row = self._sync("/links/l")

        self.assertEqual(row.sync_status, SyncStatus.CLEAN)
        self.assertEqual(row.checksum, link_checksum("../data/t.txt"))
        # Created with its directory on the holder, nowhere else.
        self.assertEqual(os.readlink(self.roots["m1"] / "links" / "l"), "../data/t.txt")
        self.assertFalse(os.path.lexists(self.roots["m0"] / "links"))
        self.assertEqual(self._located("/links/l"), ["m1"])

    def test_replicated_target_puts_the_link_on_every_holder(self):
        for name in ("m0", "m1"):
            (self.roots[name] / "t.txt").write_text("t")
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        for name in ("m0", "m1"):
            self.assertEqual(os.readlink(self.roots[name] / "l"), "t.txt")
        self.assertEqual(self._located("/l"), ["m0", "m1"])

    def test_link_to_a_directory(self):
        (self.roots["m0"] / "dir").mkdir()
        self._link_row("/l", "dir")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        self.assertEqual(os.readlink(self.roots["m0"] / "l"), "dir")
        self.assertFalse(os.path.lexists(self.roots["m1"] / "l"))

    def test_same_link_already_on_the_member_is_done(self):
        (self.roots["m0"] / "t.txt").write_text("t")
        (self.roots["m0"] / "l").symlink_to("t.txt")
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        self.assertEqual(self._located("/l"), ["m0"])

    def test_something_else_at_the_name_is_a_conflict_and_kept(self):
        (self.roots["m0"] / "t.txt").write_text("t")
        (self.roots["m0"] / "l").symlink_to("other")
        (self.roots["m1"] / "t.txt").write_text("t")
        (self.roots["m1"] / "l").write_text("a file")
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CONFLICT)
        self.assertEqual(os.readlink(self.roots["m0"] / "l"), "other")
        self.assertEqual((self.roots["m1"] / "l").read_text(), "a file")

    def test_target_not_yet_on_a_member_waits(self):
        # ln -s right after creating the target: it is still only cached.
        (self.cache_dir / "t.txt").write_text("t")
        now = time.time()
        self.state.set_file_metadata(FileMetadata(
            virtual_path="/t.txt", size=1, checksum=None, last_access=now,
            last_modified=now, sync_status=SyncStatus.DIRTY, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.DIRTY)
        self.assertFalse(os.path.lexists(self.roots["m0"] / "l"))
        # The target syncs to m0; the next pass places the link there.
        (self.roots["m0"] / "t.txt").write_text("t")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        self.assertEqual(self._located("/l"), ["m0"])

    def test_target_gone_everywhere_is_an_error(self):
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.ERROR)

    def test_unreachable_holder_leaves_the_link_dirty(self):
        for name in ("m0", "m1"):
            (self.roots[name] / "t.txt").write_text("t")
        now = time.time()
        self.state.set_file_metadata(FileMetadata(
            virtual_path="/t.txt", size=1, checksum="x", last_access=now,
            last_modified=now, sync_status=SyncStatus.CLEAN, replica_count=2,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        self.state.add_file_location(FileLocation(
            virtual_path="/t.txt", backend_name="m1", checksum="x",
            last_synced=0, is_cached=False,
        ))
        self.backends["m1"].set_available(False)
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.DIRTY)
        self.assertEqual(self._located("/l"), ["m0"])
        self.backends["m1"].set_available(True)
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        self.assertEqual(self._located("/l"), ["m0", "m1"])

    def test_a_link_gone_from_the_cache_is_skipped(self):
        (self.roots["m0"] / "t.txt").write_text("t")
        self._link_row("/l", "t.txt", cache=False)
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.DIRTY)
        self.assertFalse(os.path.lexists(self.roots["m0"] / "l"))

    def test_a_file_row_whose_cache_entry_is_a_link_uploads_nothing(self):
        # ditz sync-reads-through-cache-link: never upload what a link names.
        outside = self.temp_dir / "outside.txt"
        outside.write_text("secret")
        (self.cache_dir / "f").symlink_to(outside)
        now = time.time()
        self.state.set_file_metadata(FileMetadata(
            virtual_path="/f", size=6, checksum=None, last_access=now,
            last_modified=now, sync_status=SyncStatus.DIRTY, replica_count=2,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        self._sync("/f")
        for name in ("m0", "m1"):
            self.assertFalse(os.path.lexists(self.roots[name] / "f"))


class TestLinkIndexAudit(TestLinkSync):
    """rebuild-index and reprobe-locations treat link rows as links."""

    def test_reprobe_records_members_with_the_same_link(self):
        from storage.index_audit import reprobe_member_locations

        (self.roots["m0"] / "l").symlink_to("t.txt")
        (self.roots["m1"] / "l").symlink_to("other")
        self._link_row("/l", "t.txt")
        report = reprobe_member_locations(self.state, self.backends, cache_root=self.cache_dir)
        self.assertEqual(self._located("/l"), ["m0"])
        self.assertEqual([i.kind for i in report.issues if i.virtual_path == "/l"],
                         ["member_link_mismatch"])
        # The cached link and its target are untouched.
        self.assertEqual(os.readlink(self.cache_dir / "l"), "t.txt")

    def test_rebuilt_link_rows_sync_again_idempotently(self):
        from storage.index_audit import rebuild_index_from_cache

        (self.roots["m0"] / "t.txt").write_text("t")
        self._link_row("/l", "t.txt")
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        rebuild_index_from_cache(self.cache_dir, self.state)
        row = self.state.get_file_metadata("/l")
        self.assertEqual((row.link_target, row.sync_status), ("t.txt", SyncStatus.DIRTY))
        self.assertEqual(self._sync("/l").sync_status, SyncStatus.CLEAN)
        self.assertEqual(self._located("/l"), ["m0"])


if __name__ == "__main__":
    unittest.main()
