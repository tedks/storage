"""
Local cache manager with LRU eviction

Manages the local cache of files from remote backing filesystems.
Files are cached on first access and evicted using LRU when space is needed.
"""

import bisect
import errno
import logging
import os
import stat as stat_module
import secrets
import time
import threading
from pathlib import Path
from contextlib import contextmanager, nullcontext
from enum import Enum
from typing import Callable, ContextManager, Optional, List, Set, Tuple

from .backends.base import Backend, bulk_request
from .backends.ssh_pool import BulkCapacityCancelled, BulkCapacityTimeout
from .cache_metadata import (
    PARTIAL_SUFFIX, PREFIX_XATTR, copy_member_attributes, read_cache_metadata, regular_file_stat,
    write_cache_metadata,
)
from .member_repairs import VIEW_WRITE, MemberRepairs, is_at_or_under
from .pathing import resolve_path

from .models import FileMetadata, SyncStatus
from .state import StateManager

logger = logging.getLogger(__name__)


def _remove_prefix_marker(path: Path):
    """Remove the streaming-prefix marker from a whole-file copy.

    Reading a user xattr needs read permission on the file and changing one
    needs write permission, owner or not, and the copy keeps the member
    file's mode. So the marker is looked for as the file is; only if that
    is refused does the copy get owner read and write for the check and the
    removal, its mode restored after. Most copies have no marker, which
    costs one getxattr. No xattrs on this filesystem: nothing to do. Any
    other failure raises: a copy that keeps the marker would be served
    unindexed for good.
    """
    absent = (errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP)
    try:
        os.getxattr(path, PREFIX_XATTR)
        refused = False
    except OSError as exc:
        if exc.errno in absent:
            return
        if exc.errno not in (errno.EACCES, errno.EPERM):
            raise
        refused = True
    mode = stat_module.S_IMODE(os.stat(path).st_mode)
    usable = mode | stat_module.S_IRUSR | stat_module.S_IWUSR
    if usable != mode:
        os.chmod(path, usable)
    try:
        if refused:
            try:
                os.getxattr(path, PREFIX_XATTR)
            except OSError as exc:
                if exc.errno in absent:
                    return
                raise
        os.removexattr(path, PREFIX_XATTR)
    finally:
        if usable != mode:
            os.chmod(path, mode)


class _PartialCharge:
    """The data blocks one streaming descriptor has written, as sorted,
    disjoint, block-aligned [start, end) ranges, and their total.

    The ranges are what this cache wrote (plus the extents the inode had
    when its first grant measured them), so each later grant knows which
    of its blocks are new without asking the filesystem. Bounded by the
    number of disjoint spans the session fetched. identity is the inode's
    (st_dev, st_ino): while the descriptor is open, eviction leaves any
    name for that inode alone (see CacheManager._held_identities).
    """

    __slots__ = ("ranges", "bytes", "identity")

    def __init__(self, ranges, identity=None):
        self.identity = identity
        self.ranges = list(ranges)
        self.bytes = sum(end - start for start, end in self.ranges)

    def overlap(self, start: int, end: int) -> int:
        ranges = self.ranges
        i = bisect.bisect_left(ranges, (start, -1))
        if i and ranges[i - 1][1] > start:
            i -= 1
        covered = 0
        while i < len(ranges) and ranges[i][0] < end:
            covered += min(end, ranges[i][1]) - max(start, ranges[i][0])
            i += 1
        return covered

    def add(self, start: int, end: int) -> int:
        """Record [start, end) as written; return the bytes it added."""
        if start >= end:
            return 0
        added = end - start - self.overlap(start, end)
        ranges = self.ranges
        i = bisect.bisect_left(ranges, (start, -1))
        if i and ranges[i - 1][1] >= start:
            i -= 1
        j = bisect.bisect_right(ranges, (end, float("inf")))
        if i < j:
            start = min(start, ranges[i][0])
            end = max(end, ranges[j - 1][1])
        ranges[i:j] = [(start, end)]
        self.bytes += added
        return added

    def trim(self, end: int) -> int:
        """Drop everything at or past end; return the bytes dropped."""
        kept = [(s, min(e, end)) for s, e in self.ranges if s < end]
        dropped = self.bytes - sum(e - s for s, e in kept)
        self.ranges = kept
        self.bytes -= dropped
        return dropped


class CacheFetch(Enum):
    FAILED = 0
    INSTALLED = 1
    ALREADY_CACHED = 2


_STAT_NOT_PROBED = object()


class CacheManager:
    """
    Manages local cache with LRU eviction

    The cache stores complete files downloaded from remote backends.
    Access (read or write) updates the LRU timestamp.
    """

    def __init__(self, cache_root: Path, state_manager: StateManager,
                 max_size_bytes: int, bulk_cold_copies: bool = False):
        """
        Initialize cache manager

        Args:
            cache_root: Root directory for cache storage
            state_manager: State manager instance
            max_size_bytes: Maximum cache size in bytes
            bulk_cold_copies: Reclassify whole-file copies on a threaded mount
        """
        self.cache_root = cache_root
        # The renames members missed, read from the cache's own carriers
        # before anything can sync, sweep or evict: every component that
        # fences a member or pins a file asks this (storage/member_repairs.py).
        # An unreadable carrier raises CorruptRepairRecord: no open.
        self.member_repairs = MemberRepairs(Path(cache_root), sweep_new=True)
        # Set by StorageFS: a context manager that holds the descriptor
        # table lock and yields whether the path has an open descriptor.
        # Eviction stats and unlinks inside it, so no descriptor can be
        # opened on the file in between, and none open can be missed: bytes
        # written through one after the stat would die with the inode.
        # None (no StorageFS attached: tests, CLI tools) means no descriptors.
        self.descriptor_guard: Optional[Callable[[str], ContextManager[bool]]] = None
        # Set by StorageFS: NamespaceService.forget_listed_attrs, which
        # StorageFS._invalidate_file_cache also calls. Called before a cached
        # file is evicted and when one is recorded dirty, so getattr never
        # answers for a vanished copy from a directory listing that may
        # predate its last write. It takes only a leaf lock and is safe
        # under every lock here. None (no StorageFS) means no listings.
        self.forget_listed_attrs: Optional[Callable[..., None]] = None
        # Set by StorageFS: a context manager that try-acquires the outer
        # locks of a canonical path (storage/locks.py) and yields whether it
        # got them. Eviction runs under self._lock, inside those locks'
        # order, so it never waits for one: a path another thread's open,
        # rename or upload holds is skipped (the locks are reentrant, so a
        # path the evicting thread holds itself is not). None (no
        # StorageFS) means no contention.
        self.path_guard: Optional[Callable[[str], ContextManager[bool]]] = None
        self.state_manager = state_manager
        self.bulk_cold_copies = bulk_cold_copies
        self.max_size_bytes = max_size_bytes
        self._lock = threading.RLock()
        self.cache_root.mkdir(parents=True, exist_ok=True)
        filesystem = os.statvfs(self.cache_root)
        self.block_size = filesystem.f_frsize or filesystem.f_bsize
        # Keep whole-copy reservations separate from the normal tree scan.
        # Streaming data is charged immediately, including an unlinked
        # partial until its last session descriptor closes.
        self._whole_file_temps: dict[Path, int] = {}
        self._streaming_fds: dict[int, _PartialCharge] = {}
        self._current_size_bytes = self._scan_total_cache_size()
        self._size_dirty = False
        # The last eviction dry run that found too few victims: (epoch,
        # index change count, monotonic time, bytes it could free). Until
        # one of those moves, or SHORTFALL_TTL passes, a request for more
        # than that fails without walking the index again (see
        # _known_shortfall). Only a request it would fail anyway is refused.
        self._shortfall: Optional[Tuple[int, int, float, int]] = None
        self._evictable_epoch = 0
        self._grant_warning_logged = False

    # How long a dry run's "too few victims" stands without any change the
    # cache or the index can see (a path lock released, a member repair
    # resolved). A stale answer only keeps a stream transient for this long.
    SHORTFALL_TTL_SECONDS = 1.0

    def _round_up(self, size: int) -> int:
        return (size + self.block_size - 1) // self.block_size * self.block_size

    def _extent_ranges(self, fd: int, start: int, end: int) -> List[Tuple[int, int]]:
        """Allocated data extents within a block-aligned interval, as
        sorted, disjoint, block-aligned ranges (SEEK_DATA/SEEK_HOLE)."""
        ranges = []
        while start < end:
            try:
                data = os.lseek(fd, start, os.SEEK_DATA)
            except OSError as exc:
                if exc.errno == errno.ENXIO:  # no data before EOF
                    break
                raise
            start = data // self.block_size * self.block_size
            if start >= end:
                break
            hole = os.lseek(fd, data, os.SEEK_HOLE)
            next_block = min(end, self._round_up(hole))
            if ranges and ranges[-1][1] == start:
                ranges[-1] = (ranges[-1][0], next_block)
            else:
                ranges.append((start, next_block))
            start = next_block
        return ranges

    def _data_bytes(self, fd: int, start: int, end: int) -> int:
        """Count allocated data blocks intersecting a block-aligned interval."""
        return sum(stop - begin for begin, stop in self._extent_ranges(fd, start, end))

    def _path_data_bytes(self, path: Path, st: os.stat_result) -> int:
        if st.st_size == 0 or st.st_blocks == 0:
            return 0
        rounded = self._round_up(st.st_size)
        # No shortcut on st_blocks alone: it counts metadata (an external
        # xattr block) as well as data, and a sparse inode charged by its
        # data extents while streaming must be counted the same once it is
        # a named file.
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                opened = os.fstat(fd)
                if (opened.st_dev, opened.st_ino) != (st.st_dev, st.st_ino):
                    return 0
                return min(self._data_bytes(fd, 0, rounded), opened.st_blocks * 512)
            finally:
                os.close(fd)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return 0
            # Use allocated sectors when the file cannot be opened for an
            # extent query or the filesystem does not support extent seeks.
            return min(rounded, st.st_blocks * 512)

    def _held_identities(self):
        """Inodes a streaming descriptor still holds; caller holds the lock.

        Such an inode is charged through its descriptor until
        close_streaming_fd, even once it has a name in the tree (an
        installed fill, a promoted prefix): unlinking that name frees no
        blocks while the descriptor is open, so eviction skips it and a
        direct change to it is left to a rescan."""
        return {charge.identity for charge in self._streaming_fds.values()}

    def _scan_total_cache_size(self) -> int:
        """Reconcile data blocks, counting named and held inodes only once."""
        total = sum(charge.bytes for charge in self._streaming_fds.values())
        seen = set()
        for fd in self._streaming_fds:
            st = os.fstat(fd)
            seen.add((st.st_dev, st.st_ino))
        for root, dirs, files in os.walk(self.cache_root):
            for name in files:
                path = Path(root) / name
                if path in self._whole_file_temps:
                    continue
                st = regular_file_stat(path)
                if st is None:
                    continue
                identity = (st.st_dev, st.st_ino)
                if identity in seen:
                    continue
                if st.st_nlink > 1:
                    seen.add(identity)
                total += self._path_data_bytes(path, st)
        return total

    def _ensure_size_current_locked(self):
        """Refresh tracked size if it has been marked dirty by direct writes."""
        if self._size_dirty:
            self._current_size_bytes = self._scan_total_cache_size()
            self._size_dirty = False

    def recover_views(self, member_names: List[str]):
        """At mount start, before any worker, eviction or FUSE request: drop
        surviving aliases of kept view bytes, then keep the cached conflict
        views of fenced members (see the two methods). member_names are the
        configured members in their order, which decides what a cache name
        means, as in resolve_path."""
        self._break_kept_aliases(member_names)
        self._keep_fenced_views(member_names)
        # Names it unlinked no longer count in the cache.
        self.mark_cache_size_dirty()

    def _inside_cache(self, path: Path) -> bool:
        """Whether path's directory is inside the cache with no symlinked
        component: a name the sweep may read, link or unlink."""
        try:
            relative = path.parent.relative_to(self.cache_root)
        except ValueError:
            return False
        root = Path(os.path.realpath(self.cache_root))
        return Path(os.path.realpath(path.parent)) == root / relative

    def _view_identity(self, cache_file: Path, member_names: List[str]):
        """(member, canonical path) a cache file is a conflict view of, as
        resolve_path reads its name with the configured members, or None
        (an ordinary file, or a member no longer configured)."""
        requested = "/" + str(cache_file.relative_to(self.cache_root))
        resolved = resolve_path(requested, member_names, self.get_cache_path)
        if resolved.backend_name is None:
            return None
        return resolved.backend_name, resolved.canonical_path

    def _break_kept_aliases(self, member_names: List[str]):
        """Unlink every conflict-view cache name that is still a second name
        for bytes a view-write repair keeps: release unlinks it after the
        record is durable, but a crash or a failed unlink can leave it, and
        a cache name may later be written in place. Only a name whose inode
        is the live payload's, inside the cache, and read as that member's
        view of that path, is removed; anything else stays."""
        for record in self.member_repairs.list():
            if record.kind != VIEW_WRITE:
                continue
            for member in record.members:
                alias = self.get_cache_path(f"{record.new_path}.{member}")
                if (not self._inside_cache(alias)
                        or self._view_identity(alias, member_names) != (member, record.new_path)):
                    continue
                try:
                    if self.member_repairs.break_alias(alias):
                        logger.warning("Removed a cache alias of the bytes kept by repair %s",
                                       record.id)
                except OSError as exc:
                    logger.error("Could not remove the cache alias %s of repair %s: %s",
                                 alias, record.id, exc)

    def _keep_fenced_views(self, member_names: List[str]):
        """Keep, as view-write repairs, the cached conflict views of members
        that a repair fences at those names.

        A write through a view goes to its member at release, after a
        successful flush. If a repair appeared in between and the process
        died before the release, nothing recorded it. Any such view is kept
        here, before anything can evict or refetch it: conservatively, since
        an unwritten view is indistinguishable (CTO RRS-CTO-FLUSH-RELEASE).
        A candidate is kept only with positive identity: resolve_path, with
        the configured members in order, must read its name as that
        member's view of a path at or under the repair's names. A file it
        reads otherwise (an ordinary file, another member's view, a member
        no longer configured) is left in place, untouched, with a warning
        if its name ends like the fenced member's view. Nothing outside the
        cache, or under a symlinked component, is walked, linked or
        unlinked.
        """
        repairs = self.member_repairs
        for record in repairs.list():
            for member in record.members:
                suffix = f".{member}"
                for root in {record.old_path, record.new_path}:
                    candidates = [self.get_cache_path(root + suffix)]
                    tree = self.get_cache_path(root)
                    if (tree.is_dir() and not tree.is_symlink()
                            and self._inside_cache(tree / "x")):
                        for dirpath, _dirs, files in os.walk(tree):
                            candidates.extend(Path(dirpath) / name for name in files
                                              if name.endswith(suffix))
                    for view in candidates:
                        self._keep_one_view(record, member, view, member_names)

    def _keep_one_view(self, record, member: str, view: Path, member_names: List[str]):
        repairs = self.member_repairs
        try:
            st = os.lstat(view)
        except FileNotFoundError:
            return
        if not stat_module.S_ISREG(st.st_mode) or not self._inside_cache(view):
            return
        identity = self._view_identity(view, member_names)
        if (identity is None or identity[0] != member
                or not any(is_at_or_under(identity[1], name)
                           for name in (record.old_path, record.new_path))):
            logger.warning("Left %s in place: it is not %s's conflict view under repair %s",
                           view, member, record.id)
            return
        if repairs.kept_inode(view):
            return
        missing = repairs.matches_missing_payload(view)
        if missing is not None:
            logger.warning("Left %s in place: repair %s's bytes are missing and this file "
                           "cannot be proven to be them", view, missing)
            return
        try:
            repairs.preserve_view_write(
                identity[1], member, view, f"{st.st_dev}:{st.st_ino}",
                "conflict view of a fenced member found when the cache "
                "opened; it may hold a write never sent")
        except OSError as exc:
            logger.error("Could not keep the conflict view %s of fenced member %s: %s",
                         view, member, exc)

    def get_cache_path(self, virtual_path: str) -> Path:
        """Get the local cache path for a virtual file"""
        # Remove leading slash and resolve within cache
        relative_path = virtual_path.lstrip('/')
        return self.cache_root / relative_path

    def is_cached(self, virtual_path: str) -> bool:
        """Check if a file is in the cache"""
        return regular_file_stat(self.get_cache_path(virtual_path)) is not None

    def get_cached_size(self, virtual_path: str) -> int:
        """Get size of cached file"""
        st = regular_file_stat(self.get_cache_path(virtual_path))
        return st.st_size if st is not None else 0

    def get_total_cache_size(self) -> int:
        """Return materialized data bytes, including in-flight whole copies."""
        with self._lock:
            self._ensure_size_current_locked()
            return self._current_size_bytes + sum(
                self._temp_data_bytes(path) for path in self._whole_file_temps
            )

    def _temp_data_bytes(self, path: Path) -> int:
        st = regular_file_stat(path)
        return self._path_data_bytes(path, st) if st is not None else 0

    def mark_cache_size_dirty(self):
        """Mark cache size accounting as stale after direct external file mutations."""
        with self._lock:
            self._size_dirty = True

    def note_evictable_change(self):
        """Forget a remembered victim shortfall: a copy may be evictable now
        (a descriptor closed, a copy was installed).

        Takes no lock, so a release never waits behind an eviction pass. A
        lost race between two notes still moves the epoch.
        """
        self._evictable_epoch += 1

    def _direct_charge(self, path: Optional[Path], fd: Optional[int],
                       span: Optional[Tuple[int, int]]):
        """(inode identity or None, data bytes, stat or None) for
        cache_size_change, or None when the change cannot be charged
        incrementally.

        Only a regular file with exactly one name qualifies: the tree scan
        counts such an inode once, at that name. A hard-linked or unlinked
        inode, one a streaming descriptor still holds (counted through the
        descriptor), or a measurement error, makes the caller rescan
        instead. With a span only the data blocks it touches are measured,
        and the stat is returned for _span_delta.
        """
        try:
            if fd is not None:
                st = os.fstat(fd)
            else:
                st = regular_file_stat(path)
                if st is None:
                    return (None, 0)
            if not stat_module.S_ISREG(st.st_mode) or st.st_nlink != 1:
                return None
            identity = (st.st_dev, st.st_ino)
            if identity in self._held_identities():
                return None
            if fd is None:
                return identity, self._path_data_bytes(path, st), None
            if span is None:
                return identity, self._fd_data_bytes(fd, st), None
            start = span[0] // self.block_size * self.block_size
            return identity, self._data_bytes(fd, start, self._round_up(span[1])), st
        except OSError:
            return None

    def _fd_data_bytes(self, fd: int, st: os.stat_result) -> int:
        """What the tree scan charges a named inode (_path_data_bytes): its
        data extents, at most its allocated blocks."""
        return min(self._data_bytes(fd, 0, self._round_up(st.st_size)), st.st_blocks * 512)

    def _span_delta(self, fd: int, before, after) -> Optional[int]:
        """The change in a named inode's charge across a pwrite of a span,
        from the span's data blocks measured before and after it.

        The charge is min(data extents, allocated blocks). While the
        rounded size is within the allocation on both sides, the extents
        are below the cap, and the change is the span's. Otherwise (a
        compressed or sparse inode with metadata blocks) the cap may bind,
        and the whole inode is measured once, after: the extents outside
        the span did not change (every write to a named cache inode runs
        under the cache lock), so the extents before are those after minus
        the span's growth. None when the measurement fails.
        """
        _, span_before, st_before = before
        _, span_after, st_after = after
        cap_before = st_before.st_blocks * 512
        cap_after = st_after.st_blocks * 512
        if (self._round_up(st_before.st_size) <= cap_before
                and self._round_up(st_after.st_size) <= cap_after):
            return span_after - span_before
        try:
            whole_after = self._data_bytes(fd, 0, self._round_up(st_after.st_size))
        except OSError:
            return None
        whole_before = max(0, whole_after - span_after + span_before)
        return min(whole_after, cap_after) - min(whole_before, cap_before)

    @contextmanager
    def cache_size_change(self, path: Optional[Path] = None, *, fd: Optional[int] = None,
                          span: Optional[Tuple[int, int]] = None):
        """Serialize a direct write or truncate with grants, charging its blocks.

        Give the cache file it changes: a path (a truncate, an O_TRUNC open,
        a create) or an open descriptor, and for a pwrite the byte span
        [start, end) it covers. The data blocks are measured before and
        after the change under the cache lock and the difference charged,
        so the next grant needs no rescan. With no target, a target that
        does not qualify (see _direct_charge), or a path whose inode
        changed in between, the change is unknown and the next allocation
        rescans the tree.
        """
        with self._lock:
            known = path is not None or fd is not None
            before = self._direct_charge(path, fd, span) if known else None
            try:
                yield
            finally:
                after = self._direct_charge(path, fd, span) if before is not None else None
                delta = None
                if (after is not None and after[0] is not None
                        and (before[0] is None or before[0] == after[0])):
                    delta = after[1] - before[1]
                    if after[2] is not None and before[2] is not None:
                        delta = self._span_delta(fd, before, after)
                if delta is None:
                    self._size_dirty = True
                else:
                    self._current_size_bytes = max(0, self._current_size_bytes + delta)

    def get_free_space_needed(self, required_bytes: int, *,
                              finishing: Optional[Tuple[Path, int]] = None) -> int:
        """
        Calculate how much space needs to be freed

        An in-flight whole copy counts at least its reservation, or its
        apparent allocation if larger. finishing, (temp path, data bytes),
        counts a finished copy by the data bytes measured for it instead:
        what the total charges it once installed.

        Returns:
            Bytes to free (0 if enough space available)
        """
        with self._lock:
            self._ensure_size_current_locked()
            reserved = 0
            for path, size in self._whole_file_temps.items():
                if finishing is not None and path == finishing[0]:
                    reserved += finishing[1]
                    continue
                st = regular_file_stat(path)
                apparent_growth = min(self._round_up(st.st_size), st.st_blocks * 512) if st else 0
                reserved += max(size, apparent_growth)
            available = self.max_size_bytes - self._current_size_bytes - reserved

        if available >= required_bytes:
            return 0

        return required_bytes - available

    def _path_guard(self, virtual_path: str) -> ContextManager[bool]:
        return self.path_guard(virtual_path) if self.path_guard else nullcontext(True)

    def evict_lru_files(self, bytes_needed: int) -> bool:
        """Evict CLEAN, non-recomputable cache entries in access order."""
        return self._evict_by_access(bytes_needed, recomputable=False)

    def evict_recomputable_files(self, bytes_needed: int) -> bool:
        """Reclaim CLEAN, recomputable cache entries in access order."""
        return self._evict_by_access(bytes_needed, recomputable=True)

    def _evict_by_access(
        self, bytes_needed: int, *, recomputable: Optional[bool], dry_run: bool = False,
        count: bool = False,
    ):
        """Whether bytes_needed were freed (or, dry, could be); with count,
        the bytes found instead, stopping once they reach bytes_needed."""
        if bytes_needed <= 0:
            return 0 if count else True
        with self._lock:
            freed = 0
            for virtual_path in self.state_manager.iter_lru_paths(recomputable=recomputable):
                cache_path = self.get_cache_path(virtual_path)
                with self._path_guard(virtual_path) as free:
                    if not free or regular_file_stat(cache_path) is None:
                        continue
                    try:
                        freed += self._evict_if_still_clean(
                            virtual_path, cache_path, recomputable=recomputable, dry_run=dry_run,
                        )
                    except OSError as e:
                        print(f"Warning: Failed to evict {virtual_path}: {e}")
                if freed >= bytes_needed:
                    return freed if count else True
            return freed if count else False

    def ensure_space(self, bytes_needed: int) -> bool:
        """Free the minimum whole-file LRU victims for a cache allocation."""
        with self._lock:
            if bytes_needed > self.max_size_bytes:
                return False
            return self._free_space(self.get_free_space_needed(bytes_needed))

    def _free_space(self, needed: int) -> bool:
        """Evict CLEAN copies in access order until `needed` bytes are freed.

        A dry run first keeps useful copies when even every eligible victim
        would leave the allocation over budget. It is best effort, not a
        guarantee: opens, closes and index changes do not take the cache
        lock, so the real pass can find fewer victims than the dry run did,
        evict some copies, and still fail. A dry run that falls short is
        remembered (see _known_shortfall), so a stream asking chunk after
        chunk does not walk the whole index each time. Caller holds the lock.
        """
        if needed <= 0:
            return True
        if self._known_shortfall(needed):
            return False
        # The epoch is read before the walk, so a close during it voids the
        # result. The index count is read after: the walk flushes pending
        # access times itself. A row change during the walk is caught by the
        # TTL.
        epoch = self._evictable_epoch
        freeable = self._freeable_bytes(needed)
        if freeable < needed:
            self._shortfall = (epoch, self.state_manager.change_count(),
                               time.monotonic(), freeable)
            return False
        self._shortfall = None
        return self._evict_by_access(needed, recomputable=None)

    def _known_shortfall(self, needed: int) -> bool:
        shortfall = self._shortfall
        if shortfall is None:
            return False
        epoch, changes, at, freeable = shortfall
        if (epoch != self._evictable_epoch
                or changes != self.state_manager.change_count()
                or time.monotonic() - at >= self.SHORTFALL_TTL_SECONDS):
            self._shortfall = None
            return False
        return needed > freeable

    def _freeable_bytes(self, needed: int) -> int:
        """Bytes a real eviction pass could free, counted up to `needed`."""
        return self._evict_by_access(needed, recomputable=None, dry_run=True, count=True)

    def _grants_unavailable(self, reason: str) -> bool:
        """Refuse a streaming grant the filesystem cannot measure; log it once."""
        if not self._grant_warning_logged:
            self._grant_warning_logged = True
            logger.warning(
                "Some streaming reads are not being cached in %s: %s. Those "
                "reads are served from the member without keeping their bytes. "
                "Use a cache filesystem with exact SEEK_DATA/SEEK_HOLE (ext4, "
                "xfs, btrfs, tmpfs). Logged once.",
                self.cache_root, reason,
            )
        return False

    def write_streaming_chunk(self, fd: int, offset: int, data: bytes) -> bool:
        """Keep a chunk when its new data blocks fit, else leave the inode alone.

        The caller serializes writes on this sparse inode and fetches the
        network bytes before entering. The cache lock orders the grant and
        the local write against every other allocation.

        The first grant on a descriptor measures the inode's extents once
        (SEEK_DATA/SEEK_HOLE) and refuses, logging once, when they exceed
        the blocks it has allocated (st_blocks): a filesystem that reports
        holes as data cannot be charged. From then on the charge is the
        blocks this descriptor has written (_PartialCharge), so a grant
        costs a lookup in that range list, not an extent walk, and an
        extent report that goes wrong later cannot hide a block the budget
        has not paid for. On a filesystem that allocates less than it is
        given (compression), that only overstates the charge.
        """
        if not data:
            return True
        start = offset // self.block_size * self.block_size
        end = self._round_up(offset + len(data))
        with self._lock:
            st = os.fstat(fd)
            if st.st_nlink == 0:
                return False
            charge = self._streaming_fds.get(fd)
            if charge is None:
                try:
                    extents = self._extent_ranges(fd, 0, self._round_up(st.st_size))
                except OSError as exc:
                    if exc.errno in (errno.EINVAL, errno.ENOTSUP, errno.ENOSYS):
                        return self._grants_unavailable(f"extent seeks failed ({exc.strerror})")
                    raise
                charge = _PartialCharge(extents, (st.st_dev, st.st_ino))
                if charge.bytes > st.st_blocks * 512:
                    return self._grants_unavailable("the filesystem reports sparse holes as data")
                # The tree scan counted this inode at its path; from here it
                # is counted through the descriptor, at the same bytes.
                self._streaming_fds[fd] = charge
            self._ensure_size_current_locked()
            required = end - start - charge.overlap(start, end)
            if required and not self.ensure_space(required):
                return False
            cursor = offset
            try:
                remaining = memoryview(data)
                while remaining:
                    written = os.pwrite(fd, remaining, cursor)
                    if written <= 0:
                        raise OSError(errno.EIO, "partial cache write made no progress")
                    remaining = remaining[written:]
                    cursor += written
            finally:
                # Only the blocks bytes reached; nothing here can raise and
                # replace the write's own error.
                if cursor > offset:
                    growth = charge.add(start, self._round_up(cursor))
                    self._current_size_bytes += growth
        return True

    def close_streaming_fd(self, fd: int):
        """Close a session descriptor and settle its inode's charge.

        The charge stays with the descriptor until it closes, which is after
        every reader's: a FUSE reader of the partial holds a session
        reference, StorageFS._release closes the reader's descriptor before
        it drops that reference, and the session closes this descriptor only
        once no references remain (or at unmount). It stays there even once
        the inode is named in the tree (an installed fill, a promoted
        prefix), so eviction leaves it alone until now (_held_identities).

        An unlinked inode's charge is released. A named one moves into tree
        accounting: with one name, the charge becomes what the scan counts
        for it; otherwise the next allocation rescans. Either way a copy may
        be evictable now, so a remembered shortfall is forgotten.
        """
        with self._lock:
            charge = self._streaming_fds.pop(fd, None)
            try:
                if charge is not None:
                    st = os.fstat(fd)
                    if st.st_nlink == 0:
                        self._current_size_bytes = max(0, self._current_size_bytes - charge.bytes)
                    elif st.st_nlink == 1 and not self._size_dirty:
                        self._current_size_bytes = max(
                            0, self._current_size_bytes + self._fd_data_bytes(fd, st) - charge.bytes)
                    else:
                        self._size_dirty = True
            except OSError:
                self._size_dirty = True
            finally:
                self.note_evictable_change()
                os.close(fd)

    def truncate_streaming_fd(self, fd: int, size: int):
        """Keep accounting in step when a failed fill preserves only a prefix.

        The charge drops the blocks past the new end from what this
        descriptor wrote; nothing is measured, so once the truncate has
        succeeded nothing here can fail.
        """
        with self._lock:
            os.ftruncate(fd, size)
            charge = self._streaming_fds.get(fd)
            if charge is not None:
                self._current_size_bytes = max(
                    0, self._current_size_bytes - charge.trim(self._round_up(size)))

    def cache_file(self, virtual_path: str, backend: Backend, source_virtual_path: str = None) -> bool:
        """Ensure a member file is cached, including when another fill won."""
        return self.cache_file_result(virtual_path, backend, source_virtual_path) != CacheFetch.FAILED

    def cache_file_result(
        self, virtual_path: str, backend: Backend, source_virtual_path: str = None,
        *, member_stat=_STAT_NOT_PROBED,
    ) -> CacheFetch:
        """
        Download a file from backend into cache

        The cache lock is not held across member I/O: space is reserved
        under it, the bytes land in a temp file beside the cache path and
        are installed only if the cache path is still absent
        (fetch_into_cache), so nothing sees a half-fetched file as cached
        and a file that appeared meanwhile, possibly written, is kept.

        Args:
            virtual_path: Virtual path of file
            backend: Backend to download from
            source_virtual_path: Virtual path to read from backend (defaults to virtual_path)
            member_stat: Pre-transfer probe, or None for a failed probe. If omitted, probe here.

        Returns:
            Whether this copy was installed, another copy won, or the fetch failed.

        Raises:
            OSError with ENOSPC or EDQUOT when the cache has no room for the
            copy (its budget or the filesystem), and what the member probes
            raise.
        """
        source_virtual_path = source_virtual_path or virtual_path
        # Member probes: no lock, and their errors propagate as before.
        if not backend.exists(source_virtual_path):
            return CacheFetch.FAILED
        if member_stat is _STAT_NOT_PROBED:
            try:
                member_stat = backend.stat(source_virtual_path)
            except (OSError, NotImplementedError):
                member_stat = None
        file_size = getattr(member_stat, "st_size", None)
        if file_size is None:
            file_size = backend.get_size(source_virtual_path)

        try:
            installed = self.fetch_into_cache(
                backend, source_virtual_path, self.get_cache_path(virtual_path), member_stat,
                expected_size=file_size,
            )
            return CacheFetch.INSTALLED if installed else CacheFetch.ALREADY_CACHED
        except (BulkCapacityCancelled, BulkCapacityTimeout):
            raise
        except OSError as e:
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                # No room in the cache is not a failed member: another
                # member or a retry would only download the file again to
                # hit the same limit. The open reports it as it is.
                raise
            print(f"Error caching {virtual_path} from {backend.name}: {e}")
            return CacheFetch.FAILED
        except Exception as e:
            print(f"Error caching {virtual_path} from {backend.name}: {e}")
            return CacheFetch.FAILED

    def fetch_into_cache(self, backend: Backend, source_virtual_path: str, cache_path: Path,
                         member_stat=None, *, expected_size: int = 0) -> bool:
        """Copy a member file to cache_path through a temp file beside it.

        The copy runs without the cache lock into a fresh name that ends in
        PARTIAL_SUFFIX, so listings hide it and the startup sweep removes
        it after a crash. It is then hard-linked to cache_path, which fails
        if anything is there already: a concurrent fetch that installed
        first, a create, or a copy a writer has since changed is never
        replaced, and the existing inode keeps its bytes and xattrs. The
        link and the size accounting run under the lock. The cache
        filesystem must support hard links (it must support xattrs anyway).
        The reservation covers this temp file until publication or cleanup.
        Publication needs room only for data beyond the reservation (a
        member file that grew during the copy): an unbudgeted local write
        that took the cache over its limit meanwhile does not undo a copy
        whose space was already promised to it.

        Returns True when this copy was installed, False when a file was
        already there and this copy was discarded. Raises what the copy or
        the link raised (FileExistsError when something other than a
        regular file is there), with the temp file removed.
        """
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.parent / f".{secrets.token_hex(8)}{PARTIAL_SUFFIX}"
        with self._lock:
            reserved = self._round_up(expected_size)
            if not self.ensure_space(reserved):
                raise OSError(errno.ENOSPC, "insufficient cache space")
            self._whole_file_temps[temp_path] = reserved
        try:
            with bulk_request() if self.bulk_cold_copies else nullcontext():
                backend.copy_to_local(source_virtual_path, temp_path)
            st = os.lstat(temp_path)
            if not stat_module.S_ISREG(st.st_mode):
                raise OSError(errno.EINVAL, "download did not produce a regular file", temp_path)
            # A whole file is never a streaming prefix, whatever xattrs the
            # member's copy came with.
            _remove_prefix_marker(temp_path)
            if not st.st_mode & stat_module.S_IRUSR:
                os.chmod(temp_path, stat_module.S_IMODE(st.st_mode) | stat_module.S_IRUSR,
                         follow_symlinks=False)
            fd = os.open(temp_path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                copy_member_attributes(fd, member_stat)
            finally:
                os.close(fd)
            with self._lock:
                try:
                    size = self._temp_data_bytes(temp_path)
                    # A copy that grew spends space only if it can still be
                    # installed: something already at the path (another
                    # fill, a writer) wins at the link below, and this copy
                    # is discarded without evicting anything for it.
                    if (size > reserved and not os.path.lexists(cache_path)
                            and not self._fits_beyond_reservation(temp_path, size - reserved, size)):
                        raise OSError(errno.ENOSPC, "insufficient cache space")
                    try:
                        os.link(temp_path, cache_path)
                    except FileExistsError:
                        if regular_file_stat(cache_path) is None:
                            raise
                        return False
                    self._current_size_bytes += size
                    self.note_evictable_change()
                    return True
                finally:
                    self._drop_whole_temp(temp_path)
        except BaseException:
            with self._lock:
                if temp_path in self._whole_file_temps:
                    self._drop_whole_temp(temp_path)
            raise

    def _fits_beyond_reservation(self, temp_path: Path, excess: int, size: int) -> bool:
        """Make room for a finished copy's `excess` bytes past its reservation.

        Caller holds the lock with the temp file still reserved, and the
        shortfall counts the copy at `size`, its measured data bytes (what
        the total will charge), not its allocation. Only the part of that
        shortfall the excess accounts for is evicted for: what other writers
        overspent is not this copy's to free.
        """
        if size > self.max_size_bytes:
            return False
        return self._free_space(min(excess, self.get_free_space_needed(0, finishing=(temp_path, size))))

    def _drop_whole_temp(self, path: Path):
        """Release the temp reservation under the cache lock."""
        try:
            path.unlink(missing_ok=True)
        except OSError:
            self._size_dirty = True
        finally:
            del self._whole_file_temps[path]

    def mark_accessed(self, virtual_path: str, *, accessed_at: Optional[float] = None):
        """Mark a file as accessed (update LRU)"""
        self.state_manager.update_access_time(virtual_path, accessed_at=accessed_at)

    def mark_file_dirty(self, virtual_path: str, cache_path: Path) -> Optional[int]:
        """Record a modification of a cached file: DIRTY, its size and mtime, a new generation.

        One UPDATE on the index row, so a sync in flight for the old content
        sees the generation move and does not report it as synced. The stat,
        the update and the mirror of the row into the cache file's metadata
        xattr run under the index lock, so concurrent writers record their
        stats in update order and mirror their rows in the same order.
        The path's listing attributes are forgotten first: they describe the
        member copy, which no longer matches. A listing in flight is still
        published (removing=False; see NamespaceService.forget_listed_attrs).
        Returns the new generation, or
        None when the path has no row.
        """
        with self.state_manager.metadata_lock:
            if self.forget_listed_attrs:
                self.forget_listed_attrs(virtual_path, removing=False)
            generation = self.state_manager.mark_dirty_from_cache_file(virtual_path, cache_path)
            if generation is None:
                return None
            metadata = self.state_manager.get_file_metadata(virtual_path)
            if metadata:
                self.sync_cache_metadata(virtual_path, metadata, cache_path=cache_path)
        return generation

    def _evict_if_still_clean(
        self, virtual_path: str, cache_path: Path, *, recomputable: Optional[bool] = False,
        dry_run: bool = False,
    ) -> int:
        """Reclaim cache bytes if the row is still CLEAN and describes the file.

        Recheck after selection under the index lock, which mark_file_dirty
        also takes. A stale CLEAN is marked and kept: a file is stale when
        its live size or mtime differs from the stat in its row or in its
        metadata xattr (CacheMetadataRecord.is_stale_for), whatever status
        either records. A file whose xattr cannot be read or parsed is kept
        unmarked: nothing vouches for it. One without the xattr (a mirror
        that was never possible, such as on a read-only copy) has only its
        row to go by. Hold descriptor_guard
        across the stat and the unlink or truncation, so no open descriptor
        can be missed or registered in between. Forget listing attributes
        before changing the cache inode. Lock order is the cache lock, the
        index lock, then the descriptor lock. Returns the bytes freed.
        """
        with self.state_manager.metadata_lock:
            row = self.state_manager.get_file_metadata(virtual_path)
            if row is None or row.sync_status != SyncStatus.CLEAN or row.link_target is not None:
                return 0
            if recomputable is not None and row.recomputable != recomputable:
                return 0
            truncate = recomputable is True
            if truncate and not row.recompute_cmd:
                return 0
            if recomputable is None and not any(
                not location.is_cached for location in self.state_manager.get_file_locations(virtual_path)
            ):
                # Pressure eviction reclaims only a copy a member is recorded
                # to hold, so the next open can fetch it again. A CLEAN row
                # alone is not that evidence (a status published without a
                # location, a rebuilt index), and the cache cannot recreate
                # a recompute-only placeholder on demand yet.
                return 0
            # A member that missed a rename may hold these bytes only under
            # the old name: the cached copy stays until the repair is resolved.
            if self.member_repairs.covers(virtual_path) or self.member_repairs.kept_inode(cache_path):
                return 0
            guard = self.descriptor_guard(virtual_path) if self.descriptor_guard else nullcontext(False)
            with guard as is_open:
                if is_open:
                    return 0
                st = regular_file_stat(cache_path)
                if st is None:
                    return 0
                if (st.st_dev, st.st_ino) in self._held_identities():
                    # A streaming descriptor holds the inode (a readerless
                    # fill installed before its session closed, which no
                    # FUSE descriptor pins): unlinking the name would
                    # reclaim nothing yet. close_streaming_fd notes the
                    # change, so the next allocation can take it.
                    return 0
                stale = st.st_size != row.size or st.st_mtime != row.last_modified
                if not stale:
                    try:
                        record = read_cache_metadata(cache_path)
                    except (OSError, ValueError, TypeError, KeyError):
                        return 0
                    stale = record is not None and record.is_stale_for(st.st_size, st.st_mtime)
                if not stale:
                    if st.st_nlink > 1 and not truncate:
                        return 0  # unlinking one alias cannot reclaim its blocks
                    freed = self._path_data_bytes(cache_path, st)
                    if dry_run or freed == 0:
                        return freed
                    if self.forget_listed_attrs:
                        self.forget_listed_attrs(virtual_path)
                    if truncate:
                        truncated = self._truncate_recomputable(cache_path, st)
                        if truncated is None:
                            return 0
                    else:
                        cache_path.unlink()
            if stale:
                # Even in a dry run: the mismatch is verified, and the dirty
                # transition is what makes the file sync. Nothing is reclaimed.
                self.mark_file_dirty(virtual_path, cache_path)
                return 0
            self._current_size_bytes = max(0, self._current_size_bytes - freed)
            if truncate:
                row.size = 0
                row.checksum = None
                row.last_modified = truncated.st_mtime
                self.store_file_metadata(row, cache_path=cache_path)
            else:
                self.state_manager.remove_file_location(virtual_path, "cache")
            return freed

    @staticmethod
    def _truncate_recomputable(cache_path: Path, st: os.stat_result) -> Optional[os.stat_result]:
        """Keep reconstruction xattrs on the inode selected for eviction."""
        fd = os.open(cache_path, os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
                st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns,
            ):
                return None
            os.ftruncate(fd, 0)
            return os.fstat(fd)
        finally:
            os.close(fd)

    def heal_stale_clean(
        self,
        virtual_path: str,
        cache_path: Path,
        *,
        stat_size: Optional[int] = None,
        stat_mtime: Optional[float] = None,
    ) -> bool:
        """Mark a CLEAN row DIRTY when the cache file was written after it.

        The row's size and mtime are the stat recorded by the last
        mark_file_dirty (or the last fetch). A CLEAN row whose stat differs
        from the file means a write landed and its row update was lost (a
        crash between the two), or something wrote to the cache directory
        behind the mount; in that second case a sync may publish CLEAN for
        a half-written file and the next open heals it again. One row read
        and one lstat, unless the caller passes a stat it already has. The
        read and the stat are not under the index lock: a writer racing
        them bumps the generation, and mark_file_dirty's own bump keeps the
        row at the newer write. Returns True when the file was marked.
        """
        row = self.state_manager.get_file_metadata(virtual_path)
        if row is None or row.sync_status != SyncStatus.CLEAN:
            return False
        if stat_size is None or stat_mtime is None:
            try:
                st = os.lstat(str(cache_path))
            except OSError:
                return False
            stat_size, stat_mtime = st.st_size, st.st_mtime
        if stat_size == row.size and stat_mtime == row.last_modified:
            return False
        return self.mark_file_dirty(virtual_path, cache_path) is not None

    def store_file_metadata(
        self,
        metadata: FileMetadata,
        *,
        cache_path: Optional[Path] = None,
    ) -> Optional[FileMetadata]:
        """Store a metadata snapshot and mirror the row as stored into the xattr.

        set_file_metadata keeps content columns a write recorded after the
        snapshot was read, so the xattr is written from a re-read of the row,
        never from the caller's object, and the write, re-read and mirror are
        one critical section under the index lock so no other writer's
        mirror can land between them. Returns the stored row.
        """
        with self.state_manager.metadata_lock:
            self.state_manager.set_file_metadata(metadata)
            stored = self.state_manager.get_file_metadata(metadata.virtual_path)
            if stored is not None:
                self.sync_cache_metadata(metadata.virtual_path, stored, cache_path=cache_path)
        return stored

    def sync_cache_metadata(
        self,
        virtual_path: str,
        metadata: FileMetadata,
        *,
        cache_path: Optional[Path] = None,
    ) -> bool:
        """Mirror file metadata into the cache file xattr when the cache entry exists."""
        target_path = cache_path or self.get_cache_path(virtual_path)
        if regular_file_stat(target_path) is None:
            return False

        try:
            write_cache_metadata(target_path, metadata, virtual_path=virtual_path)
            return True
        except OSError:
            return False

    def flush_file(self, virtual_path: str):
        """Flush a cached file to ensure writes are synced"""
        cache_path = self.get_cache_path(virtual_path)
        if regular_file_stat(cache_path) is not None:
            try:
                # Open and sync file
                with open(cache_path, 'rb') as f:
                    os.fsync(f.fileno())
            except OSError:
                pass

    def create_cached_file(self, virtual_path: str) -> Path:
        """
        Create a new file in cache

        Args:
            virtual_path: Virtual path of file

        Returns:
            Path to created cache file
        """
        with self._lock:
            cache_path = self.get_cache_path(virtual_path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.touch()

            # Mark as accessed
            self.mark_accessed(virtual_path)

            return cache_path

    def delete_cached_file(self, virtual_path: str):
        """Delete a file from cache"""
        with self._lock:
            cache_path = self.get_cache_path(virtual_path)
            if os.path.lexists(cache_path):
                try:
                    # A link is removed, not followed; it was never counted.
                    st = regular_file_stat(cache_path)
                    # An inode a streaming descriptor holds is charged
                    # through it; close_streaming_fd refunds it.
                    held = st is not None and (st.st_dev, st.st_ino) in self._held_identities()
                    file_size = 0
                    if st is not None and not held:
                        file_size = self._path_data_bytes(cache_path, st)
                    cache_path.unlink()
                    if st is not None and st.st_nlink > 1 and not held:
                        self._size_dirty = True
                    else:
                        self._current_size_bytes = max(0, self._current_size_bytes - file_size)
                except OSError as e:
                    self._size_dirty = True
                    print(f"Warning: Failed to delete cache file {virtual_path}: {e}")
