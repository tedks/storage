"""
Local cache manager with LRU eviction

Manages the local cache of files from remote backing filesystems.
Files are cached on first access and evicted using LRU when space is needed.
"""

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
from typing import Callable, ContextManager, Optional, List, Set

from .backends.base import Backend, bulk_request
from .backends.ssh_pool import BulkCapacityCancelled, BulkCapacityTimeout
from .cache_metadata import (
    PARTIAL_SUFFIX, PREFIX_XATTR, copy_member_attributes, regular_file_stat, write_cache_metadata,
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
        # Bytes promised to fetches in flight (cache_file): counted against
        # max_size_bytes so concurrent fetches cannot each see the same free
        # space. Under _lock.
        self._reserved_bytes = 0
        # Bumped by every rescan; a fetch that sees it move knows the scan
        # may have counted its temp file (see _replace_into_cache).
        self._scan_count = 0

        # Ensure cache root exists
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._current_size_bytes = self._scan_total_cache_size()
        self._size_dirty = False

    def _scan_total_cache_size(self) -> int:
        """Calculate total size of cache in bytes by scanning the filesystem.

        Counts regular files only, by lstat: a symlink in the cache tree is
        not a cached copy, and following it would charge its target's size
        (anything on the host) against a cache that can never evict it.
        """
        total = 0
        for root, dirs, files in os.walk(self.cache_root):
            for name in files:
                st = regular_file_stat(Path(root) / name)
                if st is not None:
                    total += st.st_size
        return total

    def _ensure_size_current_locked(self):
        """Refresh tracked size if it has been marked dirty by direct writes."""
        if self._size_dirty:
            self._current_size_bytes = self._scan_total_cache_size()
            self._size_dirty = False
            self._scan_count += 1

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
        """Calculate total size of cache in bytes"""
        with self._lock:
            self._ensure_size_current_locked()
            return self._current_size_bytes

    def mark_cache_size_dirty(self):
        """Mark cache size accounting as stale after direct external file mutations."""
        with self._lock:
            self._size_dirty = True

    def get_free_space_needed(self, required_bytes: int) -> int:
        """
        Calculate how much space needs to be freed

        Returns:
            Bytes to free (0 if enough space available)
        """
        with self._lock:
            self._ensure_size_current_locked()
            available = self.max_size_bytes - self._current_size_bytes - self._reserved_bytes

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

    def _evict_by_access(self, bytes_needed: int, *, recomputable: bool) -> bool:
        if bytes_needed <= 0:
            return True
        with self._lock:
            freed = 0
            for virtual_path in self.state_manager.iter_lru_paths(recomputable=recomputable):
                cache_path = self.get_cache_path(virtual_path)
                with self._path_guard(virtual_path) as free:
                    if not free:
                        continue
                    if regular_file_stat(cache_path) is None:
                        self._note_cache_absent(virtual_path, cache_path)
                        continue
                    try:
                        freed += self._evict_if_still_clean(
                            virtual_path, cache_path, recomputable=recomputable,
                        )
                    except OSError as e:
                        print(f"Warning: Failed to evict {virtual_path}: {e}")
                if freed >= bytes_needed:
                    return True
            return False

    def ensure_space(self, bytes_needed: int) -> bool:
        """
        Ensure enough cache space is available

        Attempts eviction in this order:
        1. LRU non-recomputable files
        2. LRU recomputable files

        Args:
            bytes_needed: Amount of space needed in bytes

        Returns:
            True if space was made available, False if insufficient space
        """
        space_needed = self.get_free_space_needed(bytes_needed)

        if space_needed <= 0:
            return True  # Already have enough space

        # Try evicting LRU files first
        if self.evict_lru_files(space_needed):
            return True

        # The first tier may have freed some of the space.
        return self.evict_recomputable_files(self.get_free_space_needed(bytes_needed))

    @contextmanager
    def admit_streaming_partial(self, file_size: int):
        """Charge the new sparse file's apparent size before another open can allocate."""
        with self._lock:
            admitted = self.ensure_space(file_size)
            try:
                yield admitted
            finally:
                if admitted:
                    self._size_dirty = True

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

        with self._lock:
            if not self.ensure_space(file_size):
                print(f"Warning: Insufficient cache space for {virtual_path}")
                return CacheFetch.FAILED
            self._reserved_bytes += file_size
        try:
            installed = self.fetch_into_cache(
                backend, source_virtual_path, self.get_cache_path(virtual_path), member_stat,
            )
            return CacheFetch.INSTALLED if installed else CacheFetch.ALREADY_CACHED
        except (BulkCapacityCancelled, BulkCapacityTimeout):
            raise
        except Exception as e:
            print(f"Error caching {virtual_path} from {backend.name}: {e}")
            return CacheFetch.FAILED
        finally:
            with self._lock:
                self._reserved_bytes -= file_size

    def fetch_into_cache(self, backend: Backend, source_virtual_path: str, cache_path: Path,
                         member_stat=None) -> bool:
        """Copy a member file to cache_path through a temp file beside it.

        The copy runs without the cache lock into a fresh name that ends in
        PARTIAL_SUFFIX, so listings hide it and the startup sweep removes
        it after a crash. It is then hard-linked to cache_path, which fails
        if anything is there already: a concurrent fetch that installed
        first, a create, or a copy a writer has since changed is never
        replaced, and the existing inode keeps its bytes and xattrs. The
        link and the size accounting run under the lock. The cache
        filesystem must support hard links (it must support xattrs anyway).
        No space is reserved here (cache_file does that).

        Returns True when this copy was installed, False when a file was
        already there and this copy was discarded. Raises what the copy or
        the link raised (FileExistsError when something other than a
        regular file is there), with the temp file removed.
        """
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.parent / f".{secrets.token_hex(8)}{PARTIAL_SUFFIX}"
        with self._lock:
            scans = self._scan_count
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
                size = os.fstat(fd).st_size
            finally:
                os.close(fd)
            with self._lock:
                try:
                    os.link(temp_path, cache_path)
                except FileExistsError:
                    if not (cache_path.is_file() and not cache_path.is_symlink()):
                        raise
                    return False
                self._current_size_bytes += size
            return True
        except BaseException:
            # Whatever the copy wrote may have been counted by a rescan,
            # and a failure after the link leaves the sum unknown.
            with self._lock:
                self._size_dirty = True
            raise
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                with self._lock:
                    self._size_dirty = True
            with self._lock:
                if self._scan_count != scans:
                    # A rescan during the copy may have counted the temp
                    # file as well as the linked copy.
                    self._size_dirty = True

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
        self, virtual_path: str, cache_path: Path, *, recomputable: bool = False,
    ) -> int:
        """Reclaim cache bytes if the row is still CLEAN and describes the file.

        Recheck after selection under the index lock, which mark_file_dirty
        also takes. A stale CLEAN is marked and kept. Hold descriptor_guard
        across the stat and the unlink or truncation, so no open descriptor
        can be missed or registered in between. Forget listing attributes
        before changing the cache inode. Lock order is the cache lock, the
        index lock, then the descriptor lock. Returns the bytes freed.
        """
        with self.state_manager.metadata_lock:
            row = self.state_manager.get_file_metadata(virtual_path)
            if (row is None or row.sync_status != SyncStatus.CLEAN or row.link_target is not None
                    or row.recomputable != recomputable or (recomputable and not row.recompute_cmd)):
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
                stale = st.st_size != row.size or st.st_mtime != row.last_modified
                if not stale:
                    if st.st_size == 0:
                        return 0
                    if self.forget_listed_attrs:
                        self.forget_listed_attrs(virtual_path)
                    if recomputable:
                        truncated = self._truncate_recomputable(cache_path, st)
                        if truncated is None:
                            return 0
                    else:
                        cache_path.unlink()
            if stale:
                self.mark_file_dirty(virtual_path, cache_path)
                return 0
            self._current_size_bytes = max(0, self._current_size_bytes - st.st_size)
            if recomputable:
                row.size = 0
                row.checksum = None
                row.last_modified = truncated.st_mtime
                self.store_file_metadata(row, cache_path=cache_path)
            else:
                self.state_manager.remove_file_location(virtual_path, "cache")
                self._note_cache_absent(virtual_path, cache_path)
            return st.st_size

    def _note_cache_absent(self, virtual_path: str, cache_path: Path):
        """Drop a path with no cache file from later eviction scans until its row is written.

        The mark goes in before the second stat: bytes placed after that
        stat are followed by a row write, whose trigger drops the mark,
        and bytes placed before it are seen here. No path writes the row
        before it places the bytes, so a mark can only hide a file the
        cache does not hold.
        """
        self.state_manager.mark_cache_absent(virtual_path)
        if regular_file_stat(cache_path) is not None:
            self.state_manager.clear_cache_absent(virtual_path)

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
        and one lstat, unless the caller passes a stat it already has; the
        common case, no difference, takes no lock.

        A difference is confirmed under the index lock with a fresh row
        and a fresh lstat before anything is marked. Without that, a row
        read before a recomputable eviction and a stat taken after it would
        look like a lost write: the eviction truncates the file and then
        stores size 0, and mark_file_dirty would record the truncated
        placeholder as DIRTY and sync an empty file over the members. The
        eviction truncates and stores in one section under the index lock,
        so the recheck sees both or neither. Returns True when the file was
        marked.
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
        with self.state_manager.metadata_lock:
            row = self.state_manager.get_file_metadata(virtual_path)
            if row is None or row.sync_status != SyncStatus.CLEAN:
                return False
            try:
                st = os.lstat(str(cache_path))
            except OSError:
                return False
            if st.st_size == row.size and st.st_mtime == row.last_modified:
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
            existing_size = cache_path.stat().st_size if cache_path.exists() else 0
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.touch()
            new_size = cache_path.stat().st_size if cache_path.exists() else 0
            self._current_size_bytes += (new_size - existing_size)

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
                    file_size = st.st_size if st is not None else 0
                    cache_path.unlink()
                    self._current_size_bytes = max(0, self._current_size_bytes - file_size)
                except OSError as e:
                    self._size_dirty = True
                    print(f"Warning: Failed to delete cache file {virtual_path}: {e}")
