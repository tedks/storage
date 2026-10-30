"""
Locks for running the FUSE loop with more than one thread.

Two outer locks order the handlers that change what a path names
(docs/browsing_latency_plan.md D6). Lock order, outer to inner:

1. NamespaceLock: shared by every handler that takes a path lock, and by
   the remote watcher's invalidation; exclusive only for directory rename
   and rmdir, which change what many paths name at once.
2. PathLocks: one reentrant lock per canonical path, held across open,
   create, unlink, truncate, utimens, mkdir, symlink, a file rename (both
   paths, in sorted order) and release (including its upload of a member
   copy). getattr, readdir, readlink, read, write, fsync, chmod, chown,
   the xattr handlers and statfs take neither lock.
3. CacheManager._lock, never held across member I/O.
4. The index lock (StateManager.metadata_lock), then StorageFS._fd_lock.
5. Leaves, unchanged: _member_checks_lock, dir_cache_lock then
   listing_lock, _checksum_lock, prefetch_sessions_lock then session.lock,
   _access_lock then _db_lock.

Eviction runs under the cache lock, against the order, so it only
try-acquires (NamespaceLock.try_shared, PathLocks.try_hold) and skips a
busy path. Nothing else may take a level-1 or level-2 lock while holding a
lock from levels 3 to 5.

Outside the order: the sync workers and the streaming prefetch worker take
neither outer lock. A sync upload can still race a rename of its path, as
it could before (ditz threading-sync-vs-rename).
"""

import threading
from contextlib import contextmanager
from typing import Dict, Iterator, List


class NamespaceLock:
    """A readers-writer lock; both sides reentrant for the holding thread.

    A waiting writer stops new readers (so a directory rename is not
    starved by a stream of opens), except a thread that already holds the
    lock shared: it re-enters at once, since making it wait for the writer
    that is waiting for it would deadlock. The writer may also take the lock
    shared. Not upgradable: a thread holding it shared must not ask for it
    exclusive (that raises RuntimeError rather than deadlocking).
    """

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = None
        self._writer_depth = 0
        self._writers_waiting = 0
        self._local = threading.local()

    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    def _acquire_shared(self, blocking: bool) -> bool:
        me = threading.get_ident()
        with self._cond:
            if self._depth() == 0 and self._writer != me:
                if not blocking and (self._writer is not None or self._writers_waiting):
                    return False
                while self._writer is not None or self._writers_waiting:
                    self._cond.wait()
            self._readers += 1
        self._local.depth = self._depth() + 1
        return True

    def _release_shared(self):
        self._local.depth = self._depth() - 1
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    @contextmanager
    def shared(self) -> Iterator[None]:
        self._acquire_shared(blocking=True)
        try:
            yield
        finally:
            self._release_shared()

    @contextmanager
    def try_shared(self) -> Iterator[bool]:
        """Yield whether the lock was taken shared without waiting."""
        acquired = self._acquire_shared(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                self._release_shared()

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        me = threading.get_ident()
        with self._cond:
            if self._writer == me:
                self._writer_depth += 1
            else:
                if self._depth():
                    raise RuntimeError("NamespaceLock: shared holder asked for exclusive")
                self._writers_waiting += 1
                try:
                    while self._writer is not None or self._readers:
                        self._cond.wait()
                finally:
                    self._writers_waiting -= 1
                self._writer = me
                self._writer_depth = 1
        try:
            yield
        finally:
            with self._cond:
                self._writer_depth -= 1
                if self._writer_depth == 0:
                    self._writer = None
                    self._cond.notify_all()


class _PathEntry:
    __slots__ = ("lock", "refs")

    def __init__(self):
        self.lock = threading.RLock()
        # Holders plus waiters; the entry leaves the table at zero.
        self.refs = 0


class PathLocks:
    """A table of reentrant locks keyed by canonical path.

    Entries exist only while held or waited for (refcounted), so the table
    is as large as the number of busy paths. No striping: two paths never
    share a lock, so an unrelated path is never blocked and the sorted
    two-path order cannot deadlock on a collision.
    """

    def __init__(self):
        self._mutex = threading.Lock()
        self._entries: Dict[str, _PathEntry] = {}

    def _ref(self, path: str) -> _PathEntry:
        with self._mutex:
            entry = self._entries.get(path)
            if entry is None:
                entry = self._entries[path] = _PathEntry()
            entry.refs += 1
            return entry

    def _unref(self, path: str, entry: _PathEntry):
        with self._mutex:
            entry.refs -= 1
            if entry.refs == 0:
                del self._entries[path]

    @contextmanager
    def hold(self, *paths: str) -> Iterator[None]:
        """Hold the locks of every distinct path, acquired in sorted order."""
        held: List[tuple] = []
        try:
            for path in sorted(set(paths)):
                entry = self._ref(path)
                try:
                    entry.lock.acquire()
                except BaseException:
                    self._unref(path, entry)
                    raise
                held.append((path, entry))
            yield
        finally:
            for path, entry in reversed(held):
                entry.lock.release()
                self._unref(path, entry)

    @contextmanager
    def try_hold(self, path: str) -> Iterator[bool]:
        """Yield whether the path's lock was taken without waiting."""
        entry = self._ref(path)
        acquired = entry.lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                entry.lock.release()
            self._unref(path, entry)

    def __len__(self) -> int:
        with self._mutex:
            return len(self._entries)
