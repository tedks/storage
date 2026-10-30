import concurrent.futures
import contextvars
import errno
import os
import stat
import time
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, List, Optional, Set, Tuple

from fuse import FuseOSError

from .backends.base import Backend
from .checksums import ChecksumBasis, ChecksumRecord
from .models import SyncStatus
from .pathing import ResolvedPath, canonicalize_virtual_path
from .sync_tasks import DirOperation


class NamespaceService:
    """Namespace and directory-view operations extracted from StorageFS."""

    LISTING_ATTRS_MAX_AGE = 5.0

    def __init__(
        self,
        *,
        backends: Dict[str, Backend],
        cache_manager,
        state_manager,
        sync_manager,
        logger,
        resolve_path: Callable[[str], ResolvedPath],
        schedule_checksum_task: Callable[[Backend, str, Optional[ChecksumBasis]], None],
        partial_suffix: str,
        is_hidden_entry: Callable[[str], bool],
        dir_cache: Dict[str, Dict[str, Set[str]]],
        dir_cache_lock: Lock,
        readdir_snapshot: Dict[str, Tuple[float, List[str]]],
        readdir_snapshot_ttl: float,
        checksum_records: Dict[tuple, ChecksumRecord],
        checksum_lock,
    ):
        self.backends = backends
        self.cache_manager = cache_manager
        self.state_manager = state_manager
        self.sync_manager = sync_manager
        self.logger = logger
        self.resolve_path = resolve_path
        self.schedule_checksum_task = schedule_checksum_task
        self.partial_suffix = partial_suffix
        self.is_hidden_entry = is_hidden_entry
        self.dir_cache = dir_cache
        self.dir_cache_lock = dir_cache_lock
        self.readdir_snapshot = readdir_snapshot
        self.readdir_snapshot_ttl = readdir_snapshot_ttl
        self.checksum_records = checksum_records
        self.checksum_lock = checksum_lock
        # The listing state below (entry_attrs, readdir_snapshot and the
        # listings in flight) is guarded by listing_lock, not dir_cache_lock,
        # and where both are taken listing_lock nests inside dir_cache_lock:
        # forget_listed_attrs runs under the cache, index and descriptor
        # locks (eviction and invalidation) or the index lock (a write).
        # listing_lock is a leaf: it is held only for dictionary
        # updates, never across I/O or while taking another lock.
        self.listing_lock = Lock()
        # Per-directory member attributes from the last listing:
        # {dir: (monotonic time, {backend: {name: attrs}})}.
        # getattr answers from it until listing_attrs_ttl after the listing
        # was published, so `ls -l` costs no member round trip per entry.
        # The age is capped even when the readdir TTL is unbounded
        # (watchers): a change that reaches no invalidation (a dropped
        # watcher event) is visible within the cap plus the listing's own
        # duration. A cached copy that is evicted, invalidated or written
        # drops its name at once (forget_listed_attrs).
        self.entry_attrs: Dict[str, Tuple[float, Dict[str, Dict[str, dict]]]] = {}
        # Directories being listed, and a generation per such directory that
        # an invalidation of the directory or of a child bumps. A listing
        # that raced one is not published, so it cannot reinstate what the
        # invalidation removed; invalidations elsewhere do not affect it.
        # Both hold only directories with a listing in flight.
        self._listings_in_flight: Dict[str, int] = {}
        self._listing_generation: Dict[str, int] = {}
        # One single-thread executor per member lists it concurrently with
        # the others. A dedicated thread per member keeps one extra member
        # connection each (SSH clients are per thread) and never has two
        # listing threads reconnecting the same member. Created on first use,
        # after FUSE's daemonizing fork, and shut down by shutdown().
        self._listing_executors: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
        self._listing_executors_lock = Lock()
        self._listing_shut_down = False

    @property
    def listing_attrs_ttl(self) -> float:
        """How long a listing's attributes answer getattr (and keep its snapshot)."""
        return min(self.readdir_snapshot_ttl, self.LISTING_ATTRS_MAX_AGE)

    def backend_availability(self) -> Tuple[List[str], List[str]]:
        """Return backend names split by current availability."""
        available: List[str] = []
        unavailable: List[str] = []
        for name, backend in self.backends.items():
            if backend.is_available():
                available.append(name)
            else:
                unavailable.append(name)
        return available, unavailable

    def cache_state_payload(self, resolved: ResolvedPath) -> Dict[str, object]:
        """Build synthetic cache-state xattr payload for a virtual path."""
        cache_path = resolved.cache_path
        partial_path = Path(f"{cache_path}{self.partial_suffix}")
        available_backends, unavailable_backends = self.backend_availability()

        cache_present = bool(cache_path.exists() and cache_path.is_file())
        partial_present = bool(partial_path.exists() and partial_path.is_file())

        payload: Dict[str, object] = {
            "path": resolved.canonical_path,
            "cache_path": str(cache_path),
            "cache_present": cache_present,
            "partial_present": partial_present,
            "backend_available": available_backends,
            "backend_unavailable": unavailable_backends,
            "updated_at": time.time(),
        }

        try:
            payload["cache_size"] = cache_path.stat().st_size if cache_present else 0
        except OSError:
            payload["cache_size"] = 0

        try:
            payload["partial_size"] = partial_path.stat().st_size if partial_present else 0
        except OSError:
            payload["partial_size"] = 0

        return payload

    def invalidate_dir_cache_entry(self, path: str):
        """Remove a directory entry from the cached listing."""
        if not path:
            path = "/"
        if not path.startswith("/"):
            path = "/" + path
        # A conflict view's path (/foo.member) is listed under its
        # canonical name (foo).
        canonical = self.resolve_path(path).canonical_path
        parent = canonicalize_virtual_path(str(Path(canonical).parent))
        with self.dir_cache_lock:
            self.dir_cache.pop(path, None)
            with self.listing_lock:
                if path in self._listings_in_flight:
                    self._listing_generation[path] += 1
                self.readdir_snapshot.pop(path, None)
                self.entry_attrs.pop(path, None)
                if parent != path:
                    self._forget_listed_name_locked(path, canonical, parent)

    def forget_listed_attrs(self, path: str, *, removing: bool = True):
        """Stop answering getattr for a path from its parent's listing.

        For a cached copy about to be removed (eviction, watcher
        invalidation; removing=True) or just recorded dirty (removing=False):
        the listing's attributes may predate a local write, and once the
        copy is gone getattr would serve them. Drops the canonical name for
        every member: a conflict copy's invalidation also forgets the plain
        name and the other members' copies, which costs them a probe and is
        never wrong.

        When removing, a listing of the parent in flight is also kept from
        being published, so a listing read before the write was synced
        cannot put the name back. A write does not do that: it would starve
        listings of a directory with an active writer, and while the file
        is dirty its copy exists and getattr never reads the listing; a
        listing published meanwhile is cleaned up by the removal's forget.

        Takes only listing_lock, so it is safe under the cache, index and
        descriptor locks. Callers that remove the copy call it before the
        unlink, so a getattr whose lstat finds the copy gone finds the name
        gone too (one already past its lstat may still answer from either).
        """
        if not path:
            path = "/"
        if not path.startswith("/"):
            path = "/" + path
        canonical = self.resolve_path(path).canonical_path
        parent = canonicalize_virtual_path(str(Path(canonical).parent))
        if parent == path:
            return
        with self.listing_lock:
            self._forget_listed_name_locked(path, canonical, parent, bump=removing)

    def _forget_listed_name_locked(self, path: str, canonical: str, parent: str, *, bump: bool = True):
        """Drop a path's names from its parent's listing; listing_lock held.

        With bump, a listing of the parent in flight is not published.
        """
        if bump and parent in self._listings_in_flight:
            self._listing_generation[parent] += 1
        # The path's own attributes live in its parent's listing, under the
        # canonical name (a conflict view's /foo.member is listed as foo).
        listing = self.entry_attrs.get(parent)
        if listing:
            # A real name that itself ends in ".<member>" also drops its
            # stripped name from the listing. That costs extra member stats
            # on getattr until the listing refreshes: conservative, not a
            # bug.
            names_to_drop = {Path(path).name, Path(canonical).name}
            for names in listing[1].values():
                for name in names_to_drop:
                    names.pop(name, None)

    def _get_target_backends(
        self,
        backend_hint: Optional[str],
        backend_names: Optional[Set[str]] = None,
    ) -> List[Backend]:
        """Return the backend instances affected by an operation."""
        if backend_hint:
            backend = self.backends.get(backend_hint)
            return [backend] if backend else []
        if backend_names is not None:
            return [
                backend
                for name, backend in self.backends.items()
                if name in backend_names
            ]
        return list(self.backends.values())

    def _propagate_dir_operation(
        self,
        operation: DirOperation,
        path: str,
        backend_hint: Optional[str] = None,
        new_path: Optional[str] = None,
        backend_names: Optional[Set[str]] = None,
    ) -> List[str]:
        """Best-effort synchronous propagation; returns backend names that failed."""
        failures: List[str] = []
        for backend in self._get_target_backends(backend_hint, backend_names=backend_names):
            if not backend:
                continue
            try:
                if operation == DirOperation.MKDIR:
                    backend.mkdir(path)
                elif operation == DirOperation.RMDIR:
                    backend.rmdir(path)
                elif operation == DirOperation.RENAME:
                    if not new_path:
                        raise ValueError("rename requires new_path")
                    backend.rename(path, new_path)
            except Exception as exc:
                failures.append(backend.name)
                self.logger.debug(
                    "dir op %s failed for %s on %s: %s",
                    operation.value,
                    path,
                    backend.name,
                    exc,
                )
        return failures

    def _schedule_directory_checksums(
        self,
        dir_path: str,
        backend_entries: Dict[str, Set[str]],
        backend_attrs: Optional[Dict[str, Dict[str, dict]]] = None,
    ):
        """Queue checksum computations for backend collisions in the directory.

        A directory is skipped by the mode its listing reported; a member is
        asked with isdir only when its listing carried no attributes. A
        regular file's listed size, mtime and ctime (None where the member
        does not report it) go with the task as its basis,
        so a checksum already recorded from the same basis is not recomputed.
        """
        owners: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                owners.setdefault(name, set()).add(backend_name)

        for backend_name, names in backend_entries.items():
            backend = self.backends.get(backend_name)
            if not backend:
                continue
            for name in names:
                if len(owners.get(name, set())) <= 1:
                    continue
                virtual_child = str(Path(dir_path) / name)
                attrs = (backend_attrs or {}).get(backend_name, {}).get(name) or {}
                mode = attrs.get("st_mode")
                basis = None
                if mode is not None and not stat.S_ISLNK(mode):
                    if stat.S_ISDIR(mode):
                        continue
                    size, mtime = attrs.get("st_size"), attrs.get("st_mtime")
                    if stat.S_ISREG(mode) and size is not None and mtime is not None:
                        basis = (size, mtime, attrs.get("st_ctime"))
                else:
                    # No listed mode, or a symlink, which isdir follows. A
                    # symlink's listed attributes are the link's, which do not
                    # change with its target, so it gets no basis.
                    try:
                        if backend.isdir(virtual_child):
                            continue
                    except Exception:
                        pass
                self.schedule_checksum_task(backend, virtual_child, basis)

    def _apply_conflict_view(
        self,
        dir_path: str,
        base_entries: Set[str],
        backend_entries: Optional[Dict[str, Set[str]]] = None,
    ) -> Set[str]:
        """
        Convert raw directory entries into conflict-aware view.

        If multiple backends contain the same name with differing checksums,
        present them as <name>.<backend>. Otherwise present the plain name.
        backend_entries defaults to the directory's cached member names.
        """
        if backend_entries is None:
            with self.dir_cache_lock:
                backend_entries = dict(self.dir_cache.get(dir_path, {}))

        aggregated: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                aggregated.setdefault(name, set()).add(backend_name)

        visible: Set[str] = set()
        for name, backend_names in aggregated.items():
            if len(backend_names) <= 1:
                visible.add(name)
                continue

            canonical = str(Path(dir_path) / name)
            with self.checksum_lock:
                checks = [
                    self.checksum_records.get((canonical, backend))
                    for backend in backend_names
                ]
            if all(checks) and len({c.checksum for c in checks if c}) > 1:
                for backend in backend_names:
                    visible.add(f"{name}.{backend}")
            else:
                visible.add(name)

        for entry in base_entries:
            if entry in aggregated:
                continue

            suppressed = False
            for backend_name in self.backends.keys():
                suffix = f".{backend_name}"
                if entry.endswith(suffix):
                    base_candidate = entry[: -len(suffix)]
                    if base_candidate in aggregated:
                        suppressed = True
                        break
            if suppressed:
                continue

            visible.add(entry)

        return visible

    def getattr(self, path: str):
        """Get file attributes."""
        resolved = self.resolve_path(path)

        try:
            st = os.lstat(str(resolved.cache_path))
            return {
                key: getattr(st, key)
                for key in (
                    "st_atime",
                    "st_ctime",
                    "st_gid",
                    "st_mode",
                    "st_mtime",
                    "st_nlink",
                    "st_size",
                    "st_uid",
                )
            }
        except OSError as exc:
            if resolved.backend_name:
                backend_iter = [(resolved.backend_name, self.backends.get(resolved.backend_name))]
            else:
                backend_iter = list(self.backends.items())

            listed = self._listed_attrs(resolved, backend_iter)
            if listed is not None:
                def field(key, default):
                    value = listed.get(key)
                    return default if value is None else value

                return {
                    "st_atime": field("st_atime", listed["st_mtime"]),
                    "st_ctime": field("st_ctime", listed["st_mtime"]),
                    "st_gid": field("st_gid", os.getgid()),
                    "st_mode": listed["st_mode"],
                    "st_mtime": listed["st_mtime"],
                    "st_nlink": field("st_nlink", 1),
                    "st_size": listed["st_size"],
                    "st_uid": field("st_uid", os.getuid()),
                }

            for backend_name, backend in backend_iter:
                if not backend:
                    continue
                if not backend.is_available():
                    if resolved.backend_name:
                        raise FuseOSError(errno.EHOSTUNREACH)
                    continue
                try:
                    st = backend.stat(resolved.canonical_path)
                    return {
                        "st_atime": getattr(st, "st_atime", time.time()),
                        "st_ctime": getattr(
                            st,
                            "st_ctime",
                            getattr(st, "st_mtime", time.time()),
                        ),
                        "st_gid": getattr(st, "st_gid", os.getgid()),
                        "st_mode": getattr(st, "st_mode", stat.S_IFREG | 0o644),
                        "st_mtime": getattr(st, "st_mtime", time.time()),
                        "st_nlink": getattr(st, "st_nlink", 1),
                        "st_size": getattr(st, "st_size", 0),
                        "st_uid": getattr(st, "st_uid", os.getuid()),
                    }
                except Exception as backend_error:
                    self.logger.debug(
                        "getattr backend fallback failed for %s on %s: %s",
                        resolved.canonical_path,
                        backend_name,
                        backend_error,
                    )

            available_backends, _ = self.backend_availability()
            if not available_backends and self.backends:
                raise FuseOSError(errno.EHOSTUNREACH)

            raise FuseOSError(exc.errno)

    def _listed_attrs(self, resolved: ResolvedPath, backend_iter) -> Optional[dict]:
        """Attributes of a path from its parent's recent listing, if any.

        Walks the members in the order getattr probes them, skipping the
        unavailable ones as getattr does. The first available member decides:
        its listed attributes are returned when complete, and None (probe the
        members) is returned when its listing is missing, failed, carried no
        attributes, or lists the name as a symlink (listings are lstat; the
        member's stat decides whether to follow it) or without mode, size or
        mtime. Only a member that listed successfully without the name is
        passed over, as its stat would have failed.
        """
        parent = resolved.parent_dir
        name = Path(resolved.canonical_path).name
        with self.listing_lock:
            listing = self.entry_attrs.get(parent)
        if not listing or (time.monotonic() - listing[0]) > self.listing_attrs_ttl:
            return None
        for backend_name, backend in backend_iter:
            if not backend or not backend.is_available():
                continue
            member_attrs = listing[1].get(backend_name)
            if member_attrs is None:
                return None
            attrs = member_attrs.get(name)
            if attrs is None:
                continue
            mode = attrs.get("st_mode")
            if (
                mode is None
                or stat.S_ISLNK(mode)
                or attrs.get("st_size") is None
                or attrs.get("st_mtime") is None
            ):
                return None
            return attrs
        return None

    def readdir(self, path: str):
        """Read directory contents."""
        now = time.monotonic()
        with self.listing_lock:
            snapshot = self.readdir_snapshot.get(path)
            listing = self.entry_attrs.get(path)
        # A snapshot is reused only while its listing's attributes are also
        # fresh, so the `ls -l` that follows a readdir never probes per entry.
        if (
            snapshot
            and (now - snapshot[0]) <= self.readdir_snapshot_ttl
            and listing
            and (now - listing[0]) <= self.listing_attrs_ttl
        ):
            return list(snapshot[1])

        with self.listing_lock:
            self._listings_in_flight[path] = self._listings_in_flight.get(path, 0) + 1
            generation = self._listing_generation.setdefault(path, 0)
        try:
            return self._list_members(path, generation)
        finally:
            with self.listing_lock:
                remaining = self._listings_in_flight[path] - 1
                if remaining:
                    self._listings_in_flight[path] = remaining
                else:
                    del self._listings_in_flight[path]
                    del self._listing_generation[path]

    def shutdown(self):
        """Stop the member listing threads; later listings run inline.

        Called from StorageFS.destroy. With nothreads FUSE, destroy runs on
        the thread that calls readdir, so no listing is in flight then; a
        worker still blocked in member I/O (a hung member) is not
        interrupted and ends at its SFTP timeout, as the checksum
        executor's workers do.
        """
        with self._listing_executors_lock:
            self._listing_shut_down = True
            executors = list(self._listing_executors.values())
            self._listing_executors.clear()
        for executor in executors:
            executor.shutdown(wait=False, cancel_futures=True)

    def _listing_executor(self, backend_name: str) -> Optional[concurrent.futures.ThreadPoolExecutor]:
        with self._listing_executors_lock:
            if self._listing_shut_down:
                return None
            executor = self._listing_executors.get(backend_name)
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix=f"readdir-{backend_name}",
                )
                self._listing_executors[backend_name] = executor
            return executor

    def _list_member(self, backend: Backend, path: str):
        """(names, attrs by name or None) for one member, or the exception it raised."""
        try:
            attrs = backend.listdir_attrs(path)
            listed = None
            if attrs is None:
                names = set(backend.listdir(path))
            else:
                listed = {
                    name: attr for name, attr in attrs
                    if not self.is_hidden_entry(name)
                }
                names = set(listed)
            return {e for e in names if not self.is_hidden_entry(e)}, listed
        except Exception as exc:
            return exc

    def _submit_member_listings(self, path: str) -> Dict[str, Optional[concurrent.futures.Future]]:
        """Start listing every member; None means list it inline."""
        futures: Dict[str, Optional[concurrent.futures.Future]] = {}
        if len(self.backends) <= 1:
            return {name: None for name in self.backends}
        for backend_name, backend in self.backends.items():
            executor = self._listing_executor(backend_name)
            future = None
            if executor is not None:
                try:
                    # In the caller's context, so a listing for a FUSE
                    # request stays interactive on the member's pool.
                    future = executor.submit(
                        contextvars.copy_context().run, self._list_member, backend, path)
                except RuntimeError:
                    # shutdown() raced the submit.
                    future = None
            futures[backend_name] = future
        return futures

    def _list_members(self, path: str, generation: int) -> List[str]:
        """List the cache and every member, and publish the result unless an
        invalidation of the directory or a child raced it.

        The members are listed concurrently and outside dir_cache_lock; their
        results are merged under it.
        """

        futures = self._submit_member_listings(path)

        entries: Set[str] = {".", ".."}
        try:
            cache_path = self.cache_manager.get_cache_path(path)
            if cache_path.exists() and cache_path.is_dir():
                entries.update(
                    entry for entry in os.listdir(str(cache_path))
                    if not self.is_hidden_entry(entry)
                )
        except BaseException:
            # No member listing outlives the readdir that started it, so
            # nothing is in flight once FUSE moves on (shutdown() relies on it).
            concurrent.futures.wait([f for f in futures.values() if f is not None])
            raise

        outcomes = {}
        for backend_name, future in futures.items():
            if future is None:
                outcomes[backend_name] = self._list_member(self.backends[backend_name], path)
                continue
            try:
                outcomes[backend_name] = future.result()
            except Exception as exc:
                # Cancelled by shutdown().
                outcomes[backend_name] = exc

        backend_attrs: Dict[str, Dict[str, dict]] = {}
        member_entries: Dict[str, Set[str]] = {}
        with self.dir_cache_lock:
            previous = self.dir_cache.get(path, {})
            for backend_name, outcome in outcomes.items():
                if isinstance(outcome, Exception):
                    cached = previous.get(backend_name)
                    if cached is not None:
                        # Shared with the previous map: these sets are only
                        # ever replaced, never mutated in place.
                        member_entries[backend_name] = cached
                    self.logger.debug(
                        "readdir fallback for %s on backend %s due to %s",
                        path,
                        backend_name,
                        outcome,
                    )
                    continue
                names, listed = outcome
                member_entries[backend_name] = names
                if listed is not None:
                    backend_attrs[backend_name] = listed
            # A raced invalidation of the directory dropped its names, and one
            # of a child changed what this listing saw; either way the result
            # may predate the change, so it is not stored. A child-only
            # invalidation leaves the older names in place, which the next
            # listing replaces.
            # Checked and stored under listing_lock, so forget_listed_attrs
            # (which takes only listing_lock) cannot bump in between.
            with self.listing_lock:
                if self._listing_generation[path] == generation:
                    self.dir_cache[path] = member_entries
        for names in member_entries.values():
            entries.update(names)

        self._schedule_directory_checksums(path, member_entries, backend_attrs)
        visible_entries = self._apply_conflict_view(path, entries, member_entries)
        ordered_entries = sorted(visible_entries)

        with self.listing_lock:
            if self._listing_generation[path] == generation:
                listed_at = time.monotonic()
                self.readdir_snapshot[path] = (listed_at, ordered_entries)
                self.entry_attrs[path] = (listed_at, backend_attrs)
            # A snapshot is not reused once its listing has expired, so both go.
            now = time.monotonic()
            expired = [
                dir_path for dir_path, (at, _) in self.entry_attrs.items()
                if now - at > self.listing_attrs_ttl
            ]
            for dir_path in expired:
                del self.entry_attrs[dir_path]
                self.readdir_snapshot.pop(dir_path, None)

        return ordered_entries

    def mkdir(self, path: str, mode: int):
        """Create a directory."""
        resolved = self.resolve_path(path)
        cache_path = resolved.canonical_cache_path

        try:
            cache_path.mkdir(parents=True, exist_ok=False)
            os.chmod(cache_path, mode)

            failures = self._propagate_dir_operation(
                DirOperation.MKDIR,
                resolved.canonical_path,
                backend_hint=resolved.backend_name,
            )

            self.invalidate_dir_cache_entry(resolved.parent_dir)
            self.invalidate_dir_cache_entry(resolved.canonical_path)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    resolved.canonical_path,
                    DirOperation.MKDIR,
                    mode=mode,
                    backend_hint=failed_backend,
                    priority=80,
                    force=True,
                )
        except OSError as exc:
            raise FuseOSError(exc.errno)

    def rmdir(self, path: str):
        """Remove a directory."""
        resolved = self.resolve_path(path)
        cache_path = resolved.canonical_cache_path

        try:
            cache_path.rmdir()

            failures = self._propagate_dir_operation(
                DirOperation.RMDIR,
                resolved.canonical_path,
                backend_hint=resolved.backend_name,
            )

            self.invalidate_dir_cache_entry(resolved.parent_dir)
            self.invalidate_dir_cache_entry(resolved.canonical_path)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    resolved.canonical_path,
                    DirOperation.RMDIR,
                    backend_hint=failed_backend,
                    priority=80,
                    force=True,
                )
        except OSError as exc:
            raise FuseOSError(exc.errno)

    def unlink(self, path: str):
        """Remove a file."""
        resolved = self.resolve_path(path)

        try:
            resolved.cache_path.unlink()
            self.cache_manager.mark_cache_size_dirty()

            self.invalidate_dir_cache_entry(resolved.parent_dir)
            self.invalidate_dir_cache_entry(resolved.canonical_path)

            if not resolved.backend_name:
                self.state_manager.delete_file(resolved.canonical_path)
            backend = self.backends.get(resolved.backend_name) if resolved.backend_name else None
            if backend:
                try:
                    backend.delete_file(resolved.canonical_path)
                except Exception:
                    pass
        except OSError as exc:
            raise FuseOSError(exc.errno)

    def rename(self, old: str, new: str, on_local_moved: Optional[Callable[[], None]] = None):
        """Rename a file or directory.

        The local move (the cache rename, the index rewrite and the metadata
        xattrs) is one critical section under the index lock, and
        on_local_moved runs at its end, before the members are renamed:
        StorageFS re-points open descriptors there, so a write recording
        itself (under the same lock) sees either the old row at the old path
        or the moved row at the new one, never a path whose row is gone.
        """
        old_resolved = self.resolve_path(old)
        new_resolved = self.resolve_path(new)

        if old_resolved.backend_name != new_resolved.backend_name:
            raise FuseOSError(errno.EXDEV)

        old_cache = old_resolved.canonical_cache_path
        new_cache = new_resolved.canonical_cache_path

        try:
            with self.state_manager.metadata_lock:
                new_cache.parent.mkdir(parents=True, exist_ok=True)
                old_cache.rename(new_cache)

                if new_cache.is_dir():
                    rename_backend_names = set(self.backends.keys())
                else:
                    rename_backend_names = self.state_manager.get_location_backends(
                        old_resolved.canonical_path
                    )

                renamed_entries = self.state_manager.rename_path(
                    old_resolved.canonical_path,
                    new_resolved.canonical_path,
                )
                for _, updated in renamed_entries:
                    metadata = self.state_manager.get_file_metadata(updated)
                    if metadata:
                        self.cache_manager.sync_cache_metadata(updated, metadata)
                if on_local_moved is not None:
                    on_local_moved()

            failures = self._propagate_dir_operation(
                DirOperation.RENAME,
                old_resolved.canonical_path,
                backend_hint=old_resolved.backend_name,
                new_path=new_resolved.canonical_path,
                backend_names=rename_backend_names if not old_resolved.backend_name else None,
            )

            self.invalidate_dir_cache_entry(old_resolved.parent_dir)
            self.invalidate_dir_cache_entry(new_resolved.parent_dir)
            self.invalidate_dir_cache_entry(old_resolved.canonical_path)
            self.invalidate_dir_cache_entry(new_resolved.canonical_path)

            if renamed_entries:
                for _, updated in renamed_entries:
                    metadata = self.state_manager.get_file_metadata(updated)
                    if metadata and metadata.sync_status == SyncStatus.DIRTY:
                        self.sync_manager.queue_sync(updated, priority=80, force=True)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    old_resolved.canonical_path,
                    DirOperation.RENAME,
                    new_path=new_resolved.canonical_path,
                    backend_hint=failed_backend,
                    priority=90,
                    force=True,
                )
        except OSError as exc:
            raise FuseOSError(exc.errno)
