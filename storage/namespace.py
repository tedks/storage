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


# _listed_attrs: the name is absent from a fresh listing of every available
# member, so it does not exist (ruling oq-listing-negative-lookup A).
LISTED_MISSING = object()

DEFAULT_LISTING_MAX_STALE = 3600.0


class NamespaceService:
    """Namespace and directory-view operations extracted from StorageFS."""

    def __init__(
        self,
        *,
        backends: Dict[str, Backend],
        cache_manager,
        state_manager,
        sync_manager,
        logger,
        resolve_path: Callable[[str], ResolvedPath],
        schedule_checksum_task: Callable[..., None],
        partial_suffix: str,
        is_hidden_entry: Callable[[str], bool],
        dir_cache: Dict[str, Dict[str, Set[str]]],
        dir_cache_lock: Lock,
        readdir_snapshot: Dict[str, Tuple[float, List[str]]],
        readdir_snapshot_ttl: float,
        checksum_records: Dict[tuple, ChecksumRecord],
        checksum_lock,
        listing_max_stale: float = DEFAULT_LISTING_MAX_STALE,
        negative_window: Optional[float] = None,
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
        # A listing is fresh for readdir_snapshot_ttl (infinite with
        # watchers on every member) and is then served stale, while one
        # background refresh per directory runs, until listing_max_stale.
        self.readdir_snapshot_ttl = readdir_snapshot_ttl
        self.listing_max_stale = listing_max_stale
        # A listing answers ENOENT for a name it lacks only while it is
        # fresh and at most this old (None: the fresh window). Finite even
        # when watchers make the fresh window infinite: a watcher that
        # drops events (a reconnect, an overflow) must not hide a file for
        # the life of the listing.
        self.negative_window = negative_window
        # Each published snapshot's merge inputs, (cache and member names,
        # member names by member, size splits, wall time of the listing), so
        # serving it recomputes the conflict view from the checksums
        # recorded since; dropped with it. Size splits are the listing's
        # (see _size_splits): they change only with the next listing.
        self._view_inputs: Dict[
            str, Tuple[Set[str], Dict[str, Set[str]], Dict[str, Dict[str, int]], float]
        ] = {}
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
        # {dir: (monotonic time, {backend: {name: attrs}}, unknown names)}.
        # getattr answers from it while the listing is fresh or stale (see
        # listing_state), so `ls -l` and path walks cost no member round
        # trip per entry, and while it is fresh a name no available member
        # listed is ENOENT without a stat. A cached copy that is evicted,
        # invalidated or written drops its name at once and adds it to the
        # unknown names, which getattr stats again (forget_listed_attrs):
        # a dropped name is not known to be absent, and a file synced after
        # the listing was taken is on a member the listing did not see.
        # With watchers, a change that reaches no invalidation (a dropped
        # watcher event) stays unseen until the directory is invalidated.
        self.entry_attrs: Dict[str, Tuple[float, Dict[str, Dict[str, dict]], Set[str]]] = {}
        # Directories being listed, and a generation per such directory that
        # an invalidation of the directory or of a child bumps. A listing
        # that raced one is not published, so it cannot reinstate what the
        # invalidation removed; invalidations elsewhere do not affect it.
        # Both hold only directories with a listing in flight, foreground
        # or background, from the moment a background refresh is queued
        # until it ends or is cancelled.
        self._listings_in_flight: Dict[str, int] = {}
        self._listing_generation: Dict[str, int] = {}
        # Directories with a background refresh queued or running: one each.
        self._background_refreshes: Set[str] = set()
        # When the member reads behind each directory's published listing
        # began (monotonic). A background refresh and a foreground listing
        # of one directory share a generation, so a listing that began
        # before the published one is not published over it: the slower,
        # older read must not replace the newer. Dropped with the
        # directory's dir_cache entry on invalidation.
        self._listing_as_of: Dict[str, float] = {}
        # One single-thread executor per member lists it concurrently with
        # the others. A dedicated thread per member keeps one extra member
        # connection each (SSH clients are per thread) and never has two
        # listing threads reconnecting the same member. Created on first use,
        # after FUSE's daemonizing fork, and shut down by shutdown().
        self._listing_executors: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
        # Background refreshes run one at a time on their own thread, which
        # hands each member's listing to that member's executor and waits,
        # as the FUSE thread does for a foreground listing. One at a time
        # keeps at most one refresh's listings queued ahead of a foreground
        # listing on each member's executor.
        self._refresh_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._listing_executors_lock = Lock()
        self._listing_shut_down = False

    @property
    def listing_stale_limit(self) -> float:
        """Age past which a listing is neither served nor refreshed, only replaced.

        A fresh window of 0 asks for a listing at every access, so it has
        no stale phase either.
        """
        if self.readdir_snapshot_ttl <= 0:
            return 0.0
        return max(self.readdir_snapshot_ttl, self.listing_max_stale)

    def _answers_missing(self, listed_at: float, now: float) -> bool:
        window = self.readdir_snapshot_ttl if self.negative_window is None else self.negative_window
        age = now - listed_at
        return age <= self.readdir_snapshot_ttl and age <= window

    def listing_state(self, listed_at: float, now: float) -> str:
        """'fresh', 'stale' (serve it and refresh in the background) or 'expired'."""
        age = now - listed_at
        if age <= self.readdir_snapshot_ttl:
            return "fresh"
        if age <= self.listing_stale_limit:
            return "stale"
        return "expired"

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
                self._listing_as_of.pop(path, None)
                if path in self._listings_in_flight:
                    self._listing_generation[path] += 1
                self.readdir_snapshot.pop(path, None)
                self._view_inputs.pop(path, None)
                self.entry_attrs.pop(path, None)
                if parent != path:
                    self._forget_listed_name_locked(path, canonical, parent)

    def invalidate_listing_subtree(self, path: str):
        """Drop every listing at or below a directory (rename, rmdir).

        invalidate_dir_cache_entry drops one directory's listing; a renamed
        or removed directory's subdirectories have listings too, which
        would otherwise answer under the old name (or ENOENT under the new
        one) for their whole window. Listings in flight there are kept
        from being published.
        """
        canonical = self.resolve_path(path).canonical_path
        prefix = canonical.rstrip("/") + "/"

        def under(key: str) -> bool:
            return key == canonical or key.startswith(prefix)

        with self.dir_cache_lock:
            for key in [k for k in self.dir_cache if under(k)]:
                del self.dir_cache[key]
            with self.listing_lock:
                for key in self._listings_in_flight:
                    if under(key):
                        self._listing_generation[key] += 1
                for table in (self.readdir_snapshot, self._view_inputs,
                              self.entry_attrs, self._listing_as_of):
                    for key in [k for k in table if under(k)]:
                        del table[key]

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

        With bump (a member's copy changed: watcher event, release upload,
        eviction), a name the parent's published listing split by size
        drops that listing instead (snapshot, view inputs, attributes; the
        member names stay in dir_cache, which needs dir_cache_lock), so the
        next readdir lists again: the split came from sizes that just
        changed, and with watchers the listing would otherwise never
        refresh. A local write (no bump) changes no member's size, so it
        does not, and keeps from starving listings of a directory with an
        active writer.
        """
        inputs = self._view_inputs.get(parent)
        if bump and inputs is not None and (
            Path(path).name in inputs[2] or Path(canonical).name in inputs[2]
        ):
            if parent in self._listings_in_flight:
                self._listing_generation[parent] += 1
            self.readdir_snapshot.pop(parent, None)
            self._view_inputs.pop(parent, None)
            self.entry_attrs.pop(parent, None)
            self._listing_as_of.pop(parent, None)
            return
        if bump and parent in self._listings_in_flight:
            self._listing_generation[parent] += 1
        # The path's own attributes live in its parent's listing, under the
        # canonical name (a conflict view's /foo.member is listed as foo).
        listing = self.entry_attrs.get(parent)
        if listing:
            # A real name that itself ends in ".<member>" also drops its
            # stripped name from the listing. That costs extra member stats
            # on getattr until the listing refreshes: conservative, not a
            # bug. The names become unknown, not absent: getattr stats them
            # rather than answering ENOENT (the name may be on a member the
            # listing never saw it on, e.g. a file synced after it).
            names_to_drop = {Path(path).name, Path(canonical).name}
            for names in listing[1].values():
                for name in names_to_drop:
                    names.pop(name, None)
            listing[2].update(names_to_drop)

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

    @staticmethod
    def _size_splits(
        backend_entries: Dict[str, Set[str]],
        backend_attrs: Optional[Dict[str, Dict[str, dict]]],
    ) -> Dict[str, Dict[str, int]]:
        """Names on several members whose copies differ in size: {name: {member: size}}.

        Only names whose every copy was listed as a regular file with a size
        qualify; copies of different sizes cannot hold the same bytes, so
        the name is a conflict without hashing anything. A directory,
        symlink or copy listed without attributes leaves the name to the
        hashes. Equal sizes prove nothing, and neither do equal mtimes: an
        SSH upload stamps the upload time, while rsync and local copies keep
        the source's, which is where a divergence would hide.
        """
        owners: Dict[str, List[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                owners.setdefault(name, []).append(backend_name)
        splits: Dict[str, Dict[str, int]] = {}
        for name, members in owners.items():
            if len(members) <= 1:
                continue
            sizes: Dict[str, int] = {}
            for backend_name in members:
                attrs = (backend_attrs or {}).get(backend_name, {}).get(name) or {}
                mode, size = attrs.get("st_mode"), attrs.get("st_size")
                if mode is None or not stat.S_ISREG(mode) or size is None:
                    break
                sizes[backend_name] = size
            else:
                if len(set(sizes.values())) > 1:
                    splits[name] = sizes
        return splits

    def _schedule_directory_checksums(
        self,
        dir_path: str,
        backend_entries: Dict[str, Set[str]],
        backend_attrs: Optional[Dict[str, Dict[str, dict]]] = None,
        size_splits: Optional[Dict[str, Dict[str, int]]] = None,
    ):
        """Queue checksum computations for backend collisions in the directory.

        Names in size_splits (computed from the listing when not given) are
        already known to conflict and are not hashed. A directory is skipped
        by the mode its listing reported. A copy listed without a mode, or
        as a symlink, is queued with check_dir: the worker asks the member
        whether it is a directory before hashing, so the FUSE thread never
        does. A regular file's listed size, mtime and ctime (None where the
        member does not report it) go with the task as its basis, so a
        checksum already recorded from the same basis is not recomputed.
        Only queues; never calls a member.
        """
        if size_splits is None:
            size_splits = self._size_splits(backend_entries, backend_attrs)
        owners: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                owners.setdefault(name, set()).add(backend_name)

        for backend_name, names in backend_entries.items():
            backend = self.backends.get(backend_name)
            if not backend:
                continue
            for name in names:
                if len(owners.get(name, set())) <= 1 or name in size_splits:
                    continue
                virtual_child = str(Path(dir_path) / name)
                attrs = (backend_attrs or {}).get(backend_name, {}).get(name) or {}
                mode = attrs.get("st_mode")
                basis = None
                check_dir = False
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
                    check_dir = True
                self.schedule_checksum_task(backend, virtual_child, basis, check_dir=check_dir)

    def _apply_conflict_view(
        self,
        dir_path: str,
        base_entries: Set[str],
        backend_entries: Optional[Dict[str, Set[str]]] = None,
        size_splits: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> Set[str]:
        """
        Convert raw directory entries into conflict-aware view.

        If multiple backends contain the same name with differing sizes
        (size_splits) or, every copy hashed and verified, differing
        checksums, present them as <name>.<backend>. Otherwise, including
        while a hash is pending or a copy is unverified, present the plain
        name. backend_entries defaults to the directory's cached member
        names, size_splits to none.
        """
        if backend_entries is None:
            with self.dir_cache_lock:
                backend_entries = dict(self.dir_cache.get(dir_path, {}))
        size_splits = size_splits or {}

        aggregated: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                aggregated.setdefault(name, set()).add(backend_name)

        visible: Set[str] = set()
        for name, backend_names in aggregated.items():
            if len(backend_names) <= 1:
                visible.add(name)
                continue

            if name in size_splits:
                for backend in backend_names:
                    visible.add(f"{name}.{backend}")
                continue

            canonical = str(Path(dir_path) / name)
            with self.checksum_lock:
                checks = [
                    self.checksum_records.get((canonical, backend))
                    for backend in backend_names
                ]
            if (
                all(c is not None and c.checksum is not None for c in checks)
                and len({c.checksum for c in checks}) > 1
            ):
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
            if listed is LISTED_MISSING:
                raise FuseOSError(errno.ENOENT)
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

    def _listed_attrs(self, resolved: ResolvedPath, backend_iter):
        """Attributes of a path from its parent's listing, if any.

        Walks the members in the order getattr probes them, skipping the
        unavailable ones as getattr does. The first available member that
        lists the name decides: its listed attributes are returned when
        complete, and None (probe the members) when it lists the name as a
        symlink (listings are lstat; the member's stat decides whether to
        follow it) or without mode, size or mtime. None is also returned
        when there is no listing, it has expired, the name is unknown (see
        entry_attrs), or a member walked before the name was found has no
        attributes in it (its listing failed or carried none). A member
        that listed successfully without the name is passed over, as its
        stat would have failed.

        LISTED_MISSING: the listing is fresh, no older than negative_window,
        and every available member listed without the name, so getattr
        answers ENOENT without a stat. A stale listing never answers that;
        its misses are probed.

        A stale listing schedules a background refresh of the parent.
        """
        parent = resolved.parent_dir
        name = Path(resolved.canonical_path).name
        # Availability first: is_available() may do more than read a flag,
        # and the walk below runs under listing_lock, which is a leaf.
        available = [
            backend_name for backend_name, backend in backend_iter
            if backend and backend.is_available()
        ]
        now = time.monotonic()
        result = None
        with self.listing_lock:
            # Under the lock throughout: forget_listed_attrs edits these
            # dictionaries in place (pops the name, then marks it unknown),
            # and a walk between the two would read the name as absent.
            listing = self.entry_attrs.get(parent)
            state = self.listing_state(listing[0], now) if listing else "expired"
            if state != "expired" and name not in listing[2]:
                result = self._walk_listing_locked(listing, name, available)
                if result is LISTED_MISSING and not self._answers_missing(listing[0], now):
                    result = None
        if state == "stale":
            self._schedule_refresh(parent)
        return result

    @staticmethod
    def _walk_listing_locked(listing, name: str, available: List[str]):
        for backend_name in available:
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
        return LISTED_MISSING if available else None

    def readdir(self, path: str):
        """Read directory contents.

        A fresh listing is served as is. A stale one is served too, and a
        background refresh of the directory is scheduled; an expired one
        (or none) is replaced by listing every member now.
        """
        with self.listing_lock:
            snapshot = self.readdir_snapshot.get(path)
            listing = self.entry_attrs.get(path)
        # The snapshot and the attributes are published, dropped and pruned
        # together; a snapshot is served only with its attributes, so the
        # `ls -l` that follows a readdir never probes per entry.
        if snapshot and listing:
            state = self.listing_state(snapshot[0], time.monotonic())
            if state == "stale":
                self._schedule_refresh(path)
            if state != "expired":
                return self._served_snapshot(path, snapshot)

        generation = self._begin_listing(path)
        try:
            return self._list_members(path, generation)
        finally:
            self._end_listing(path)

    def _served_snapshot(self, path: str, snapshot) -> List[str]:
        """A snapshot's entries, with the conflict view recomputed.

        Checksums finish after the listing that scheduled them; a name whose
        copies turned out to differ splits into name.member at the next
        readdir rather than at the next listing. In memory only, linear in
        the directory's size per readdir.
        """
        with self.listing_lock:
            inputs = self._view_inputs.get(path)
        if inputs is None:
            return list(snapshot[1])
        entries, member_entries, size_splits, _ = inputs
        return sorted(self._apply_conflict_view(path, entries, member_entries, size_splits))

    def size_split_markers(self, canonical_path: str) -> Tuple[Dict[str, int], float]:
        """({member: listed size}, wall time of that listing) when the
        published listing of canonical_path's parent split it by size, else
        ({}, 0.0). For user.storage.checksums: such a name was never hashed."""
        parent = str(Path(canonical_path).parent)
        with self.listing_lock:
            inputs = self._view_inputs.get(parent)
        if inputs is None:
            return {}, 0.0
        sizes = inputs[2].get(Path(canonical_path).name)
        return (dict(sizes), inputs[3]) if sizes else ({}, 0.0)

    def _begin_listing(self, path: str) -> int:
        """Mark a listing of path in flight; return the generation it must match."""
        with self.listing_lock:
            self._listings_in_flight[path] = self._listings_in_flight.get(path, 0) + 1
            return self._listing_generation.setdefault(path, 0)

    def _end_listing(self, path: str, *, background: bool = False):
        with self.listing_lock:
            remaining = self._listings_in_flight[path] - 1
            if remaining:
                self._listings_in_flight[path] = remaining
            else:
                del self._listings_in_flight[path]
                del self._listing_generation[path]
            if background:
                self._background_refreshes.discard(path)

    def _schedule_refresh(self, path: str):
        """Queue one background refresh of path unless one is queued or running.

        The refresh counts as a listing in flight from now, so an
        invalidation while it waits or runs keeps its result unpublished.
        Takes listing_lock and then _listing_executors_lock, never nested.
        """
        with self.listing_lock:
            if path in self._background_refreshes:
                return
            self._background_refreshes.add(path)
        generation = self._begin_listing(path)
        executor = self._background_executor()
        future = None
        if executor is not None:
            try:
                future = executor.submit(self._run_refresh, path, generation)
            except RuntimeError:
                # shutdown() raced the submit.
                future = None
        if future is None:
            self._end_listing(path, background=True)
            return
        # A refresh cancelled by shutdown() never runs, so its bookkeeping
        # is released here instead.
        future.add_done_callback(
            lambda f: self._end_listing(path, background=True) if f.cancelled() else None
        )

    def _run_refresh(self, path: str, generation: int):
        try:
            if not self._listing_shut_down:
                self._list_members(path, generation, background=True)
        except Exception as exc:
            # The stale listing stays in place; the next access past it
            # schedules another refresh, or lists in the foreground once
            # it expires.
            self.logger.debug("background refresh of %s failed: %s", path, exc)
        finally:
            self._end_listing(path, background=True)

    def _background_executor(self) -> Optional[concurrent.futures.ThreadPoolExecutor]:
        with self._listing_executors_lock:
            if self._listing_shut_down:
                return None
            if self._refresh_executor is None:
                self._refresh_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="readdir-refresh",
                )
            return self._refresh_executor

    def shutdown(self):
        """Stop the listing threads; later foreground listings run inline.

        Called from StorageFS.destroy. libfuse runs destroy only after its
        loop has returned (the multithreaded loop joins every worker first),
        so no foreground listing is in flight then. A background refresh
        may be: queued ones are cancelled (their bookkeeping released by
        the done callback). One already running finds its queued member
        listings cancelled and lists no member inline, so it falls back to
        the names it had; it may still publish that into the in-memory
        maps, which nothing reads after destroy. A worker
        still blocked in member I/O (a hung member) is not interrupted and
        ends at its SFTP timeout, as the checksum executor's workers do.
        """
        with self._listing_executors_lock:
            self._listing_shut_down = True
            executors = list(self._listing_executors.values())
            self._listing_executors.clear()
            if self._refresh_executor is not None:
                executors.append(self._refresh_executor)
                self._refresh_executor = None
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

    def _list_members(self, path: str, generation: int, *, background: bool = False) -> List[str]:
        """List the cache and every member, and publish the result unless an
        invalidation of the directory or a child raced it, or a listing that
        began later was published first.

        The members are listed concurrently and outside dir_cache_lock; their
        results are merged under it. The listing's time is when its reads
        began, so its freshness never outlasts what it read. A background
        refresh does no member I/O after shutdown(): a member it would list
        inline counts as failed.
        """

        started_at = time.monotonic()
        listed_wall = time.time()
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
                if background and self._listing_shut_down:
                    outcomes[backend_name] = RuntimeError("listing shut down")
                    continue
                outcomes[backend_name] = self._list_member(self.backends[backend_name], path)
                continue
            try:
                outcomes[backend_name] = future.result()
            except Exception as exc:
                # Cancelled by shutdown().
                outcomes[backend_name] = exc

        backend_attrs: Dict[str, Dict[str, dict]] = {}
        member_entries: Dict[str, Set[str]] = {}
        failed: Set[str] = set()
        with self.dir_cache_lock:
            previous = self.dir_cache.get(path, {})
            for backend_name, outcome in outcomes.items():
                if isinstance(outcome, Exception):
                    failed.add(backend_name)
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
        for names in member_entries.values():
            entries.update(names)

        # A member whose listing failed keeps its previous names (above) and,
        # for the size test only, the attributes of the published listing,
        # so one failed refresh does not merge a split name; it is not hashed
        # this round. A second failure in a row, a failure after a change to
        # the name dropped the listing, or one after a local write forgot
        # the name's attributes has none for it and shows the plain name:
        # its sizes are not known.
        split_attrs = backend_attrs
        if failed:
            split_attrs = dict(backend_attrs)
            with self.listing_lock:
                # Copied under the lock: forget_listed_attrs edits them in place.
                published = self.entry_attrs.get(path)
                for backend_name in failed:
                    if published is not None and backend_name in published[1]:
                        split_attrs[backend_name] = dict(published[1][backend_name])
        size_splits = self._size_splits(member_entries, split_attrs)
        self._schedule_directory_checksums(
            path,
            {name: names for name, names in member_entries.items() if name not in failed},
            backend_attrs,
            size_splits,
        )
        visible_entries = self._apply_conflict_view(path, entries, member_entries, size_splits)
        ordered_entries = sorted(visible_entries)

        # Published in one step: member names, time, snapshot, view inputs
        # and attributes, under dir_cache_lock and then listing_lock (their
        # order), so no other listing ever sees half of one published.
        # Not published when an invalidation of the directory or a child
        # raced it (the result may predate the change; a child-only
        # invalidation leaves the older names, which the next listing
        # replaces), or when a listing that began later was published
        # first. Checked and stored under listing_lock, so
        # forget_listed_attrs (which takes only listing_lock) cannot bump
        # in between.
        with self.dir_cache_lock:
            with self.listing_lock:
                if (
                    self._listing_generation[path] == generation
                    and started_at >= self._listing_as_of.get(path, float("-inf"))
                ):
                    self.dir_cache[path] = member_entries
                    self._listing_as_of[path] = started_at
                    self.readdir_snapshot[path] = (started_at, ordered_entries)
                    self._view_inputs[path] = (entries, member_entries, size_splits, listed_wall)
                    self.entry_attrs[path] = (started_at, backend_attrs, set())
                # A listing that began later was published while this one
                # ran: answer with it, so a readdir never shows names older
                # than what getattr now answers from.
                newer = self._listing_as_of.get(path, float("-inf")) > started_at
                newer_snapshot = self.readdir_snapshot.get(path) if newer else None
                # A snapshot is not served once its listing has expired, so
                # both go; the time goes only with the listing it stamps (a
                # guard: they are always set and dropped together). Linear
                # in the listed directories, under both locks, once per
                # listing.
                now = time.monotonic()
                expired = [
                    dir_path for dir_path, (at, _, _) in self.entry_attrs.items()
                    if self.listing_state(at, now) == "expired"
                ]
                for dir_path in expired:
                    at = self.entry_attrs.pop(dir_path)[0]
                    self.readdir_snapshot.pop(dir_path, None)
                    self._view_inputs.pop(dir_path, None)
                    if self._listing_as_of.get(dir_path) == at:
                        del self._listing_as_of[dir_path]

        if newer_snapshot is not None:
            return self._served_snapshot(path, newer_snapshot)
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
            self.invalidate_listing_subtree(resolved.canonical_path)

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
            self.invalidate_listing_subtree(old_resolved.canonical_path)
            self.invalidate_listing_subtree(new_resolved.canonical_path)

            if renamed_entries:
                for _, updated in renamed_entries:
                    metadata = self.state_manager.get_file_metadata(updated)
                    # A SYNCING row can only be one whose attempt ended
                    # without settling it (the rename waits for uploads in
                    # flight); nothing else would queue it again.
                    if metadata and metadata.sync_status in (SyncStatus.DIRTY, SyncStatus.SYNCING):
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
