import errno
import os
import stat
import time
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, List, Optional, Set, Tuple

from fuse import FuseOSError

from .backends.base import Backend
from .checksums import ChecksumRecord
from .pathing import ResolvedPath
from .sync_tasks import DirOperation
from .state import SyncStatus


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
        schedule_checksum_task: Callable[[Backend, str], None],
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
        with self.dir_cache_lock:
            self.dir_cache.pop(path, None)
            self.readdir_snapshot.pop(path, None)

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
    ):
        """Queue checksum computations for backend collisions in the directory."""
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
                try:
                    if backend.isdir(virtual_child):
                        continue
                except Exception:
                    pass
                self.schedule_checksum_task(backend, virtual_child)

    def _apply_conflict_view(self, dir_path: str, base_entries: Set[str]) -> Set[str]:
        """
        Convert raw directory entries into conflict-aware view.

        If multiple backends contain the same name with differing checksums,
        present them as <name>.<backend>. Otherwise present the plain name.
        """
        with self.dir_cache_lock:
            backend_entries = self.dir_cache.get(dir_path, {})

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

    def readdir(self, path: str):
        """Read directory contents."""
        now = time.monotonic()
        with self.dir_cache_lock:
            snapshot = self.readdir_snapshot.get(path)
        if snapshot and (now - snapshot[0]) <= self.readdir_snapshot_ttl:
            return list(snapshot[1])

        cache_path = self.cache_manager.get_cache_path(path)
        entries: Set[str] = {".", ".."}

        if cache_path.exists() and cache_path.is_dir():
            entries.update(
                entry for entry in os.listdir(str(cache_path))
                if not self.is_hidden_entry(entry)
            )

        with self.dir_cache_lock:
            dir_cache = self.dir_cache.setdefault(path, {})

            for backend_name, backend in self.backends.items():
                try:
                    attrs = backend.listdir_attrs(path)
                    if attrs is None:
                        backend_entries = set(backend.listdir(path))
                    else:
                        backend_entries = {name for name, _ in attrs}
                    backend_entries = {
                        e for e in backend_entries if not self.is_hidden_entry(e)
                    }
                    entries.update(backend_entries)
                    dir_cache[backend_name] = backend_entries
                except Exception as exc:
                    cached = dir_cache.get(backend_name, set())
                    if cached:
                        entries.update(cached)
                    self.logger.debug(
                        "readdir fallback for %s on backend %s due to %s",
                        path,
                        backend_name,
                        exc,
                    )

        self._schedule_directory_checksums(path, dir_cache)
        visible_entries = self._apply_conflict_view(path, entries)
        ordered_entries = sorted(visible_entries)

        with self.dir_cache_lock:
            self.readdir_snapshot[path] = (time.monotonic(), ordered_entries)

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

    def rename(self, old: str, new: str):
        """Rename a file or directory."""
        old_resolved = self.resolve_path(old)
        new_resolved = self.resolve_path(new)

        if old_resolved.backend_name != new_resolved.backend_name:
            raise FuseOSError(errno.EXDEV)

        old_cache = old_resolved.canonical_cache_path
        new_cache = new_resolved.canonical_cache_path

        try:
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
