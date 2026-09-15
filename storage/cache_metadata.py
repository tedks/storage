"""Cache-file metadata xattrs and cache/index audit helpers."""

import errno
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional

from .file_config import get_inherited_config
from .models import FileLocation, FileMetadata, SyncStatus


XATTR_CACHE_METADATA_NAME = "user.storage.metadata"
CACHE_METADATA_VERSION = 1
_INTERNAL_CACHE_NAMES = frozenset({
    ".storage.rsync-tmp",
    ".storage.control.sock",
    ".storage.db",
    ".storage.db-shm",
    ".storage.db-wal",
})
_INTERNAL_CACHE_PREFIXES = (
    ".storage.mount.",
    ".storage.db-",
)
_INTERNAL_CACHE_SUFFIXES = (
    ".storage.partial",
)


def _is_missing_xattr_error(exc: OSError) -> bool:
    missing_codes = {errno.ENODATA, errno.ENOENT}
    enoattr = getattr(errno, "ENOATTR", None)
    if enoattr is not None:
        missing_codes.add(enoattr)
    return exc.errno in missing_codes


def is_internal_cache_artifact(name: str) -> bool:
    """Return True when the cache entry name belongs to Storage internals."""
    if name in _INTERNAL_CACHE_NAMES:
        return True
    if any(name.startswith(prefix) for prefix in _INTERNAL_CACHE_PREFIXES):
        return True
    if any(name.endswith(suffix) for suffix in _INTERNAL_CACHE_SUFFIXES):
        return True
    return False


def cache_path_to_virtual_path(cache_root: Path, cache_path: Path) -> str:
    """Convert a cache file path back into its virtual path."""
    relative = cache_path.relative_to(cache_root)
    virtual_path = "/" + relative.as_posix()
    return virtual_path if virtual_path != "//" else "/"


@dataclass
class CacheMetadataRecord:
    """Persisted metadata snapshot for a cached file."""

    virtual_path: str
    size: int
    last_modified: float
    checksum: Optional[str]
    sync_status: SyncStatus
    replica_count: int
    recomputable: bool
    recompute_cmd: Optional[str]
    encrypted: bool
    encryption_key_id: Optional[str]
    version: int = CACHE_METADATA_VERSION

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable payload."""
        return {
            "version": self.version,
            "virtual_path": self.virtual_path,
            "size": self.size,
            "last_modified": self.last_modified,
            "checksum": self.checksum,
            "sync_status": self.sync_status.value,
            "replica_count": self.replica_count,
            "recomputable": self.recomputable,
            "recompute_cmd": self.recompute_cmd,
            "encrypted": self.encrypted,
            "encryption_key_id": self.encryption_key_id,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "CacheMetadataRecord":
        """Parse a cache metadata payload."""
        data = dict(payload)
        if data.get("version") != CACHE_METADATA_VERSION:
            raise ValueError(f"unsupported cache metadata version: {data.get('version')!r}")
        data["sync_status"] = SyncStatus(data["sync_status"])
        return cls(**data)

    @classmethod
    def from_file_metadata(
        cls,
        metadata: FileMetadata,
        *,
        virtual_path: Optional[str] = None,
        stat_size: Optional[int] = None,
        stat_mtime: Optional[float] = None,
    ) -> "CacheMetadataRecord":
        """Snapshot cache metadata from a FileMetadata record and on-disk stat."""
        return cls(
            virtual_path=virtual_path or metadata.virtual_path,
            size=metadata.size if stat_size is None else stat_size,
            last_modified=metadata.last_modified if stat_mtime is None else stat_mtime,
            checksum=metadata.checksum,
            sync_status=metadata.sync_status,
            replica_count=metadata.replica_count,
            recomputable=metadata.recomputable,
            recompute_cmd=metadata.recompute_cmd,
            encrypted=metadata.encrypted,
            encryption_key_id=metadata.encryption_key_id,
        )

    def to_file_metadata(
        self,
        *,
        virtual_path: str,
        stat_size: int,
        stat_mtime: float,
        last_access: float,
    ) -> FileMetadata:
        """Build runtime metadata using the live cache path and file stat."""
        return FileMetadata(
            virtual_path=virtual_path,
            size=stat_size,
            checksum=self.checksum,
            last_access=last_access,
            last_modified=stat_mtime,
            sync_status=self.sync_status,
            replica_count=self.replica_count,
            recomputable=self.recomputable,
            recompute_cmd=self.recompute_cmd,
            encrypted=self.encrypted,
            encryption_key_id=self.encryption_key_id,
        )


@dataclass
class CacheScanEntry:
    """Scanned cache entry plus any metadata/xattr issues found."""

    cache_path: Path
    virtual_path: str
    stat_size: int
    stat_mtime: float
    stat_atime: float
    metadata: Optional[CacheMetadataRecord]
    issues: List[str] = field(default_factory=list)

    def build_file_metadata(self) -> FileMetadata:
        """Build authoritative metadata for index rebuild/verification."""
        if self.metadata is None:
            config = get_inherited_config(self.cache_path)
            return FileMetadata(
                virtual_path=self.virtual_path,
                size=self.stat_size,
                checksum=None,
                last_access=self.stat_atime,
                last_modified=self.stat_mtime,
                sync_status=SyncStatus.DIRTY,
                replica_count=config.replica_count,
                recomputable=config.recomputable,
                recompute_cmd=config.recompute_cmd,
                encrypted=config.encrypted,
                encryption_key_id=None,
            )

        return self.metadata.to_file_metadata(
            virtual_path=self.virtual_path,
            stat_size=self.stat_size,
            stat_mtime=self.stat_mtime,
            last_access=self.stat_atime,
        )

    def build_cache_location(self) -> FileLocation:
        """Build the cache location row for this cache entry."""
        metadata = self.build_file_metadata()
        return FileLocation(
            virtual_path=metadata.virtual_path,
            backend_name="cache",
            checksum=metadata.checksum or "",
            last_synced=self.stat_mtime,
            is_cached=True,
        )


def read_cache_metadata(cache_path: Path) -> Optional[CacheMetadataRecord]:
    """Read the cache metadata xattr, returning None when it is absent."""
    try:
        raw = os.getxattr(str(cache_path), XATTR_CACHE_METADATA_NAME)
    except OSError as exc:
        if _is_missing_xattr_error(exc):
            return None
        raise

    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid cache metadata payload for {cache_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"invalid cache metadata payload type for {cache_path}: {type(payload).__name__}")
    return CacheMetadataRecord.from_dict(payload)


def write_cache_metadata(
    cache_path: Path,
    metadata: FileMetadata,
    *,
    virtual_path: Optional[str] = None,
) -> CacheMetadataRecord:
    """Write the cache metadata xattr for a cache file."""
    st = os.lstat(str(cache_path))
    record = CacheMetadataRecord.from_file_metadata(
        metadata,
        virtual_path=virtual_path,
        stat_size=st.st_size,
        stat_mtime=st.st_mtime,
    )
    os.setxattr(
        str(cache_path),
        XATTR_CACHE_METADATA_NAME,
        json.dumps(record.to_dict(), sort_keys=True).encode("utf-8"),
    )
    return record


def _is_listable_dir(path: Path) -> bool:
    """Whether os.walk could list this directory with the current credentials."""
    if os.access in os.supports_effective_ids:
        # os.walk, lstat and isfile run with the effective uid/gid; os.access
        # checks the real ids unless told otherwise.
        return os.access(path, os.R_OK | os.X_OK, effective_ids=True)
    return os.access(path, os.R_OK | os.X_OK)


def cache_file_would_be_scanned(cache_root: Path, virtual_path: str) -> bool:
    """Return True when scan_cache_entries(cache_root) would yield virtual_path.

    Answers for one path without walking the cache, and must stay in step with
    the scanner below: the path is canonical ("/a/b", with no empty, "." or ".."
    components), no component is an internal cache artifact (the scanner prunes
    those), the root and every parent are real, listable directories (os.walk
    neither follows directory symlinks nor lists unreadable directories), and
    the leaf lstats and is a regular file once a final symlink is followed.
    cache_root must already be resolved.

    Known limit: on a case-folding or Unicode-normalizing filesystem (APFS, ext4
    casefold directories) a differently spelled path such as "/A.bin" lstats to
    the real "a.bin" and counts as scanned, although the scanner only ever
    yields the on-disk spelling. The supported cache filesystems are
    case-sensitive.
    """
    if not virtual_path.startswith("/"):
        return False
    parts = virtual_path[1:].split("/")
    if any(part in ("", ".", "..") for part in parts):
        return False
    if any(is_internal_cache_artifact(part) for part in parts):
        return False
    if not _is_listable_dir(cache_root):
        return False

    current = cache_root
    for part in parts[:-1]:
        current = current / part
        if os.path.islink(current) or not os.path.isdir(current):
            return False
        if not _is_listable_dir(current):
            return False

    leaf = current / parts[-1]
    try:
        os.lstat(str(leaf))
    except (OSError, ValueError):
        return False
    return os.path.isfile(str(leaf))


def scan_cache_entries(cache_root: Path) -> Iterator[CacheScanEntry]:
    """Walk cache files, parse metadata xattrs, and surface drift/invariant issues."""
    cache_root = cache_root.resolve()
    for root, dirnames, filenames in os.walk(cache_root):
        dirnames[:] = [name for name in dirnames if not is_internal_cache_artifact(name)]
        root_path = Path(root)

        for name in filenames:
            if is_internal_cache_artifact(name):
                continue

            cache_path = root_path / name
            try:
                st = os.lstat(str(cache_path))
            except OSError:
                continue

            if not os.path.isfile(str(cache_path)):
                continue

            virtual_path = cache_path_to_virtual_path(cache_root, cache_path)
            issues: List[str] = []
            metadata: Optional[CacheMetadataRecord] = None

            try:
                metadata = read_cache_metadata(cache_path)
            except ValueError as exc:
                issues.append(f"invalid_xattr:{exc}")
            except OSError as exc:
                issues.append(f"xattr_error:{exc}")

            if metadata is None:
                issues.append("missing_xattr")
            else:
                if metadata.virtual_path != virtual_path:
                    issues.append(
                        f"path_mismatch:xattr={metadata.virtual_path}:cache={virtual_path}"
                    )
                if metadata.size != st.st_size:
                    issues.append(
                        f"size_mismatch:xattr={metadata.size}:stat={st.st_size}"
                    )
                if abs(metadata.last_modified - st.st_mtime) > 1e-6:
                    issues.append(
                        f"mtime_mismatch:xattr={metadata.last_modified}:stat={st.st_mtime}"
                    )

            yield CacheScanEntry(
                cache_path=cache_path,
                virtual_path=virtual_path,
                stat_size=st.st_size,
                stat_mtime=st.st_mtime,
                stat_atime=st.st_atime,
                metadata=metadata,
                issues=issues,
            )
