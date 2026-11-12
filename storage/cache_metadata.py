"""Cache-file metadata xattrs and cache/index audit helpers."""

import errno
import json
import os
import stat
import uuid
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
    ".storage.stage1-manifest",
})

STAGE1_MANIFEST = ".storage.stage1-manifest"
STAGE1_VERSION = 1


def stage1_claim_path(cache_root: Path) -> Path:
    """Deterministic claim beside an explicitly created scratch cache root."""
    return cache_root.parent / f".{cache_root.name}.storage-stage1-claim"


def stage1_authority_present(cache_root: Path) -> bool:
    """Detect either marker, including a dangling symlink, without creating state."""
    return (os.path.lexists(stage1_claim_path(cache_root))
            or os.path.lexists(cache_root / STAGE1_MANIFEST))


def _stage1_write_new(path: Path, payload: dict) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        while data:
            written = os.write(fd, data)
            if written <= 0:
                raise OSError(errno.EIO, "short Stage1 authority write")
            data = data[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _stage1_read(path: Path) -> dict:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError(f"Stage1 authority is not a regular file: {path}")
        data = os.read(fd, 1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            raise ValueError("Stage1 authority is too large")
        result = json.loads(data)
        if not isinstance(result, dict):
            raise ValueError("Stage1 authority is not an object")
        return result
    finally:
        os.close(fd)


def initialize_stage1_scratch_root(cache_root: Path, state_db: Path) -> str:
    """Claim an absent scratch root; never adopt or clean up an existing one.

    This provisions authority only. The restricted Stage1 read mode remains
    unavailable until its callback and direct-cache refusal boundary exists.
    An interrupted claim intentionally remains for diagnosis, not adoption.
    """
    cache_root, state_db = Path(cache_root).absolute(), Path(state_db).absolute()
    parent = cache_root.parent
    if (not stat.S_ISDIR(os.lstat(parent).st_mode)
            or state_db.is_relative_to(cache_root)
            or os.path.lexists(cache_root) or os.path.lexists(state_db)):
        raise ValueError("Stage1 needs absent cache and index under an existing scratch parent")
    root_uuid = str(uuid.uuid4())
    _stage1_write_new(stage1_claim_path(cache_root), {
        "version": STAGE1_VERSION, "uuid": root_uuid,
        "cache_root": str(cache_root), "state_db": str(state_db),
    })
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    cache_root.mkdir()
    root_stat = os.lstat(cache_root)
    _stage1_write_new(cache_root / STAGE1_MANIFEST, {
        "version": STAGE1_VERSION, "uuid": root_uuid,
        "root_dev": root_stat.st_dev, "root_ino": root_stat.st_ino,
        "revision": 0, "debts": {},
    })
    root_fd = os.open(cache_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(root_fd)
    finally:
        os.close(root_fd)
    return root_uuid


def verify_stage1_scratch_root(cache_root: Path, state_db: Path, expected_uuid: str) -> dict:
    """Read-only pre-constructor authority check; absence is never empty debt."""
    cache_root, state_db = Path(cache_root).absolute(), Path(state_db).absolute()
    claim = _stage1_read(stage1_claim_path(cache_root))
    root_stat = os.lstat(cache_root)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise ValueError("Stage1 cache root is not a directory")
    manifest = _stage1_read(cache_root / STAGE1_MANIFEST)
    if (claim.get("version") != STAGE1_VERSION
            or manifest.get("version") != STAGE1_VERSION
            or claim.get("uuid") != expected_uuid
            or manifest.get("uuid") != expected_uuid
            or claim.get("cache_root") != str(cache_root)
            or claim.get("state_db") != str(state_db)
            or manifest.get("root_dev") != root_stat.st_dev
            or manifest.get("root_ino") != root_stat.st_ino
            or not isinstance(manifest.get("revision"), int)
            or not isinstance(manifest.get("debts"), dict)):
        raise ValueError("Stage1 scratch authority mismatch")
    return manifest
_INTERNAL_CACHE_PREFIXES = (
    ".storage.mount.",
    ".storage.db-",
)
# Names ending in this are cache-internal scratch (full-fetch temp files,
# streaming partials): hidden from listings, never indexed, and removed by
# the startup sweep (StreamingReadManager.cleanup_stale_partials).
PARTIAL_SUFFIX = ".storage.partial"
# On the inode of a failed streaming session's prefix kept in the cache:
# the bytes are a prefix of the member's file, never to be indexed as the
# whole file. A whole-file fetch clears it from its copy (a member file may
# carry it from an older copy that kept xattrs).
PREFIX_XATTR = "user.storage.prefix"
_INTERNAL_CACHE_SUFFIXES = (
    PARTIAL_SUFFIX,
)


def regular_file_stat(path) -> Optional[os.stat_result]:
    """The lstat of path when it is a regular file, else None.

    Cache scans and xattr writes use this rather than exists()/is_file(),
    which follow a symlink: a link in the cache tree (made through the
    mount, or planted) is not a cached copy, and following it would count
    or rewrite whatever it points at (ditz cache-scans-follow-symlinks).

    Only the last component is checked. A directory component that is a
    link is still traversed; through the mount none can be reached (getattr
    lstats the cache first, so the kernel follows a cached link itself and
    never sends a path beneath it), so only someone who can already write
    the cache directory could plant one (ditz cache-parent-symlink-traversal).
    """
    try:
        st = os.lstat(str(path))
    except (OSError, ValueError):
        return None
    return st if stat.S_ISREG(st.st_mode) else None


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

    def is_stale_for(self, stat_size: int, stat_mtime: float) -> bool:
        """Whether the cache file changed after this record was written.

        The record carries the file's size and mtime as of the setxattr that
        wrote it. A live stat that differs means bytes landed afterwards and
        the recorded status describes content that is gone. The comparison
        is exact: both sides come from stat and round-trip unchanged through
        JSON and SQLite, and any tolerance could only hide a write. What it
        cannot see is a same-size write in the same timestamp tick as the
        record (a few ms on kernels without fine-grained timestamps).
        """
        return stat_size != self.size or stat_mtime != self.last_modified

    def to_file_metadata(
        self,
        *,
        virtual_path: str,
        stat_size: int,
        stat_mtime: float,
        last_access: float,
    ) -> FileMetadata:
        """Build runtime metadata using the live cache path and file stat.

        A record that is stale for the live stat yields DIRTY whatever status
        it recorded: the file was written after the record, and nothing says
        those bytes reached a member. Erring this way costs a redundant sync;
        believing a stale CLEAN loses the write. The checksum is kept: it is
        the checksum of the content last synced, which the members still hold.
        """
        stale = self.is_stale_for(stat_size, stat_mtime)
        return FileMetadata(
            virtual_path=virtual_path,
            size=stat_size,
            checksum=self.checksum,
            last_access=last_access,
            last_modified=stat_mtime,
            sync_status=SyncStatus.DIRTY if stale else self.sync_status,
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
        raw = os.getxattr(str(cache_path), XATTR_CACHE_METADATA_NAME, follow_symlinks=False)
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
    """Write the cache metadata xattr for a cache file.

    Only on a regular file, and never through a link: a symlink is refused
    with EINVAL, and the setxattr does not follow one swapped in after the
    lstat.
    """
    st = os.lstat(str(cache_path))
    if not stat.S_ISREG(st.st_mode):
        raise OSError(errno.EINVAL, "not a regular file", str(cache_path))
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
        follow_symlinks=False,
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
    the leaf lstats as a regular file (a symlink is never scanned).
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

    return regular_file_stat(current / parts[-1]) is not None


def scan_cache_links(cache_root: Path) -> Iterator[tuple]:
    """Walk the cache for symlinks: (virtual_path, target, lstat) each.

    The links made through the mount (ditz symlink-replication); they carry
    no metadata xattr (Linux refuses user.* on a link), so the link itself
    is the record. Internal artifacts are skipped as scan_cache_entries
    skips them; directory links are not descended into.
    """
    cache_root = cache_root.resolve()
    for root, dirnames, filenames in os.walk(cache_root):
        dirnames[:] = [name for name in dirnames if not is_internal_cache_artifact(name)]
        root_path = Path(root)
        # os.walk lists a link to a directory among dirnames.
        for name in [*filenames, *dirnames]:
            if is_internal_cache_artifact(name):
                continue
            path = root_path / name
            try:
                st = os.lstat(str(path))
                if not stat.S_ISLNK(st.st_mode):
                    continue
                target = os.readlink(str(path))
            except OSError:
                continue
            yield cache_path_to_virtual_path(cache_root, path), target, st


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

            if not stat.S_ISREG(st.st_mode):
                # A symlink is not a cached copy: never follow it to read
                # or rewrite its target's metadata.
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
                if metadata.last_modified != st.st_mtime:
                    issues.append(
                        f"mtime_mismatch:xattr={metadata.last_modified}:stat={st.st_mtime}"
                    )
                if metadata.sync_status == SyncStatus.CLEAN and metadata.is_stale_for(
                    st.st_size, st.st_mtime
                ):
                    issues.append(
                        "stale_clean_xattr:file changed after its xattr said clean; treated as dirty"
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
