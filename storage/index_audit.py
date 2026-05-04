"""Cache/index rebuild and verification tooling."""

from dataclasses import dataclass, field
from pathlib import Path
import stat as stat_module
import time
from typing import Dict, List

from .backends.base import Backend
from .cache_metadata import scan_cache_entries, write_cache_metadata
from .models import FileLocation, FileMetadata, SyncStatus
from .state import StateManager


@dataclass
class IndexIssue:
    """Structured cache/index invariant problem."""

    kind: str
    virtual_path: str
    detail: str

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "virtual_path": self.virtual_path,
            "detail": self.detail,
        }


@dataclass
class RebuildIndexReport:
    """Result of reconstructing the local index from cache contents."""

    scanned_files: int = 0
    rebuilt_files: int = 0
    inferred_metadata_files: int = 0
    normalized_xattrs: int = 0
    skipped_files: int = 0
    issue_count: int = 0
    issues: List[IndexIssue] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "scanned_files": self.scanned_files,
            "rebuilt_files": self.rebuilt_files,
            "inferred_metadata_files": self.inferred_metadata_files,
            "normalized_xattrs": self.normalized_xattrs,
            "skipped_files": self.skipped_files,
            "issue_count": self.issue_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass
class VerifyIndexReport:
    """Result of comparing cache/index state without mutating either side."""

    scanned_files: int = 0
    issue_count: int = 0
    issues: List[IndexIssue] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "scanned_files": self.scanned_files,
            "issue_count": self.issue_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass
class ReprobeLocationsReport:
    """Result of probing storage members to rebuild non-cache location rows."""

    files_considered: int = 0
    members_requested: int = 0
    members_reprobed: int = 0
    members_unavailable: int = 0
    remote_locations_found: int = 0
    issue_count: int = 0
    issues: List[IndexIssue] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "files_considered": self.files_considered,
            "members_requested": self.members_requested,
            "members_reprobed": self.members_reprobed,
            "members_unavailable": self.members_unavailable,
            "remote_locations_found": self.remote_locations_found,
            "issue_count": self.issue_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def _validate_cache_root(cache_root: Path) -> Path:
    """Resolve and validate the cache root before auditing or rebuilding."""
    resolved = cache_root.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"cache directory does not exist: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"cache path is not a directory: {resolved}")
    return resolved


def _cache_path_for_metadata(cache_root: Path, virtual_path: str) -> Path:
    """Resolve the cache file path for a virtual path."""
    return cache_root / virtual_path.lstrip("/")


def _promote_conflict(
    cache_root: Path,
    state_manager: StateManager,
    metadata: FileMetadata,
    report: ReprobeLocationsReport,
    *,
    backend_name: str,
):
    """Mark the cached file conflicted in both SQLite and durable cache xattrs."""
    metadata.sync_status = SyncStatus.CONFLICT
    state_manager.set_file_metadata(metadata)

    cache_path = _cache_path_for_metadata(cache_root, metadata.virtual_path)
    if not cache_path.exists() or not cache_path.is_file():
        report.issues.append(
            IndexIssue(
                kind="missing_cache_file_for_conflict_sync",
                virtual_path=metadata.virtual_path,
                detail=f"{backend_name}: expected cache file at {cache_path}",
            )
        )
        return

    try:
        write_cache_metadata(cache_path, metadata, virtual_path=metadata.virtual_path)
    except OSError as exc:
        report.issues.append(
            IndexIssue(
                kind="cache_metadata_sync_failed",
                virtual_path=metadata.virtual_path,
                detail=f"{backend_name}: {exc}",
            )
        )


def rebuild_index_from_cache(cache_root: Path, state_manager: StateManager) -> RebuildIndexReport:
    """Reconstruct cache-backed index rows and normalize cache metadata xattrs."""
    cache_root = _validate_cache_root(cache_root)
    report = RebuildIndexReport()
    metadata_rows: List[FileMetadata] = []
    cache_locations: List[FileLocation] = []
    legacy_metadata: Dict[str, FileMetadata] = {
        metadata.virtual_path: metadata
        for metadata in state_manager.get_all_file_metadata()
    }

    for entry in scan_cache_entries(cache_root):
        report.scanned_files += 1
        metadata = entry.build_file_metadata()
        had_missing_xattr = any(issue == "missing_xattr" for issue in entry.issues)
        if had_missing_xattr:
            legacy = legacy_metadata.get(entry.virtual_path)
            if legacy is None:
                report.skipped_files += 1
                report.issues.append(
                    IndexIssue(
                        kind="missing_xattr_unrecoverable",
                        virtual_path=entry.virtual_path,
                        detail=(
                            "cache file has no metadata xattr and no legacy index row; "
                            "skipping to avoid indexing derived cache artifacts"
                        ),
                    )
                )
                continue
            metadata = legacy
            metadata.size = entry.stat_size
            metadata.last_access = entry.stat_atime
            metadata.last_modified = entry.stat_mtime
            report.inferred_metadata_files += 1

        metadata_rows.append(metadata)
        cache_locations.append(
            FileLocation(
                virtual_path=metadata.virtual_path,
                backend_name="cache",
                checksum=metadata.checksum or "",
                last_synced=entry.stat_mtime,
                is_cached=True,
            )
        )
        report.rebuilt_files += 1

        if entry.issues:
            for issue in entry.issues:
                if issue == "missing_xattr" and had_missing_xattr and entry.virtual_path not in legacy_metadata:
                    continue
                kind, _, detail = issue.partition(":")
                report.issues.append(
                    IndexIssue(
                        kind=kind,
                        virtual_path=entry.virtual_path,
                        detail=detail or issue,
                    )
                )

        try:
            write_cache_metadata(entry.cache_path, metadata, virtual_path=entry.virtual_path)
            if had_missing_xattr or entry.issues:
                report.normalized_xattrs += 1
        except OSError as exc:
            report.issues.append(
                IndexIssue(
                    kind="xattr_write_error",
                    virtual_path=entry.virtual_path,
                    detail=str(exc),
                )
            )

    state_manager.replace_index(metadata_rows, cache_locations)
    report.issue_count = len(report.issues)
    return report


def verify_index_against_cache(cache_root: Path, state_manager: StateManager) -> VerifyIndexReport:
    """Verify that cache files, cache metadata xattrs, and the SQLite index agree."""
    cache_root = _validate_cache_root(cache_root)
    report = VerifyIndexReport()

    db_metadata: Dict[str, FileMetadata] = {
        metadata.virtual_path: metadata
        for metadata in state_manager.get_all_file_metadata()
    }
    db_cache_locations = {
        location.virtual_path: location
        for location in state_manager.get_all_file_locations(is_cached=True)
    }
    seen_paths = set()

    for entry in scan_cache_entries(cache_root):
        report.scanned_files += 1
        seen_paths.add(entry.virtual_path)

        for issue in entry.issues:
            kind, _, detail = issue.partition(":")
            report.issues.append(
                IndexIssue(
                    kind=kind,
                    virtual_path=entry.virtual_path,
                    detail=detail or issue,
                )
            )

        metadata = db_metadata.get(entry.virtual_path)
        if metadata is None:
            report.issues.append(
                IndexIssue(
                    kind="missing_db_metadata",
                    virtual_path=entry.virtual_path,
                    detail="cache file exists but files row is missing",
                )
            )
        elif entry.metadata is not None:
            expected = entry.build_file_metadata()
            comparisons = (
                ("size", metadata.size, expected.size),
                ("checksum", metadata.checksum, expected.checksum),
                ("sync_status", metadata.sync_status.value, expected.sync_status.value),
                ("replica_count", metadata.replica_count, expected.replica_count),
                ("recomputable", metadata.recomputable, expected.recomputable),
                ("recompute_cmd", metadata.recompute_cmd, expected.recompute_cmd),
                ("encrypted", metadata.encrypted, expected.encrypted),
                ("encryption_key_id", metadata.encryption_key_id, expected.encryption_key_id),
            )
            for field_name, db_value, expected_value in comparisons:
                if db_value != expected_value:
                    report.issues.append(
                        IndexIssue(
                            kind="metadata_mismatch",
                            virtual_path=entry.virtual_path,
                            detail=f"{field_name}:db={db_value!r}:cache={expected_value!r}",
                        )
                    )
            if abs(metadata.last_modified - expected.last_modified) > 1e-6:
                report.issues.append(
                    IndexIssue(
                        kind="metadata_mismatch",
                        virtual_path=entry.virtual_path,
                        detail=(
                            f"last_modified:db={metadata.last_modified!r}:"
                            f"cache={expected.last_modified!r}"
                        ),
                    )
                )

        if entry.virtual_path not in db_cache_locations:
            report.issues.append(
                IndexIssue(
                    kind="missing_cache_location",
                    virtual_path=entry.virtual_path,
                    detail="cache file exists but cache location row is missing",
                )
            )

    for virtual_path in sorted(db_cache_locations):
        if virtual_path in seen_paths:
            continue
        report.issues.append(
            IndexIssue(
                kind="orphaned_cache_location",
                virtual_path=virtual_path,
                detail="cache location row exists without a cache file",
            )
        )

    report.issue_count = len(report.issues)
    return report


def reprobe_member_locations(
    state_manager: StateManager,
    backends: Dict[str, Backend],
    *,
    cache_root: Path,
) -> ReprobeLocationsReport:
    """Probe known file paths against storage members and rebuild non-cache location rows."""
    cache_root = _validate_cache_root(cache_root)
    report = ReprobeLocationsReport()
    metadata_rows = state_manager.get_all_file_metadata()
    known_paths = [metadata.virtual_path for metadata in metadata_rows]
    report.files_considered = len(metadata_rows)
    report.members_requested = len(backends)

    for backend_name in sorted(backends):
        backend = backends[backend_name]
        found_locations: List[FileLocation] = []
        member_failed = False

        for metadata in metadata_rows:
            try:
                stat_result = backend.stat(metadata.virtual_path)
            except FileNotFoundError:
                continue
            except Exception as exc:
                issue_kind = "member_unavailable" if not backend.is_available() else "member_probe_error"
                if issue_kind == "member_unavailable":
                    report.members_unavailable += 1
                report.issues.append(
                    IndexIssue(
                        kind=issue_kind,
                        virtual_path=metadata.virtual_path,
                        detail=f"{backend.name}: {exc}",
                    )
                )
                member_failed = True
                break

            if stat_module.S_ISDIR(getattr(stat_result, "st_mode", 0)):
                report.issues.append(
                    IndexIssue(
                        kind="member_type_mismatch",
                        virtual_path=metadata.virtual_path,
                        detail=f"{backend.name}: expected file but found directory",
                    )
                )
                continue

            checksum_getter = getattr(backend, "get_fast_checksum", None)
            if callable(checksum_getter):
                try:
                    remote_checksum = checksum_getter(metadata.virtual_path)
                except Exception as exc:
                    issue_kind = "member_unavailable" if not backend.is_available() else "member_probe_error"
                    if issue_kind == "member_unavailable":
                        report.members_unavailable += 1
                    report.issues.append(
                        IndexIssue(
                            kind=issue_kind,
                            virtual_path=metadata.virtual_path,
                            detail=f"{backend.name}: checksum probe failed: {exc}",
                        )
                    )
                    member_failed = True
                    break
            else:
                remote_checksum = metadata.checksum or ""
                report.issues.append(
                    IndexIssue(
                        kind="member_checksum_unverified",
                        virtual_path=metadata.virtual_path,
                        detail=f"{backend.name}: checksum probe unavailable; using local metadata checksum",
                    )
                )

            if metadata.checksum and remote_checksum != metadata.checksum:
                report.issues.append(
                    IndexIssue(
                        kind="member_checksum_mismatch",
                        virtual_path=metadata.virtual_path,
                        detail=(
                            f"{backend.name}: cache={metadata.checksum!r}:"
                            f"member={remote_checksum!r}"
                        ),
                    )
                )
                _promote_conflict(
                    cache_root,
                    state_manager,
                    metadata,
                    report,
                    backend_name=backend.name,
                )
                continue

            found_locations.append(
                FileLocation(
                    virtual_path=metadata.virtual_path,
                    backend_name=backend.name,
                    checksum=remote_checksum,
                    last_synced=getattr(stat_result, "st_mtime", time.time()),
                    is_cached=False,
                )
            )

        if member_failed:
            continue

        state_manager.replace_backend_locations_for_paths(
            backend.name,
            known_paths,
            found_locations,
            is_cached=False,
        )
        report.members_reprobed += 1
        report.remote_locations_found += len(found_locations)

    report.issue_count = len(report.issues)
    return report
