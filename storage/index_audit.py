"""Cache/index rebuild and verification tooling."""

from dataclasses import dataclass, field, replace
from pathlib import Path
import stat as stat_module
import time
from typing import Dict, List

from .backends.base import Backend
from .cache_metadata import cache_file_would_be_scanned, scan_cache_entries, write_cache_metadata
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


def _promoted_conflict_metadata(metadata: FileMetadata) -> FileMetadata:
    """Return the conflict-marked metadata record for a virtual path."""
    return metadata if metadata.sync_status == SyncStatus.CONFLICT else replace(
        metadata,
        sync_status=SyncStatus.CONFLICT,
    )


def _write_conflict_xattr(
    cache_root: Path,
    metadata: FileMetadata,
    report: ReprobeLocationsReport,
    *,
    backend_name: str,
) -> tuple[Path, FileMetadata] | None:
    """Write the conflict xattr for a cache file without mutating SQLite state."""
    promoted = _promoted_conflict_metadata(metadata)
    cache_path = _cache_path_for_metadata(cache_root, metadata.virtual_path)
    if not cache_path.exists() or not cache_path.is_file():
        report.issues.append(
            IndexIssue(
                kind="missing_cache_file_for_conflict_sync",
                virtual_path=metadata.virtual_path,
                detail=f"{backend_name}: expected cache file at {cache_path}",
            )
        )
        return None

    try:
        write_cache_metadata(cache_path, promoted, virtual_path=metadata.virtual_path)
    except OSError as exc:
        report.issues.append(
            IndexIssue(
                kind="cache_metadata_sync_failed",
                virtual_path=metadata.virtual_path,
                detail=f"{backend_name}: {exc}",
            )
        )
        return None

    return cache_path, promoted


def _rollback_conflict_xattrs(
    written_conflicts: List[tuple[FileMetadata, Path, FileMetadata]],
    report: ReprobeLocationsReport,
    *,
    backend_name: str,
) -> List[FileMetadata]:
    """Restore previously written cache xattrs after a failed conflict promotion batch."""
    failed_promotions: List[FileMetadata] = []
    for original_metadata, cache_path, promoted_metadata in reversed(written_conflicts):
        try:
            write_cache_metadata(cache_path, original_metadata, virtual_path=promoted_metadata.virtual_path)
        except OSError as exc:
            failed_promotions.append(promoted_metadata)
            report.issues.append(
                IndexIssue(
                    kind="cache_metadata_rollback_failed",
                    virtual_path=promoted_metadata.virtual_path,
                    detail=f"{backend_name}: {exc}",
                )
            )
    return failed_promotions


def _reconcile_failed_rollback_metadata(
    state_manager: StateManager,
    promoted_metadata: List[FileMetadata],
    report: ReprobeLocationsReport,
    *,
    backend_name: str,
):
    """Best-effort DB reconciliation when conflict xattr rollback cannot restore old state."""
    if not promoted_metadata:
        return
    try:
        for metadata in promoted_metadata:
            state_manager.set_file_metadata(metadata)
    except Exception as exc:
        report.issues.append(
            IndexIssue(
                kind="cache_metadata_rollback_reconcile_failed",
                virtual_path="",
                detail=f"{backend_name}: {exc}",
            )
        )


def rebuild_index_from_cache(cache_root: Path, state_manager: StateManager) -> RebuildIndexReport:
    """Reconstruct cache-backed index rows and normalize cache metadata xattrs."""
    cache_root = _validate_cache_root(cache_root)
    report = RebuildIndexReport()
    def replacement_entries():
        for entry in scan_cache_entries(cache_root):
            report.scanned_files += 1
            metadata = entry.build_file_metadata()
            had_missing_xattr = any(issue == "missing_xattr" for issue in entry.issues)
            if had_missing_xattr:
                legacy = state_manager.get_file_metadata(entry.virtual_path)
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

            cache_location = FileLocation(
                virtual_path=metadata.virtual_path,
                backend_name="cache",
                checksum=metadata.checksum or "",
                last_synced=entry.stat_mtime,
                is_cached=True,
            )
            report.rebuilt_files += 1

            if entry.issues:
                for issue in entry.issues:
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

            yield metadata, cache_location

    state_manager.replace_index_from_entries(replacement_entries())
    report.issue_count = len(report.issues)
    return report


def verify_index_against_cache(cache_root: Path, state_manager: StateManager) -> VerifyIndexReport:
    """Verify that cache files, cache metadata xattrs, and the SQLite index agree."""
    cache_root = _validate_cache_root(cache_root)
    report = VerifyIndexReport()

    for entry in scan_cache_entries(cache_root):
        report.scanned_files += 1

        for issue in entry.issues:
            kind, _, detail = issue.partition(":")
            report.issues.append(
                IndexIssue(
                    kind=kind,
                    virtual_path=entry.virtual_path,
                    detail=detail or issue,
                )
            )

        metadata = state_manager.get_file_metadata(entry.virtual_path)
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

        cache_locations = [
            location
            for location in state_manager.get_file_locations(entry.virtual_path)
            if location.is_cached
        ]
        if not cache_locations:
            report.issues.append(
                IndexIssue(
                    kind="missing_cache_location",
                    virtual_path=entry.virtual_path,
                    detail="cache file exists but cache location row is missing",
                )
            )

    # A cache row is backed only if the scan above could have produced its path.
    # A bare is_file() would also accept non-canonical ("//a", "/./a"),
    # traversal ("/../x"), symlinked-directory and internal-artifact rows, which
    # are exactly the corruption this check exists to report.
    for location in state_manager.iter_file_locations(is_cached=True):
        if cache_file_would_be_scanned(cache_root, location.virtual_path):
            continue
        report.issues.append(
            IndexIssue(
                kind="orphaned_cache_location",
                virtual_path=location.virtual_path,
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
    report.files_considered = state_manager.count_file_metadata()
    report.members_requested = len(backends)

    for backend_name in sorted(backends):
        backend = backends[backend_name]
        found_locations: List[FileLocation] = []
        pending_conflicts: List[FileMetadata] = []
        replaceable_paths: List[str] = []
        member_failed = False

        for metadata in state_manager.iter_file_metadata():
            try:
                stat_result = backend.stat(metadata.virtual_path)
            except FileNotFoundError:
                replaceable_paths.append(metadata.virtual_path)
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
                replaceable_paths.append(metadata.virtual_path)
                report.issues.append(
                    IndexIssue(
                        kind="member_type_mismatch",
                        virtual_path=metadata.virtual_path,
                        detail=f"{backend.name}: expected file but found directory",
                    )
                )
                continue

            if backend.__class__.get_fast_checksum is not Backend.get_fast_checksum:
                try:
                    remote_checksum = backend.get_fast_checksum(metadata.virtual_path)
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
                report.issues.append(
                    IndexIssue(
                        kind="member_checksum_unverified",
                        virtual_path=metadata.virtual_path,
                        detail=(
                            f"{backend.name}: checksum probe unavailable; "
                            "preserving any existing location row because checksum verification is unavailable"
                        ),
                    )
                )
                continue

            if not metadata.checksum:
                report.issues.append(
                    IndexIssue(
                        kind="member_checksum_baseline_missing",
                        virtual_path=metadata.virtual_path,
                        detail=(
                            f"{backend.name}: member checksum {remote_checksum!r} observed, "
                            "but local metadata has no trusted checksum baseline; "
                            "preserving any existing location row"
                        ),
                    )
                )
                continue

            replaceable_paths.append(metadata.virtual_path)
            if remote_checksum != metadata.checksum:
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
                pending_conflicts.append(metadata)
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

        written_conflicts: List[tuple[FileMetadata, Path, FileMetadata]] = []
        for metadata in pending_conflicts:
            original_metadata = replace(metadata)
            written = _write_conflict_xattr(
                cache_root,
                metadata,
                report,
                backend_name=backend.name,
            )
            if written is None:
                failed_rollbacks = _rollback_conflict_xattrs(
                    written_conflicts,
                    report,
                    backend_name=backend.name,
                )
                _reconcile_failed_rollback_metadata(
                    state_manager,
                    failed_rollbacks,
                    report,
                    backend_name=backend.name,
                )
                member_failed = True
                break
            cache_path, promoted = written
            written_conflicts.append((original_metadata, cache_path, promoted))

        if member_failed:
            continue

        promoted_metadata = [promoted for _, _, promoted in written_conflicts]
        try:
            state_manager.apply_reprobe_backend_update(
                promoted_metadata=promoted_metadata,
                backend_name=backend.name,
                virtual_paths=replaceable_paths,
                locations=found_locations,
                is_cached=False,
            )
        except Exception as exc:
            failed_rollbacks = _rollback_conflict_xattrs(
                written_conflicts,
                report,
                backend_name=backend.name,
            )
            _reconcile_failed_rollback_metadata(
                state_manager,
                failed_rollbacks,
                report,
                backend_name=backend.name,
            )
            report.issues.append(
                IndexIssue(
                    kind="member_db_update_failed",
                    virtual_path="",
                    detail=f"{backend.name}: {exc}",
                )
            )
            continue

        for metadata in pending_conflicts:
            metadata.sync_status = SyncStatus.CONFLICT
        report.members_reprobed += 1
        report.remote_locations_found += len(found_locations)

    report.issue_count = len(report.issues)
    return report
