"""Cache/index rebuild and verification tooling."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from .cache_metadata import scan_cache_entries, write_cache_metadata
from .models import FileLocation, FileMetadata
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


def _validate_cache_root(cache_root: Path) -> Path:
    """Resolve and validate the cache root before auditing or rebuilding."""
    resolved = cache_root.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"cache directory does not exist: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"cache path is not a directory: {resolved}")
    return resolved


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
