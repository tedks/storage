from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional


def canonicalize_virtual_path(path: str) -> str:
    """Normalize a virtual path to the absolute form used internally."""
    if path.startswith("/"):
        return path
    return "/" + path


@dataclass(frozen=True)
class ResolvedPath:
    """Resolved view of a FUSE path, including backend suffix handling."""

    requested_path: str
    canonical_path: str
    backend_name: Optional[str]
    cache_path: Path
    canonical_cache_path: Path

    @property
    def parent_dir(self) -> str:
        parent = str(Path(self.canonical_path).parent)
        return canonicalize_virtual_path(parent)


def resolve_path(
    path: str,
    backend_names: Iterable[str],
    cache_path_for: Callable[[str], Path],
) -> ResolvedPath:
    """
    Resolve a FUSE path into its canonical path, backend hint, and cache targets.

    A request like ``/foo.txt.remote0`` resolves to canonical path ``/foo.txt``
    with backend hint ``remote0`` while preserving the request-specific cache
    path for conflict-view reads.
    """
    requested_path = canonicalize_virtual_path(path)
    requested = Path(requested_path)
    requested_name = requested.name

    canonical_path = requested_path
    backend_name: Optional[str] = None

    for candidate in backend_names:
        suffix = f".{candidate}"
        if requested_name.endswith(suffix):
            base_name = requested_name[: -len(suffix)]
            canonical_path = canonicalize_virtual_path(str(requested.parent / base_name))
            backend_name = candidate
            break

    return ResolvedPath(
        requested_path=requested_path,
        canonical_path=canonical_path,
        backend_name=backend_name,
        cache_path=cache_path_for(requested_path),
        canonical_cache_path=cache_path_for(canonical_path),
    )
