"""Symbolic links made through the mount (ditz symlink-replication).

A link follows its target: it is created on exactly the members that hold
the target, resolved relative to the link's directory through the union.
Only targets that mean the same thing on a member and through the mount are
accepted: relative, staying inside the mount root, and naming something
that exists when the link is made.
"""

import hashlib
import posixpath
from typing import Optional


class LinkRefused(Exception):
    """The target cannot be placed; the message says why (logged, EPERM)."""


def resolve_link_target(link_path: str, target: str) -> str:
    """The canonical virtual path a link at link_path with contents target names.

    Lexical: '..' climbs from the link's directory without consulting the
    members, as it would through the mount when no component on the way is
    itself a link. Raises LinkRefused for an empty or absolute target, a
    NUL byte, or one that climbs above the mount root (the kernel would
    resolve it outside the mount).
    """
    if not target:
        raise LinkRefused("empty target")
    if "\0" in target:
        raise LinkRefused("NUL in target")
    if target.startswith("/"):
        raise LinkRefused("absolute target")
    parts = [p for p in posixpath.dirname(link_path).split("/") if p]
    for component in target.split("/"):
        if component in ("", "."):
            continue
        if component == "..":
            if not parts:
                raise LinkRefused("target escapes the mount root")
            parts.pop()
            continue
        parts.append(component)
    return "/" + "/".join(parts)


def link_checksum(target: str) -> str:
    """Identity of a link for the conflict view and its location rows.

    Links compare by target string; the prefix keeps the value apart from
    any content hash.
    """
    return "link:" + hashlib.sha256(target.encode("utf-8", "surrogateescape")).hexdigest()


def is_link_checksum(checksum: Optional[str]) -> bool:
    return bool(checksum) and checksum.startswith("link:")


def find_holders(target_path: str, backends, located=()):
    """The members holding target_path now: (holders, unsure).

    holders: available members whose lstat finds the target. unsure: names
    of members that may hold it but cannot be asked (unavailable with a
    location row for it, or whose lstat failed other than ENOENT); a link
    whose holders include one of these is retried later.
    """
    holders = []
    unsure = []
    for name, backend in backends.items():
        if not backend.is_available():
            if name in located:
                unsure.append(name)
            continue
        try:
            backend.lstat(target_path)
        except FileNotFoundError:
            continue
        except Exception:
            unsure.append(name)
            continue
        holders.append(backend)
    return holders, unsure
