"""Durable records of renames that storage members missed.

A member that misses a rename through the mount is never sent it again
later: nothing on the member proves the object at the old name is still the
one renamed, or that the new name is free (other writers change members
behind the mount). The missed move becomes a repair instead. While it is
open, the member gets no rename, mkdir, rmdir or upload at names overlapping
either path, and cached files under the new name are not evicted.

The records live in the cache, not the index (which is rebuildable): one
empty carrier file per record under <cache>/.storage.repairs, named by the
record id, holding the record in its user.storage.member_repair xattr. See
docs/member-rename-repairs.md for the format and the crash ordering.
"""

import errno
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

CARRIER_DIR = ".storage.repairs"
RECORD_XATTR = "user.storage.member_repair"
RECORD_VERSION = 1
# Inside ext4's per-inode xattr block on any filesystem: a larger record is
# refused before anything is written (ENAMETOOLONG).
MAX_RECORD_BYTES = 3584
MAX_REASON_CHARS = 256
INTENT = "intent"
OPEN = "open"
_NEW_SUFFIX = ".new"
_ID_RE = re.compile(r"^[0-9a-f]{32}$")


class CorruptRepairRecord(RuntimeError):
    """A carrier whose record cannot be read: the cache is not opened."""


@dataclass(frozen=True)
class MemberRepair:
    """A rename that the listed members missed, left for repair.

    Each member may still hold the moved object at old_path, or the rename
    may have taken effect there before the failure was seen. An INTENT
    record is one whose rename was interrupted before it settled: whether
    the local move or any member call happened is unknown.
    """

    id: str
    old_path: str
    new_path: str
    members: Tuple[str, ...]
    state: str
    reason: str
    recorded: float

    def to_json(self) -> bytes:
        return json.dumps({
            "v": RECORD_VERSION,
            "id": self.id,
            "old": self.old_path,
            "new": self.new_path,
            "members": list(self.members),
            "state": self.state,
            "reason": self.reason,
            "recorded": self.recorded,
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes, carrier_name: str) -> "MemberRepair":
        try:
            data = json.loads(raw.decode("utf-8"))
            record = cls(
                id=data["id"],
                old_path=data["old"],
                new_path=data["new"],
                members=tuple(data["members"]),
                state=data["state"],
                reason=data["reason"],
                recorded=float(data["recorded"]),
            )
            valid = (
                data["v"] == RECORD_VERSION
                and record.id == carrier_name
                and record.state in (INTENT, OPEN)
                and record.members
                and all(isinstance(m, str) and m for m in record.members)
                and all(isinstance(p, str) and p.startswith("/")
                        for p in (record.old_path, record.new_path))
                and isinstance(record.reason, str)
            )
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise CorruptRepairRecord(f"{carrier_name}: {exc}") from exc
        if not valid:
            raise CorruptRepairRecord(f"{carrier_name}: not a valid version-{RECORD_VERSION} record")
        return record


def is_at_or_under(path: str, root: str) -> bool:
    """Segment-bounded: /ab is not under /a. Every path is under /."""
    root = root.rstrip("/")
    return root == "" or path == root or path.startswith(root + "/")


def paths_overlap(a: str, b: str) -> bool:
    """Whether one path is at, above or below the other."""
    return is_at_or_under(a, b) or is_at_or_under(b, a)


def is_carrier_path(canonical_path: str) -> bool:
    """Whether a canonical virtual path is the carrier directory or under it."""
    return canonical_path.lstrip("/").split("/", 1)[0] == CARRIER_DIR


def _fsync(path: Path, directory: bool = False):
    fd = os.open(str(path), os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class MemberRepairs:
    """The open repairs of one cache: the carriers are the authority.

    Loaded when the cache opens, before anything can upload, rename, sweep
    or evict. Lookups reload when the carrier directory changes, which is
    how a resolution by another process (the CLI) reaches a mount; a change
    missed within one timestamp tick can only be such a removal, so missing
    it keeps a protection longer, never drops one.
    """

    def __init__(self, cache_root: Path, *, sweep_new: bool = False):
        self.dir = Path(cache_root) / CARRIER_DIR
        self._lock = threading.Lock()
        self._records: Dict[str, MemberRepair] = {}
        self._seen: Optional[tuple] = None
        with self._lock:
            self._reload_locked(strict=True, sweep_new=sweep_new)

    # -- loading ---------------------------------------------------------
    def _dir_stamp(self) -> Optional[tuple]:
        try:
            st = os.stat(self.dir)
        except FileNotFoundError:
            return None
        return (st.st_ino, st.st_mtime_ns, st.st_ctime_ns, st.st_nlink, st.st_size)

    def _reload_locked(self, *, strict: bool, sweep_new: bool = False):
        stamp = self._dir_stamp()
        records: Dict[str, MemberRepair] = {}
        if stamp is not None:
            for name in sorted(os.listdir(self.dir)):
                path = self.dir / name
                if name.endswith(_NEW_SUFFIX) and _ID_RE.match(name[:-len(_NEW_SUFFIX)]):
                    # Never renamed into place: written before any effect.
                    # Only the cache's owner removes one (another process may
                    # be writing it).
                    if sweep_new:
                        try:
                            path.unlink()
                        except FileNotFoundError:
                            pass
                    continue
                try:
                    if not _ID_RE.match(name):
                        raise CorruptRepairRecord(f"{name}: not a record id")
                    raw = os.getxattr(str(path), RECORD_XATTR, follow_symlinks=False)
                    records[name] = MemberRepair.from_json(raw, name)
                except (OSError, CorruptRepairRecord) as exc:
                    if strict:
                        raise CorruptRepairRecord(
                            f"Unreadable member repair carrier {path}: {exc}. A repair that "
                            "cannot be read cannot be enforced; inspect or remove it by hand."
                        ) from exc
                    logger.error("Keeping the previous member repairs: %s unreadable: %s", path, exc)
                    return
            stamp = self._dir_stamp()
        self._records = records
        self._seen = stamp

    def _refresh(self):
        with self._lock:
            if self._dir_stamp() != self._seen:
                self._reload_locked(strict=False)
            return dict(self._records)

    # -- writing ---------------------------------------------------------
    def _ensure_dir(self):
        if not self.dir.is_dir():
            self.dir.mkdir(mode=0o700, exist_ok=True)
            _fsync(self.dir.parent, directory=True)

    def _write_new(self, record: MemberRepair):
        payload = record.to_json()
        if len(payload) > MAX_RECORD_BYTES:
            raise OSError(errno.ENAMETOOLONG,
                          f"member repair record is {len(payload)} bytes, over {MAX_RECORD_BYTES}")
        self._ensure_dir()
        temp = self.dir / (record.id + _NEW_SUFFIX)
        final = self.dir / record.id
        fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            try:
                os.setxattr(fd, RECORD_XATTR, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.rename(temp, final)
            _fsync(self.dir, directory=True)
        except BaseException:
            try:
                temp.unlink()
            except OSError:
                pass
            raise

    def _make(self, old_path, new_path, members, state, reason) -> MemberRepair:
        return MemberRepair(
            id=uuid.uuid4().hex,
            old_path=old_path,
            new_path=new_path,
            members=tuple(sorted(set(members))),
            state=state,
            reason=reason[:MAX_REASON_CHARS],
            recorded=time.time(),
        )

    def begin(self, old_path: str, new_path: str, members: Iterable[str]) -> Optional[str]:
        """Durably record an intent to rename on members, before any effect.

        Returns the record id, or None when there are no members. Raises
        OSError when the record cannot be made durable; then the rename
        must not proceed.
        """
        members = set(members)
        if not members:
            return None
        record = self._make(old_path, new_path, members, INTENT,
                            "rename interrupted before every member's outcome was recorded")
        with self._lock:
            self._write_new(record)
            self._records[record.id] = record
            self._seen = self._dir_stamp()
        return record.id

    def record(self, old_path: str, new_path: str, members: Iterable[str], reason: str) -> str:
        """Durably record an open repair; raises OSError when it cannot."""
        record = self._make(old_path, new_path, members, OPEN, reason)
        with self._lock:
            self._write_new(record)
            self._records[record.id] = record
            self._seen = self._dir_stamp()
        logger.warning(
            "Member(s) %s missed the rename %s -> %s, which is not replayed; repair %s "
            "(storage debug member-repairs): %s",
            ", ".join(record.members), old_path, new_path, record.id, record.reason,
        )
        return record.id

    def settle(self, repair_id: str, missed: Dict[str, str]):
        """Settle an intent: drop it if no member missed the move, else
        rewrite it as an open repair for the members that did.

        A failure leaves the intent, which fences every target member: more
        than needed, never less. It is logged, not raised; the evidence is
        already durable.
        """
        with self._lock:
            intent = self._records.get(repair_id)
            if intent is None:
                return
            carrier = self.dir / repair_id
            try:
                if not missed:
                    carrier.unlink()
                    _fsync(self.dir, directory=True)
                    del self._records[repair_id]
                else:
                    reasons = "; ".join(f"{m}: {missed[m]}" for m in sorted(missed))
                    record = MemberRepair(
                        id=repair_id, old_path=intent.old_path, new_path=intent.new_path,
                        members=tuple(sorted(missed)), state=OPEN,
                        reason=reasons[:MAX_REASON_CHARS], recorded=intent.recorded,
                    )
                    payload = record.to_json()
                    if len(payload) > MAX_RECORD_BYTES:
                        raise OSError(errno.ENAMETOOLONG, "settled record too large")
                    fd = os.open(str(carrier), os.O_RDONLY)
                    try:
                        os.setxattr(fd, RECORD_XATTR, payload, os.XATTR_REPLACE)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    self._records[repair_id] = record
                    logger.warning(
                        "Member(s) %s missed the rename %s -> %s, which is not replayed; "
                        "repair %s (storage debug member-repairs): %s",
                        ", ".join(record.members), record.old_path, record.new_path,
                        repair_id, record.reason,
                    )
            except OSError:
                logger.exception(
                    "Could not settle member repair %s; it stays an intent for every member",
                    repair_id,
                )
            self._seen = self._dir_stamp()

    def resolve(self, repair_id: str) -> bool:
        """Drop a repair once someone has settled the members by hand.

        The carrier's removal is made durable before the record leaves
        memory. Returns False when no carrier has that id. Changes no member.
        """
        if not _ID_RE.match(repair_id):
            return False
        with self._lock:
            try:
                (self.dir / repair_id).unlink()
            except FileNotFoundError:
                self._records.pop(repair_id, None)
                return False
            _fsync(self.dir, directory=True)
            self._records.pop(repair_id, None)
            self._seen = self._dir_stamp()
        return True

    # -- reading ---------------------------------------------------------
    def list(self) -> List[MemberRepair]:
        """Every open repair, oldest first."""
        return sorted(self._refresh().values(), key=lambda r: (r.recorded, r.id))

    def backends_for(self, virtual_paths: Iterable[str]) -> Set[str]:
        """The members with a repair at, above or below any of virtual_paths
        (either name counts). Nothing the namespace does at those names is
        sent to them until the repair is resolved. A scan of every open
        repair, expected to be few."""
        paths = list(virtual_paths)
        return {
            member
            for record in self._refresh().values()
            if any(paths_overlap(path, name)
                   for path in paths for name in (record.old_path, record.new_path))
            for member in record.members
        }

    def covers(self, virtual_path: str) -> bool:
        """Whether virtual_path is at or under the new name of any repair: a
        member that missed the move may hold its bytes only under the old
        name, so the cached copy stays. Conservative: any member's repair."""
        return any(is_at_or_under(virtual_path, record.new_path)
                   for record in self._refresh().values())
