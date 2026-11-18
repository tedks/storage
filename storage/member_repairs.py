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
import fcntl
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
# Written durably by a resolution just before its carrier is unlinked; still
# enforced while the carrier exists (see MemberRepairs.resolve).
RESOLVED = "resolved"
# Past this many open repairs, opening the cache warns: every fence and pin
# lookup scans them all.
GROWTH_WARNING = 1000
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
            if not isinstance(data["members"], list):
                # tuple() of a string would fence its characters.
                raise TypeError("members is not a list")
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
                and record.state in (INTENT, OPEN, RESOLVED)
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


FLOCK_WAIT = 5.0


def _flock_bounded(fd: int, wait: Optional[float] = None):
    """flock(LOCK_EX) for at most wait seconds; raises OSError(EWOULDBLOCK)
    after. Callers that hold namespace locks must not wait without bound on
    another process (a resolve stopped in a terminal, say)."""
    deadline = time.monotonic() + (FLOCK_WAIT if wait is None else wait)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise OSError(errno.EWOULDBLOCK, "member repair carrier is busy")
            time.sleep(0.01)


def fsync_directories(*paths: Path):
    """Make renames within these directories durable (each once)."""
    for path in dict.fromkeys(str(p) for p in paths):
        _fsync(Path(path), directory=True)


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
        # _lock guards the in-memory records and is never held across an
        # fsync; _write_lock orders this process's writers across their I/O.
        # Another process's writer (the CLI) is ordered by flock on the
        # carrier it changes.
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._records: Dict[str, MemberRepair] = {}
        self._seen: Optional[tuple] = None
        # Descriptors of this process's in-flight intents, flock held.
        self._held: Dict[str, int] = {}
        self._dir_durable = False
        with self._lock:
            self._reload_locked(strict=True, owner=sweep_new)
        if len(self._records) > GROWTH_WARNING:
            logger.warning("%d member repairs are open; each fence and pin lookup scans them all",
                           len(self._records))

    # -- loading ---------------------------------------------------------
    def _dir_stamp(self) -> Optional[tuple]:
        try:
            st = os.stat(self.dir)
        except FileNotFoundError:
            return None
        return (st.st_ino, st.st_mtime_ns, st.st_ctime_ns, st.st_nlink, st.st_size)

    def _reload_locked(self, *, strict: bool, owner: bool = False) -> bool:
        """Read every carrier. The cache's owner (the mount, one per cache)
        also removes unfinished .new files and finishes resolutions that a
        crash interrupted. Returns False, keeping the previous records, when
        a carrier cannot be read outside strict mode."""
        stamp = self._dir_stamp()
        records: Dict[str, MemberRepair] = {}
        if stamp is not None:
            for name in sorted(os.listdir(self.dir)):
                path = self.dir / name
                if name.endswith(_NEW_SUFFIX) and _ID_RE.match(name[:-len(_NEW_SUFFIX)]):
                    # Never renamed into place: written before any effect.
                    if owner:
                        try:
                            path.unlink()
                        except FileNotFoundError:
                            pass
                    continue
                try:
                    if not _ID_RE.match(name):
                        raise CorruptRepairRecord(f"{name}: not a record id")
                    raw = os.getxattr(str(path), RECORD_XATTR, follow_symlinks=False)
                    record = MemberRepair.from_json(raw, name)
                except FileNotFoundError:
                    continue  # resolved while listing
                except (OSError, CorruptRepairRecord) as exc:
                    if strict:
                        raise CorruptRepairRecord(
                            f"Unreadable member repair carrier {path}: {exc}. A repair that "
                            "cannot be read cannot be enforced; inspect or remove it by hand."
                        ) from exc
                    if stamp != self._seen:
                        logger.error("Keeping the previous member repairs: %s unreadable: %s",
                                     path, exc)
                    # Not retried until the directory changes again.
                    self._seen = stamp
                    return False
                if record.state == RESOLVED and owner:
                    # Its resolution was written; finish it under the same
                    # flock a live resolve holds, making the state durable
                    # first in case that resolve died before its fsync.
                    try:
                        finished = self._finish_resolution(path)
                    except (OSError, CorruptRepairRecord) as exc:
                        raise CorruptRepairRecord(
                            f"Could not finish the resolution of {path}: {exc}. Inspect or "
                            "remove it by hand."
                        ) from exc
                    if finished:
                        continue
                records[name] = record
            if owner:
                _fsync(self.dir, directory=True)
        # The stamp from before the listing, also for the owner (whose own
        # unlinks then cost one extra reload): a change made while listing,
        # such as a live resolve's unlink, shows as a mismatch at the next
        # lookup, which reloads.
        self._records = records
        self._seen = stamp
        return True

    @staticmethod
    def _finish_resolution(path: Path) -> bool:
        """Unlink a resolved carrier; False (keep enforcing it) if it is
        busy or no longer resolved."""
        try:
            fd = os.open(str(path), os.O_RDONLY)
        except FileNotFoundError:
            return True
        try:
            try:
                _flock_bounded(fd)
            except OSError:
                return False
            if os.fstat(fd).st_nlink == 0:
                return True
            if MemberRepair.from_json(os.getxattr(fd, RECORD_XATTR), path.name).state != RESOLVED:
                return False
            os.fsync(fd)
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            return True
        finally:
            os.close(fd)

    def _refresh(self):
        with self._lock:
            if self._dir_stamp() != self._seen:
                self._reload_locked(strict=False)
            return dict(self._records)

    def _after_write(self, update):
        """Bring memory up to the disk after a write of ours: a full reload,
        so an external change made before or during the write is never
        hidden behind our own new stamp. If the reload cannot read a
        carrier, apply our own change to the records kept."""
        with self._lock:
            if not self._reload_locked(strict=False):
                update(self._records)

    # -- writing ---------------------------------------------------------
    def _ensure_dir(self):
        # The cache root is fsynced once per instance, not only when this
        # instance made the directory: an earlier process may have crashed
        # between its mkdir and that fsync.
        if not self.dir.is_dir():
            self.dir.mkdir(mode=0o700, exist_ok=True)
        if not self._dir_durable:
            _fsync(self.dir.parent, directory=True)
            self._dir_durable = True

    def _write_new(self, record: MemberRepair, hold: bool = False) -> Optional[int]:
        """Create a carrier durably. With hold, return its descriptor with
        an exclusive flock taken before the carrier had its final name, so
        no resolve can ever act on it until the caller closes it."""
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
                if hold:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                os.setxattr(fd, RECORD_XATTR, payload)
                os.fsync(fd)
            except BaseException:
                os.close(fd)
                raise
            if not hold:
                os.close(fd)
            try:
                os.rename(temp, final)
                _fsync(self.dir, directory=True)
            except BaseException:
                if hold:
                    os.close(fd)
                raise
            return fd if hold else None
        except BaseException:
            # Not durable: the caller is told it failed, so nothing of it
            # may be left to be read later either.
            for leftover in (temp, final):
                try:
                    leftover.unlink()
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
        with self._write_lock:
            # Held until settle: a resolve while the rename is running
            # waits for it, so an in-flight intent is never resolved.
            held = self._write_new(record, hold=True)
            try:
                self._after_write(lambda records: records.__setitem__(record.id, record))
            except BaseException:
                # Nothing has happened yet: the intent goes, and its lock.
                try:
                    (self.dir / record.id).unlink()
                    _fsync(self.dir, directory=True)
                except OSError:
                    logger.exception("Leaving intent %s after a failed reload", record.id)
                finally:
                    os.close(held)
                raise
            self._held[record.id] = held
        return record.id

    def record(self, old_path: str, new_path: str, members: Iterable[str],
               reason: str) -> Optional[str]:
        """Durably record an open repair; raises OSError when it cannot.

        Returns the record id, or None when there are no members: a record
        with none would refuse the next open, and fences nothing.
        """
        members = set(members)
        if not members:
            return None
        record = self._make(old_path, new_path, members, OPEN, reason)
        with self._write_lock:
            self._write_new(record)
            self._after_write(lambda records: records.__setitem__(record.id, record))
        logger.warning(
            "Member(s) %s missed the rename %s -> %s, which is not replayed; repair %s "
            "(storage debug member-repairs): %s",
            ", ".join(record.members), old_path, new_path, record.id, record.reason,
        )
        return record.id

    def settle(self, repair_id: str, old_path: str, new_path: str, missed: Dict[str, str]):
        """Settle an intent once every member call has returned.

        The members that missed the move get a new open record, under a new
        id, made durable first; only then is the intent removed. A
        resolution of the intent id, before or after, therefore never
        covers the miss: before, the intent is gone or resolved and the new
        record still follows; after, the id names nothing and the resolve
        fails. The paths come from the caller, not from memory, which a
        concurrent reload may have changed. With no miss, the intent is
        simply removed.

        The intent's flock, taken by begin before the carrier had its name,
        is held from begin until here, so no resolve can remove the intent
        while the rename runs (it times out as busy) and the miss is always
        durable before the intent goes. An intent begun by another instance
        is locked with a bounded wait; past it, the intent stays
        (over-fencing, never under). Any failure leaves the intent likewise; it is logged,
        not raised: the evidence is already durable.
        """
        reasons = "; ".join(f"{m}: {missed[m]}" for m in sorted(missed))[:MAX_REASON_CHARS]
        with self._write_lock:
            held = self._held.pop(repair_id, None)
            try:
                if missed:
                    self._write_new(self._make(old_path, new_path, missed, OPEN, reasons))
                    logger.warning(
                        "Member(s) %s missed the rename %s -> %s, which is not replayed; repair "
                        "recorded (storage debug member-repairs): %s",
                        ", ".join(sorted(missed)), old_path, new_path, reasons,
                    )
                self._drop_intent(repair_id, held)
            except (OSError, CorruptRepairRecord):
                logger.exception(
                    "Could not settle member repair %s; it stays an intent for every member",
                    repair_id,
                )
            finally:
                if held is not None:
                    os.close(held)
            self._after_write(lambda records: None)

    def abandon(self, repair_id: str):
        """Keep an intent as it is and release its flock: the rename failed
        after its local move, so the intent stands for every target."""
        with self._write_lock:
            held = self._held.pop(repair_id, None)
            if held is not None:
                os.close(held)

    def _drop_intent(self, repair_id: str, held: Optional[int] = None):
        carrier = self.dir / repair_id
        if held is not None:
            fd = os.dup(held)  # already locked: same open file description
        else:
            try:
                fd = os.open(str(carrier), os.O_RDONLY)
            except FileNotFoundError:
                return  # resolved by someone else meanwhile
        try:
            if held is None:
                _flock_bounded(fd)
            if os.fstat(fd).st_nlink == 0:
                return
            current = MemberRepair.from_json(os.getxattr(fd, RECORD_XATTR), repair_id)
            if current.state != INTENT:
                return  # being resolved: that resolution finishes it
            carrier.unlink()
        finally:
            os.close(fd)
        _fsync(self.dir, directory=True)

    def resolve(self, repair_id: str) -> bool:
        """Drop a repair once someone has settled the members by hand.

        Two steps, so no reader can act on a resolution that is not yet
        durable: the record is first rewritten as resolved and fsynced
        (readers still enforce it while the carrier exists), then the
        carrier is unlinked, which is what readers act on. A crash after the
        first step leaves a resolved carrier, which the next open of the
        cache finishes. Returns False when no carrier has that id (a replay,
        a typo, or an id from elsewhere). Changes no member.
        """
        if not _ID_RE.match(repair_id):
            return False
        carrier = self.dir / repair_id
        with self._write_lock:
            try:
                fd = os.open(str(carrier), os.O_RDONLY)
            except FileNotFoundError:
                self._after_write(lambda records: records.pop(repair_id, None))
                return False
            try:
                _flock_bounded(fd)
                if os.fstat(fd).st_nlink == 0:
                    found = False
                else:
                    current = MemberRepair.from_json(os.getxattr(fd, RECORD_XATTR), repair_id)
                    resolved = MemberRepair(
                        id=current.id, old_path=current.old_path, new_path=current.new_path,
                        members=current.members, state=RESOLVED, reason=current.reason,
                        recorded=current.recorded,
                    )
                    os.setxattr(fd, RECORD_XATTR, resolved.to_json(), os.XATTR_REPLACE)
                    os.fsync(fd)
                    try:
                        carrier.unlink()
                    except FileNotFoundError:
                        pass  # the owner finished it: still this resolution
                    found = True
            finally:
                os.close(fd)
            if found:
                _fsync(self.dir, directory=True)
            self._after_write(lambda records: records.pop(repair_id, None))
        return found

    # -- reading ---------------------------------------------------------
    def list(self) -> List[MemberRepair]:
        """Every repair still on disk, oldest first (a RESOLVED one only
        while its resolution is being finished)."""
        return sorted(self._refresh().values(), key=lambda r: (r.recorded, r.id))

    def backends_for(self, virtual_paths: Iterable[str]) -> Set[str]:
        """The members with a repair at, above or below any of virtual_paths
        (either name counts). Renames, mkdir, rmdir, file and link uploads
        and conflict-view deletes at those names are not sent to them until
        the repair is resolved. A scan of every open
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
