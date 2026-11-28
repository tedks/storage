"""Cache-file metadata xattrs and cache/index audit helpers."""

import errno
import json
import math
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Iterator, List, Optional

from .file_config import get_inherited_config
from .models import FileLocation, FileMetadata, SyncStatus


MEMBER_ATTRS_XATTR = "user.storage.source_attributes"
MEMBER_STAT_XATTR = "user.storage.member_stat"
# On a cached copy whose refresh could not be verified: the last good bytes,
# known stale, not a baseline for writes (StorageFS._mark_last_good_stale).
STALE_LAST_GOOD_XATTR = "user.storage.stale_last_good"
_SOURCE_ATTR_FIELDS = ("st_mode", "st_uid", "st_gid", "st_nlink", "st_ctime")
# Set-ID bits are never reported for a regular file, whatever the member
# or a chmod through the mount says: the kernel would honour a reported
# setuid bit, with the member's uid, on a mount without nosuid.
SET_ID_BITS = stat.S_ISUID | stat.S_ISGID


def reported_mode(mode: int) -> int:
    """A mode as the mount reports it: a regular file never has set-ID bits."""
    return mode & ~SET_ID_BITS if stat.S_ISREG(mode) else mode


def _inode(st: os.stat_result) -> int:
    # The inode number alone, as MEMBER_STAT_XATTR's identity: st_dev can
    # change across a remount (btrfs subvolumes, ZFS, overlayfs anonymous
    # devices) while the inode and its xattrs stay. A copy of the xattr onto
    # another file (copy2 out and back) still has another inode.
    return st.st_ino


def _source_attrs_basis(st: os.stat_result) -> dict:
    return {
        "st_mode": st.st_mode,
        "st_uid": st.st_uid,
        "st_gid": st.st_gid,
        "st_size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def _source_time_ns(source, field: str, fallback: int) -> int:
    ns = getattr(source, field + "_ns", None)
    if ns is not None:
        return ns
    seconds = getattr(source, field, None)
    return fallback if seconds is None else round(seconds * 1_000_000_000)


def _member_mtime_ns(st) -> Optional[int]:
    ns = getattr(st, "st_mtime_ns", None)
    if type(ns) is int:
        return ns
    seconds = getattr(st, "st_mtime", None)
    if seconds is None or not math.isfinite(seconds):
        return None
    return round(seconds * 1_000_000_000)


class MemberVersion:
    """confirm_member_stat's verdict on what a transfer read.

    Consumed by CacheManager.fetch_into_cache (full fills) and
    StreamingReadManager._finalize_streaming_session (streams): only
    AGREED and NEVER_VERSIONED copies are installed in the cache.
    """

    # Sticky versioning: once any stat seen for a fill carries a version
    # token (size and mtime), the fill is published only on AGREED.
    #
    # The probe's token was confirmed after the transfer: install with the
    # confirming stat's attributes.
    AGREED = "agreed"
    # Positive evidence of another version (a size, or two mtimes or modes,
    # that differ): never installed. A full fill fetches once more only
    # against a newer stat that carries a token; a stream is discarded.
    CHANGED = "changed"
    # Some stat carried a token but no compatible pair confirmed it (the
    # confirming stat failed or lost its mtime, or only the stat after the
    # transfer had one): not a verified generation, never installed; any
    # cached copy stays.
    UNCONFIRMED = "unconfirmed"
    # No stat seen carried a token (no stat, size-only, no mtime): the
    # complete bytes are installed with conservative attributes and no
    # member mtime.
    NEVER_VERSIONED = "never-versioned"


class FillAction:
    """What a completed transfer's MemberVersion allows (fill_action)."""

    INSTALL = "install"   # publish in the shared cache (AGREED, NEVER_VERSIONED)
    FAIL = "fail"         # nothing served from it: EAGAIN (retryable)


def fill_action(version: str) -> str:
    """The one verdict-to-action contract, for full fills
    (CacheManager.fetch_into_cache) and streams.

    AGREED and NEVER_VERSIONED install. CHANGED and UNCONFIRMED never
    publish and serve nothing from the copy: a cold open, read-only or
    writable, fails with EAGAIN; a warm read-only open keeps the last good
    copy (CTO-208-MMAP-FALLBACK, which withdrew CTO-208-LIVE-READ's private
    serve: a private copy shared the path's page cache with its other
    handles through mmap).
    """
    if version in (MemberVersion.AGREED, MemberVersion.NEVER_VERSIONED):
        return FillAction.INSTALL
    return FillAction.FAIL


def has_version_token(st) -> bool:
    """Whether a member stat carries a version token: a size and an mtime."""
    return st is not None and getattr(st, "st_size", None) is not None \
        and _member_mtime_ns(st) is not None


_has_token = has_version_token


class StatBudget:
    """Member stats a transfer's confirmation may still make (shared across calls)."""

    def __init__(self, stats: int = 2):
        self.remaining = stats


def confirm_member_stat(backend, virtual_path: str, probe, transferred_size: int,
                        budget: Optional[StatBudget] = None, *, full_fill: bool = False):
    """(stat to record, MemberVersion) for a transfer probed before it began.

    The member is stat'ed again after the transfer (retried once if the
    stat raises; not after NotImplementedError). CHANGED needs positive
    evidence: a known size other than the transfer's, or two stats that
    both report an mtime, or a mode, and disagree; the newest stat is
    returned. AGREED needs a probe and a confirming stat that both carry a
    token (size and mtime) with nothing disagreeing; the attributes are
    returned with the probe's atime (the confirming stat's may be the
    transfer's own read). If only one of them carries a token it is
    UNCONFIRMED (sticky versioning: a token seen anywhere in a fill makes
    AGREED the only verdict that publishes). If neither does it is
    NEVER_VERSIONED: the attributes some stat reported, newest first, the
    probe's atime, and no mtime (none was compared).

    A mode only the probe reported (the confirming stat lacked one) is
    marked mode_unconfirmed, and copy_member_attributes records it without
    write bits: its denials stand, its grants of write do not. No mode at
    all fails closed on write as before. BulkCapacityCancelled and
    BulkCapacityTimeout propagate: an interrupted request is not a fill.
    A shared budget bounds the stats across calls for one transfer (the
    retry here and a caller's second confirmation together); by default
    each call may make two. A full fill preserves definitive member errors.
    Two transient I/O failures leave the version unconfirmed, even without
    a pathname token. Explicitly unsupported stat is missing evidence.
    """
    from .backends.ssh_pool import BulkCapacityCancelled, BulkCapacityTimeout

    budget = StatBudget() if budget is None else budget
    after = None
    failed = False
    while budget.remaining > 0:
        budget.remaining -= 1
        try:
            after = backend.stat(virtual_path)
            break
        except (BulkCapacityCancelled, BulkCapacityTimeout):
            raise
        except NotImplementedError:
            break  # no stat at all: nothing to retry
        except OSError as exc:
            if full_fill and exc.errno in (errno.ENOSYS, errno.ENOTSUP):
                break
            if full_fill and exc.errno != errno.EIO:
                raise
            failed = True
        except Exception:  # noqa: BLE001 - streaming confirmation is best effort
            if full_fill:
                raise
            failed = True

    def known(st, key):
        return getattr(st, key, None) if st is not None else None

    sizes = [known(st, "st_size") for st in (probe, after)]
    mtimes = [_member_mtime_ns(st) if st is not None else None for st in (probe, after)]
    modes = [known(st, "st_mode") for st in (probe, after)]
    changed = (any(size is not None and size != transferred_size for size in sizes)
               or (None not in mtimes and mtimes[0] != mtimes[1])
               or (None not in modes and modes[0] != modes[1]))
    if changed:
        return after, MemberVersion.CHANGED
    if _has_token(probe) and _has_token(after):
        verdict = MemberVersion.AGREED
    elif _has_token(probe) or _has_token(after) or (full_fill and failed and after is None):
        verdict = MemberVersion.UNCONFIRMED
    else:
        verdict = MemberVersion.NEVER_VERSIONED
    if probe is None and after is None:
        return None, verdict
    fields = {}
    for key in ("st_mode", "st_uid", "st_gid", "st_nlink", "st_ctime", "st_size",
                "st_mtime", "st_mtime_ns"):
        value = known(after, key)
        fields[key] = known(probe, key) if value is None else value
    # Identify the confirmed pathname independently of the original probe.
    fields["st_dev"], fields["st_ino"] = known(after, "st_dev"), known(after, "st_ino")
    # The atime the member reported before the transfer read it, seconds
    # and nanoseconds from the same stat.
    atime_from = probe if (known(probe, "st_atime") is not None
                           or known(probe, "st_atime_ns") is not None) else after
    fields["st_atime"] = known(atime_from, "st_atime")
    fields["st_atime_ns"] = known(atime_from, "st_atime_ns")
    fields["mode_unconfirmed"] = known(after, "st_mode") is None and fields["st_mode"] is not None
    if verdict == MemberVersion.NEVER_VERSIONED:
        # No stat's mtime was compared with another: none is recorded.
        fields["st_mtime"] = fields["st_mtime_ns"] = None
    return SimpleNamespace(**fields), verdict


def verdict_without_confirmation(probe):
    """(stat, verdict) for a transfer no stat could follow (confirm_member_stat's rules).

    A probe with a version token is UNCONFIRMED: the token was never
    confirmed. Without one it is NEVER_VERSIONED, with the probe's
    attributes as conservatively as a confirmation without a mode: no
    mtime, and its mode without write bits (CTO-VERSION-EVIDENCE).
    """
    if _has_token(probe):
        return None, MemberVersion.UNCONFIRMED
    fields = {key: getattr(probe, key, None) if probe is not None else None
              for key in ("st_mode", "st_uid", "st_gid", "st_nlink", "st_ctime", "st_size",
                          "st_atime", "st_atime_ns")}
    fields["st_mtime"] = fields["st_mtime_ns"] = None
    fields["mode_unconfirmed"] = fields["st_mode"] is not None
    return SimpleNamespace(**fields), MemberVersion.NEVER_VERSIONED


def copy_member_attributes(fd: int, source) -> None:
    """Apply the member's stat to a private, fully downloaded inode.

    Mode bits required for Storage to read and maintain the backing file
    may differ from the mount's mode. Keep the inode daemon-owned, and
    never install or report source set-ID bits. An existing POSIX ACL (copied by a
    local backend or inherited from the cache parent) owns its group mask;
    changing the mask with chmod could grant access to other named entries.
    """
    local = os.fstat(fd)
    # source is the member's newest genuine stat (confirm_member_stat); a
    # version the transfer may not have read is the caller's to refetch,
    # never a reason to report local times.
    mode = getattr(source, "st_mode", None)
    if mode is not None and getattr(source, "mode_unconfirmed", False):
        # A mode no stat after the transfer reported keeps its denials but
        # grants no write: it fails closed, as a missing mode does (the
        # receiving file's own mode, an SFTP or staging default, says
        # nothing of the member's). A chmod through the mount grants write.
        mode &= ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    permissions = local.st_mode if mode is None else mode
    # Without a member mode, an import fails closed: the downloaded file's
    # own mode (a local member's copy, a transfer's or a streaming staging
    # file's default otherwise) is reported and enforced without write bits, so a read-only member file never becomes
    # writable. A chmod through the mount grants write explicitly.
    visible_mode = stat.S_IFREG | stat.S_IMODE(permissions)
    if mode is None:
        visible_mode &= ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    # Member IDs have no mapping to local groups or users. Only tighten
    # the staging inode's group and other permissions. An ACL mask keeps
    # the permissions the kernel chose when that ACL was installed.
    permissions &= local.st_mode | 0o700
    try:
        os.getxattr(fd, "system.posix_acl_access")
    except OSError as exc:
        if exc.errno not in (errno.ENODATA, errno.ENOTSUP):
            raise
    else:
        permissions = (permissions & ~0o070) | (local.st_mode & 0o070)
    cache_mode = (permissions & 0o777) | stat.S_IRUSR | stat.S_IWUSR
    if cache_mode != stat.S_IMODE(local.st_mode):
        os.fchmod(fd, cache_mode)

    times = tuple(_source_time_ns(source, field, getattr(local, field + "_ns"))
                  for field in ("st_atime", "st_mtime"))
    if times != (local.st_atime_ns, local.st_mtime_ns):
        os.utime(fd, ns=times)

    attrs = {}
    for key in _SOURCE_ATTR_FIELDS:
        value = getattr(source, key, None)
        if value is not None:
            attrs[key] = value
    attrs["st_mode"] = reported_mode(visible_mode)
    if "st_ctime" not in attrs and getattr(source, "st_mtime", None) is not None:
        attrs["st_ctime"] = source.st_mtime  # Same fallback as uncached SFTP stat.
    actual = os.fstat(fd)
    # The record identifies a new fill, which never needs legacy mtime
    # recovery. Use the actual stat after utime (filesystems can round).
    record = {"inode": _inode(actual), "basis": _source_attrs_basis(actual), "attrs": attrs}
    try:
        os.setxattr(fd, MEMBER_ATTRS_XATTR, json.dumps(record).encode())
    except OSError as exc:
        if exc.errno != errno.ENOTSUP:
            raise
        # A cache without user xattrs can still hold bytes and timestamps.
        # Only reserve write access when we can record the visible mode.
        os.fchmod(fd, (visible_mode & 0o777) | stat.S_IRUSR
                  if mode is None else (permissions & 0o777) | stat.S_IRUSR)


def record_native_attributes(fd: int) -> None:
    """Mark a file created through the mount as having no member fields.

    An empty record skips legacy mtime recovery (and its index read) at
    every stat. An existing record is kept: O_TRUNC keeps an inode's mode
    and owner, so it keeps their reported values too.
    """
    local = os.fstat(fd)
    record = {"inode": _inode(local), "basis": _source_attrs_basis(local), "attrs": {}}
    try:
        os.setxattr(fd, MEMBER_ATTRS_XATTR, json.dumps(record).encode(), os.XATTR_CREATE)
    except OSError as exc:
        if exc.errno not in (errno.EEXIST, errno.ENOTSUP):
            raise


def _read_record(path: Path) -> Optional[dict]:
    try:
        record = json.loads(os.getxattr(path, MEMBER_ATTRS_XATTR, follow_symlinks=False))
        if (isinstance(record, dict) and isinstance(record.get("basis"), dict)
                and isinstance(record.get("attrs"), dict)):
            return record
    except (OSError, ValueError, TypeError):
        pass
    return None


def _read_source_attrs(path: Path, local: os.stat_result) -> Optional[dict]:
    record = _read_record(path)
    if record is not None and record.get("inode") == _inode(local):
        return record
    return None


def stale_record_mode(path: Path, local: os.stat_result) -> Optional[int]:
    """The read-only mode a record for another inode still imposes, if any.

    A record whose inode no longer matches (its xattr was copied with the
    file, as a cache migrated with cp -a) describes nothing else, but if
    the mode it recorded withheld write, that mode is reported and
    enforced: failing closed costs a chmod, failing open grants write to a
    read-only member's file. A chmod through the mount rebinds the record
    to this inode without it (forget_cached_member_fields).
    """
    record = _read_record(path)
    if record is None or record.get("inode") == _inode(local):
        return None
    mode = record["attrs"].get("st_mode")
    if type(mode) is int and 0 <= mode < (1 << 32) and stat.S_ISREG(mode) and not mode & stat.S_IWUSR:
        return reported_mode(mode)
    return None


def imported_mode_denies(path: Path, local: os.stat_result, bit: int) -> bool:
    """Whether a recorded member mode withholds an owner bit (S_IRUSR or
    S_IWUSR) from this file, as stat reports it (cached_member_attributes,
    else stale_record_mode). The backing inode always has owner read and
    write for Storage itself; those synthesized bits never decide."""
    attrs = cached_member_attributes(path, local)
    mode = attrs.get("st_mode") if attrs is not None else stale_record_mode(path, local)
    return type(mode) is int and not mode & bit


def imported_mode_denies_write(path: Path, local: os.stat_result) -> bool:
    return imported_mode_denies(path, local, stat.S_IWUSR)


def cached_member_attributes(path: Path, local: os.stat_result) -> Optional[dict]:
    """Report source fields that still describe this cached inode.

    None allows legacy recovery. An empty dict is a new-format record
    without any field we can report. Native atime and mtime stay live.
    """
    record = _read_source_attrs(path, local)
    if record is None:
        return None
    attrs = {}
    for key in _SOURCE_ATTR_FIELDS:
        value = record["attrs"].get(key)
        if key == "st_ctime":
            if ((type(value) is float and math.isfinite(value))
                    or (type(value) is int and -(1 << 63) <= value < (1 << 63))):
                attrs[key] = value
        elif type(value) is int and 0 <= value < (1 << 32):
            if key != "st_mode":
                attrs[key] = value
            elif stat.S_ISREG(value):
                attrs[key] = reported_mode(value)
    basis = _source_attrs_basis(local)
    for key in ("st_mode", "st_uid", "st_gid"):
        if basis[key] != record["basis"].get(key):
            attrs.pop(key, None)
    if basis != record["basis"]:
        attrs.pop("st_ctime", None)
    return attrs


def restrict_cached_member_write(path: Path, local: os.stat_result) -> None:
    """Apply an aged member's owner-write denial to this imported inode.

    An explicit chmod retires the imported mode; leave that override on
    the inode in force. The physical mode remains usable by the daemon.
    """
    record = _read_source_attrs(path, local)
    if record is None:
        record = {"inode": _inode(local), "basis": _source_attrs_basis(local),
                  "attrs": {"st_mode": reported_mode(local.st_mode)}}
    mode = record["attrs"].get("st_mode")
    if (type(mode) is int and mode & stat.S_IWUSR
            and record["basis"].get("st_mode") == local.st_mode):
        record["attrs"]["st_mode"] = mode & ~stat.S_IWUSR
        os.setxattr(path, MEMBER_ATTRS_XATTR, json.dumps(record).encode(),
                    follow_symlinks=False)


def forget_cached_member_fields(path: Path, local: os.stat_result, *fields: str) -> None:
    """Retire synthetic fields explicitly replaced by chmod, chown or utime."""
    if not stat.S_ISREG(local.st_mode):
        return
    record = _read_source_attrs(path, local)
    stale_mode = stale_record_mode(path, local) if record is None else None
    if stale_mode is not None:
        # Rebind a copied record to this inode, keeping only what it still
        # imposed (the read-only mode) unless this change retires that too.
        attrs = {} if "st_mode" in fields else {"st_mode": stale_mode}
        record = {"inode": _inode(local), "basis": _source_attrs_basis(local), "attrs": attrs}
    elif record is None:
        if "st_mode" not in fields and (
                "st_mtime" not in fields or not read_member_baselines(path, cache_identity(local))):
            return
        # An explicit timestamp also takes precedence over a legacy
        # mtime, even when utime happened to keep the native mtime.
        record = {"inode": _inode(local), "basis": _source_attrs_basis(local), "attrs": {}}
    elif not any(key in record["attrs"] for key in fields):
        return
    for key in fields:
        record["attrs"].pop(key, None)
    data = json.dumps(record).encode()
    try:
        os.setxattr(path, MEMBER_ATTRS_XATTR, data, follow_symlinks=False)
    except PermissionError:
        # The backing file lacks owner write: an explicit chmod removed it,
        # or a legacy fill copied a read-only mode. The daemon owns the
        # inode, so lend itself write for the xattr and put the mode back
        # (callers hold the path's lock). Otherwise, the basis still masks
        # a mode or owner field while it differs.
        current = os.lstat(path)
        if current.st_uid != os.geteuid() or current.st_mode & stat.S_IWUSR:
            return
        os.chmod(path, stat.S_IMODE(current.st_mode) | stat.S_IWUSR)
        try:
            os.setxattr(path, MEMBER_ATTRS_XATTR, data, follow_symlinks=False)
        finally:
            os.chmod(path, stat.S_IMODE(current.st_mode))


def cache_identity(st: os.stat_result) -> tuple:
    return (st.st_ino, st.st_size, st.st_mtime_ns)


def read_member_baselines(path: Path, identity: tuple, *, exact_mtimes=None) -> dict:
    """Read revalidation evidence for the same inode, size and native mtime.

    The optional nanosecond field records only explicitly reported source
    nanoseconds. Older readers and xattrs use the original seconds pairs.
    """
    try:
        payload = json.loads(os.getxattr(path, MEMBER_STAT_XATTR, follow_symlinks=False))
        if tuple(payload.get("cache")) == identity:
            members = {str(name): (int(size), float(mtime))
                       for name, (size, mtime) in payload.get("members", {}).items()}
            exact = payload.get("mtime_ns", {})
            if exact_mtimes is not None and isinstance(exact, dict):
                exact_mtimes.update({name: ns for name, ns in exact.items()
                                     if name in members and type(ns) is int})
            return members
    except (OSError, ValueError, TypeError, AttributeError, OverflowError):
        pass
    return {}


def legacy_member_mtime(path: Path, local: os.stat_result,
                        load_row: Callable[[], Optional[FileMetadata]]) -> Optional[float]:
    """Recover a legacy fill's source mtime for reporting, without touching disk.

    A single member baseline can also come from an upload, which records
    the uploaded content checksum. Skip those, and any row or cached inode
    whose evidence is missing or disagrees. Never rewrite a CLEAN file's
    native mtime independently of its row and recovery xattr. The row is
    loaded only once the cache file's own baseline qualifies, so a file
    without one costs no index read.
    """
    times = read_member_baselines(path, cache_identity(local))
    if len(times) != 1:
        return None
    size, mtime = next(iter(times.values()))
    if size != local.st_size or not math.isfinite(mtime):
        return None
    row = load_row()
    if (row is None or row.sync_status != SyncStatus.CLEAN or row.checksum is not None
            or row.size != local.st_size or row.last_modified != local.st_mtime):
        return None
    return mtime


XATTR_CACHE_METADATA_NAME = "user.storage.metadata"
CACHE_METADATA_VERSION = 1
_INTERNAL_CACHE_NAMES = frozenset({
    ".storage.rsync-tmp",
    ".storage.control.sock",
    ".storage.db",
    ".storage.db-shm",
    ".storage.db-wal",
    ".storage.repairs",
})
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
                # A fill installs the member's atime, which says nothing of
                # this cache's use; its ctime is the fill (or a later
                # change), so the later of the two orders eviction.
                stat_atime=max(st.st_atime, st.st_ctime),
                metadata=metadata,
                issues=issues,
            )
