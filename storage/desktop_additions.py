"""Bounded in-memory observations for the experimental Nautilus bridge."""

import os
import time
from collections import deque
from uuid import uuid4

from .control import ControlError


_MAX_PATH = 768
_MAX_NAME = 255
_MAX_EVENTS = 128
_MAX_VIEW_NAMES = 8192


def _native_path(value):
    if not isinstance(value, str) or len(value) > 2 * _MAX_PATH:
        raise ControlError("Invalid desktop listing path")
    try:
        path = bytes.fromhex(value)
    except ValueError as exc:
        raise ControlError("Invalid desktop listing path") from exc
    if (
        path.hex() != value or not path.startswith(b"/") or b"\x00" in path
        or (path != b"/" and any(p in (b"", b".", b"..") for p in path[1:].split(b"/")))
    ):
        raise ControlError("Invalid desktop listing path")
    return path


class DesktopAdditions:
    """Both publication and query run with the namespace listing lock held."""

    def __init__(self, mountpoint):
        self.mount = os.fsencode(mountpoint)
        _native_path(self.mount.hex())
        self.generation = uuid4().hex
        self.started_us = time.monotonic_ns() // 1000
        self.events = deque()
        self.through = 0
        self.dropped = 0
        self.dropped_us = -1
        self.view_revision = 0
        self.revisions = {}

    def published(self, path, limit):
        self.view_revision += 1
        self.revisions.pop(path, None)
        self.revisions[path] = self.view_revision
        if len(self.revisions) > limit:
            self.revisions.pop(next(iter(self.revisions)))

    def snapshot_request(self, arguments):
        fields = set(arguments) - {"mount_hex", "directory_hex"}
        if fields not in ({"revision"}, {"revision", "generation"}):
            raise ControlError("Invalid desktop snapshot request")
        if _native_path(arguments.get("mount_hex")) != self.mount:
            raise ControlError("Desktop listing mount does not match control socket")
        path = os.fsdecode(_native_path(arguments.get("directory_hex")))
        revision = arguments["revision"]
        if type(revision) is not int or revision < 0:
            raise ControlError("Invalid desktop snapshot revision")
        return path, revision

    def snapshot(self, path, after, generation, entries, attrs):
        revision = self.revisions.get(path, 0)
        reply = {"generation": self.generation, "revision": revision}
        if generation is not None and generation != self.generation:
            return {**reply, "status": "generation-changed"}
        if entries is None or not revision:
            return {**reply, "status": "pending"}
        if after == revision:
            return {**reply, "status": "unchanged"}
        if len(entries) > _MAX_VIEW_NAMES + 2:
            return {**reply, "status": "too-large"}

        rows = []
        for name in entries:
            if name in (".", ".."):
                continue
            raw = os.fsencode(name)
            if not 0 < len(raw) <= _MAX_NAME or b"/" in raw or b"\x00" in raw:
                return {**reply, "status": "too-large"}
            entry = next((member[name] for member in attrs.values() if name in member), None)
            if entry is None:
                base, _, member = name.rpartition(".")
                entry = attrs.get(member, {}).get(base, {})
            fingerprint = ",".join(str(entry.get(key, "")) for key in
                                   ("st_mode", "st_size", "st_mtime", "st_ctime", "st_uid",
                                    "st_gid", "st_nlink"))
            rows.append((raw.hex(), fingerprint))
        return {**reply, "status": "ok", "entries": rows}

    def record(self, directory, revision, additions):
        if not additions:
            return
        if len(additions) > _MAX_EVENTS:
            self._drop_batch()
            return
        try:
            path = os.fsencode(directory)
            names = [(name, os.fsencode(name)) for name in sorted(additions)]
        except UnicodeEncodeError:
            self._drop_batch()
            return
        if len(path) > _MAX_PATH or any(
            not 0 < len(encoded) <= _MAX_NAME or b"/" in encoded or b"\x00" in encoded
            for _, encoded in names
        ):
            self._drop_batch()
            return
        for name, encoded in names:
            self.through += 1
            if len(self.events) == _MAX_EVENTS:
                self.dropped, self.dropped_us, *_ = self.events.popleft()
            self.events.append((self.through, time.monotonic_ns() // 1000, path,
                                revision, name, encoded.hex()))

    def _drop_batch(self):
        self.through += 1
        self.events.clear()
        self.dropped = self.through
        self.dropped_us = time.monotonic_ns() // 1000

    def poll(self, arguments, current):
        """Return only additions still backed by the originating snapshot."""
        fields = set(arguments) - {"mount_hex", "directory_hex"}
        if fields not in ({"since_us"}, {"generation", "after"}):
            raise ControlError("Invalid desktop listing request")
        if _native_path(arguments.get("mount_hex")) != self.mount:
            raise ControlError("Desktop listing mount does not match control socket")
        path = _native_path(arguments.get("directory_hex"))
        status = "ok"
        events = ()
        if "since_us" in arguments:
            since = arguments["since_us"]
            if type(since) is not int or not 0 <= since <= time.monotonic_ns() // 1000:
                raise ControlError("Invalid desktop listing attachment time")
            if since < self.started_us:
                status = "generation-changed"
            elif since <= self.dropped_us:
                status = "gap"
            else:
                events = (event for event in self.events if event[1] >= since)
        else:
            after = arguments["after"]
            if type(after) is not int or after < 0:
                raise ControlError("Invalid desktop listing cursor")
            if arguments["generation"] != self.generation:
                status = "generation-changed"
            elif after < self.dropped or after > self.through:
                status = "gap"
            else:
                events = (event for event in self.events if event[0] > after)

        names = []
        for _, _, directory, revision, name, encoded in events:
            if directory != path:
                continue
            if not current(os.fsdecode(path), revision, name):
                status = "gap"
                names.clear()
                break
            names.append(encoded)
        return {"status": status, "generation": self.generation, "through": self.through,
                "added": names}
