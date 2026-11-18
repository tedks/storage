"""Bound active FUSE callbacks while protecting each request class.

`ForegroundAdmission` decides only when an admitted callback may start. It
never interrupts a running callback and never changes what it returns.

What the bound counts: callbacks that have entered `admit` and not yet left
it. At most `cap` run at once. Each class keeps its reserved slots
(`RESERVED`: two for demand I/O, one for metadata, one for everything else);
only `cap - 4` slots are shared. Waiting callbacks are not counted, and they
still occupy a native libfuse thread: libfuse 2's multithreaded loop has no
thread cap, so this does not bound process threads. Work outside FUSE
callbacks (sync workers, streaming prefetch, watchers) is not counted, and
neither is StorageFS's classification step before admission (for an open,
one local lstat of the cache path: no member call, no lock).

A callback takes its slot before it runs and therefore before it holds any
lock, and it never waits for a second slot, so the gate adds no lock-order
edge: a slot holder can wait on a path lock, never the reverse.
"""

import threading
from collections import deque
from contextlib import contextmanager
from typing import Dict


class AdmissionClosed(InterruptedError):
    """The gate closed while a callback was waiting to enter."""


MIN_CAP = 4
MAX_CAP = 128


def validate_cap(cap) -> int:
    """Return cap if it is an int in [MIN_CAP, MAX_CAP], else raise ValueError."""
    if not isinstance(cap, int) or isinstance(cap, bool) or not MIN_CAP <= cap <= MAX_CAP:
        raise ValueError(
            f"max_foreground_operations must be an integer from {MIN_CAP} to {MAX_CAP}"
            f" (two demand slots, one metadata and one ordinary are reserved); got {cap!r}"
        )
    return cap


class ForegroundAdmission:
    RESERVED: Dict[str, int] = {"metadata": 1, "demand": 2, "ordinary": 1}
    CLASSES = tuple(RESERVED)

    def __init__(self, cap: int):
        self.cap = validate_cap(cap)
        self.shared = cap - sum(self.RESERVED.values())
        self._cond = threading.Condition()
        self._active = {kind: 0 for kind in self.CLASSES}
        self._waiting = {kind: deque() for kind in self.CLASSES}
        self._closed = False
        self.peak_active = 0

    @property
    def active_count(self) -> int:
        with self._cond:
            return sum(self._active.values())

    def active(self, kind: str) -> int:
        with self._cond:
            return self._active[kind]

    def waiting_count(self, kind: str) -> int:
        with self._cond:
            return len(self._waiting[kind])

    def waiting_for(self, kind: str, count: int, timeout: float) -> bool:
        """Wait until at least count callbacks of kind are queued."""
        with self._cond:
            return self._cond.wait_for(
                lambda: len(self._waiting[kind]) >= count, timeout=max(0.0, timeout),
            )

    def close(self) -> None:
        """Cancel queued and future callbacks; running ones finish normally."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def _can_enter(self, kind: str) -> bool:
        if self._active[kind] < self.RESERVED[kind]:
            return True
        # Slots used beyond each class's reserve come from the shared pool,
        # so every class's reserve stays free for that class.
        over = sum(max(0, self._active[k] - self.RESERVED[k]) for k in self.CLASSES)
        return over < self.shared

    @contextmanager
    def admit(self, kind: str):
        if kind not in self._active:
            raise ValueError(f"unknown foreground class: {kind}")
        ticket = object()
        with self._cond:
            self._waiting[kind].append(ticket)
            try:
                while True:
                    if self._closed:
                        raise AdmissionClosed("foreground admission closed")
                    # FIFO within a class: only the head may enter.
                    if self._waiting[kind][0] is ticket and self._can_enter(kind):
                        break
                    self._cond.wait()
            except BaseException:
                self._waiting[kind].remove(ticket)
                self._cond.notify_all()
                raise
            self._waiting[kind].popleft()
            self._active[kind] += 1
            self.peak_active = max(self.peak_active, sum(self._active.values()))
            # The next in this class may be able to enter too.
            self._cond.notify_all()
        try:
            yield
        finally:
            with self._cond:
                self._active[kind] -= 1
                self._cond.notify_all()
