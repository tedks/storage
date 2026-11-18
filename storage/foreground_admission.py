"""Bound active FUSE callbacks while protecting each request class.

`ForegroundAdmission` decides only when an admitted callback may start. It
never interrupts a running callback and never changes what it returns.

What the bound counts: callbacks admitted and not yet finished. At most
`cap` run at once. StorageFS admits every FUSE operation except init,
destroy, release, releasedir and flush (fs.py _ADMISSION_EXEMPT): those are
not counted and may run alongside `cap` admitted callbacks. Every unlink is
admitted, including the one libfuse runs inside the last close of a file
unlinked while open, so that close can wait for an ordinary slot. flush is exempt
only while StorageFS inherits fusepy's immediate-success no-op, which a test
pins; a flush that does work or can block must be reclassified, and fsync
is never exempt. So the cap bounds admitted work only: callback
invocations in flight (exempt ones and waiters) and native threads can
exceed it. Each class keeps its reserved slots
(`RESERVED`: two for demand I/O, one for metadata, one for everything else);
only `cap - 4` slots are shared. Waiting callbacks are not counted, and they
still occupy a native libfuse thread: libfuse 2's multithreaded loop has no
thread cap, so this does not bound process threads. Work outside FUSE
callbacks (sync workers, streaming prefetch, watchers) is not counted, and
neither is StorageFS's classification step before admission (for an open,
one local lstat of the cache path; for a read, one descriptor-table lookup
under the fd lock; never a member call).

Waits happen after libfuse's own locking. libfuse 2's high-level layer
takes its path tree-locks (write locks for rename, unlink and rmdir, read
locks along every path) before the Python callback, so a namespace change
queued here keeps its libfuse lock while it waits, and operations on paths
under it, closes included, wait inside libfuse until it is admitted and
finishes. That is a longer hold than uncapped (where it would run at once),
not a cycle: a callback waiting for admission holds no StorageFS lock, so
every StorageFS lock is held by a callback already running (admitted or
exempt) or by a background thread, and none of those waits for a slot.
The kernel also limits background requests (max_background, 12 by
default): queued readahead reads count
toward it and can delay a RELEASE there.

A callback waits for its slot before it runs, holding no lock while it
waits (classification's brief fd-lock lookup is released first), and it
never waits for a second slot, so the gate adds no lock-order edge: a slot
holder can wait on a path lock, never the reverse.

Only close() ends a wait early. An interrupted native request, or an
exited session, does not: one libfuse request can make several callbacks
(a rename onto an open target first renames the target to a hidden name;
mkdir and create look the new entry up afterwards), and cancelling a later
one would fail a request whose earlier step already happened. So an
interrupted caller, and an unmount, wait for queued callbacks to be
admitted and finish. libfuse joins its workers before destroy (which calls
close()), so a queued callback never runs against torn-down state. That
wait has no wall-time bound: a backlog drains in as many waves as the cap
needs, each as long as its slowest callback, and stalled member I/O
extends it. Storage's member-transport capacity waits keep their own
cancellation and deadlines (backends.base capacity_wait_cancellation).
"""

import threading
from collections import deque
from contextlib import contextmanager
from typing import Dict


class AdmissionClosed(InterruptedError):
    """The gate closed before the waiting callback entered."""


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
        """Hold one slot of kind for the body.

        Waits until this callback is first in its class's queue and a slot
        is free, or until close(), which raises AdmissionClosed. Nothing
        else ends the wait: not an interrupt of the native request, not the
        session's exit (CTO ruling STORAGE-180-ESC-I1-C).
        """
        if kind not in self._active:
            raise ValueError(f"unknown foreground class: {kind}")
        ticket = object()
        with self._cond:
            self._waiting[kind].append(ticket)
            # Wakes waiting_for() observers; admission itself never waits
            # on another class's queue growing.
            self._cond.notify_all()
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
