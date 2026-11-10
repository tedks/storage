"""Bound active FUSE callbacks while protecting each request class.

This gate runs in Python callbacks; it cannot bound libfuse's native threads.
Waiting callbacks still occupy native threads until admitted or cancelled.
"""

import threading
from collections import deque
from contextlib import contextmanager


class AdmissionClosed(InterruptedError):
    """The gate closed while a callback was waiting to enter."""


class ForegroundAdmission:
    CLASSES = ("metadata", "demand", "ordinary")

    def __init__(self, cap: int):
        if not isinstance(cap, int) or isinstance(cap, bool) or not 3 <= cap <= 128:
            raise ValueError("max foreground operations must be between 3 and 128")
        self.cap = cap
        self._cond = threading.Condition()
        self._active = {kind: 0 for kind in self.CLASSES}
        self._waiting = {kind: deque() for kind in self.CLASSES}
        self._closed = False
        self._local = threading.local()
        self.peak_active = 0

    @property
    def active_count(self) -> int:
        with self._cond:
            return sum(self._active.values())

    def current_kind(self):
        """Class held by this callback thread, or None outside admission."""
        return getattr(self._local, "kind", None)

    def waiting_for(self, kind: str, count: int, timeout: float) -> bool:
        """Observe queued demand without inspecting private callback state."""
        with self._cond:
            return self._cond.wait_for(
                lambda: len(self._waiting[kind]) >= count,
                timeout=max(0.0, timeout),
            )

    def waiting_count(self, kind: str) -> int:
        with self._cond:
            return len(self._waiting[kind])

    def close(self) -> None:
        """Cancel queued callbacks; active calls finish through their own I/O timeout."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def _can_enter(self, kind: str) -> bool:
        # One slot per class is reserved. All remaining slots are shared.
        # The sum of excess active counts cannot exceed cap - 3.
        if self._active[kind] == 0:
            return True
        excess = sum(max(0, count - 1) for count in self._active.values())
        return excess < self.cap - len(self.CLASSES)

    def reclassify_current(self, kind: str) -> bool:
        """Move this admitted callback to another class before slow work.

        The old slot is released before waiting for the new class. A direct
        method call outside FUSE admission has no slot to move.
        """
        if kind not in self._active:
            raise ValueError(f"unknown foreground class: {kind}")
        current = getattr(self._local, "kind", None)
        if current is None or current == kind:
            return False
        ticket = object()
        with self._cond:
            self._active[current] -= 1
            self._local.kind = None
            self._waiting[kind].append(ticket)
            self._cond.notify_all()
            try:
                while True:
                    if self._closed:
                        raise AdmissionClosed("foreground callback admission cancelled")
                    if self._waiting[kind][0] is ticket and self._can_enter(kind):
                        self._waiting[kind].popleft()
                        self._active[kind] += 1
                        self.peak_active = max(self.peak_active, sum(self._active.values()))
                        self._local.kind = kind
                        self._cond.notify_all()
                        return True
                    self._cond.wait()
            finally:
                if self._local.kind is None:
                    self._waiting[kind].remove(ticket)
                    self._cond.notify_all()

    @contextmanager
    def admit(self, kind: str, cancel: threading.Event = None):
        if kind not in self._active:
            raise ValueError(f"unknown foreground class: {kind}")
        ticket = object()
        entered = False
        with self._cond:
            self._waiting[kind].append(ticket)
            self._cond.notify_all()
            try:
                while True:
                    if self._closed or cancel is not None and cancel.is_set():
                        raise AdmissionClosed("foreground callback admission cancelled")
                    if self._waiting[kind][0] is ticket and self._can_enter(kind):
                        self._waiting[kind].popleft()
                        self._active[kind] += 1
                        self.peak_active = max(self.peak_active, sum(self._active.values()))
                        entered = True
                        self._cond.notify_all()
                        break
                    # Event has no notification hook; poll only cancellable waits.
                    self._cond.wait(0.05 if cancel is not None else None)
            finally:
                if not entered:
                    self._waiting[kind].remove(ticket)
                    self._cond.notify_all()
        try:
            previous = getattr(self._local, "kind", None)
            self._local.kind = kind
            yield
        finally:
            held = self._local.kind
            self._local.kind = previous
            with self._cond:
                if held is not None:
                    self._active[held] -= 1
                self._cond.notify_all()
