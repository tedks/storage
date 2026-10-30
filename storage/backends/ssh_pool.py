"""
One SSH transport per storage member, with a leased pool of channels on it.

A member holds one paramiko Transport for the life of the mount, the way a
native sftp mount holds one connection per host. paramiko's SFTPClient is
not safe for concurrent requests (its packet reader takes no lock), so each
SFTP channel is leased to one operation at a time. sshd allows about ten
session channels per connection (MaxSessions), counting SFTP and exec
alike, so the pool is small: by default four SFTP channels, one of them kept
for interactive requests (the FUSE thread and directory listings), and at
most two exec channels open at once.

Connects are single-flight. While the member is believed up, callers wait
for the one connect in progress; once a connect has failed the member is
down, and callers fail fast until the retry window passes, when one caller
probes and the rest keep failing fast. A server refusing a channel shrinks
the pool and never marks the member down. A broken transport (EOF, an
inactive transport, a connection-level OSError) is closed, which wakes every
operation still running on it; the next lease reconnects. A timed-out
request is ambiguous, so the transport is probed with a channel open before
deciding; a transport that answers probes while its SFTP requests keep
timing out (a hung remote filesystem) is marked down after a few such
stalls in a row.

Leases do not depend on which thread holds them beyond one call: libfuse's
worker threads give Python a fresh thread state per callback
(tests/test_fuse_thread_state.py), so nothing here is kept per thread
between operations. A lease is reentrant within a thread while it is held,
so an operation that calls another on the same member cannot deadlock a
pool of one.
"""

import enum
import errno
import heapq
import itertools
import logging
import os
import socket
import threading
import time
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import paramiko

from .base import is_interactive_request


class MemberUnavailable(ConnectionError):
    """The pool could not serve a request: the member is down, closed, its
    reconnect is in progress, or no channel came free in time.

    Raised before any request reaches the member, so it says nothing about a
    channel; it does mean the member could not be asked.
    """


_TRANSIENT_ERRNOS = frozenset({
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.ENETRESET,
    errno.ESHUTDOWN,
    errno.EHOSTDOWN,
    errno.EHOSTUNREACH,
    errno.ENETDOWN,
    errno.ENETUNREACH,
    errno.ETIMEDOUT,
    errno.EPIPE,
})

_TRANSIENT_MARKERS = (
    "socket is closed",
    "connection reset",
    "connection timed out",
    "broken pipe",
    "connection aborted",
    "eof",
)


def is_transient_oserror(exc: OSError) -> bool:
    """Return True when an OSError likely indicates connection loss.

    The message is consulted only when there is no errno: an error that
    names one (a missing local cache file, say) is about that, whatever
    its message happens to contain.
    """
    if exc.errno in _TRANSIENT_ERRNOS:
        return True
    if exc.errno is not None:
        return False
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_MARKERS)


class _Verdict(enum.IntEnum):
    """What an exception from an operation says about its channel."""
    HEALTHY = 0    # a path or permission error: the channel is fine
    DISCARD = 1    # the channel's state is unknown: close it
    PROBE = 2      # a timeout or channel error: close it, probe the transport
    TRANSPORT = 3  # the connection is gone: close the transport


def _verdict(exc: Optional[BaseException]) -> _Verdict:
    if exc is None or isinstance(exc, MemberUnavailable):
        return _Verdict.HEALTHY
    if isinstance(exc, socket.timeout):
        return _Verdict.PROBE
    if isinstance(exc, EOFError):
        return _Verdict.TRANSPORT
    if isinstance(exc, paramiko.SSHException):
        # paramiko's SFTP client reports a lost connection this way too
        # ("Server connection dropped"); the probe tells them apart.
        return _Verdict.PROBE
    if isinstance(exc, OSError):
        return _Verdict.TRANSPORT if is_transient_oserror(exc) else _Verdict.HEALTHY
    # Anything else (an interrupt, a bug) may have left a reply unread.
    return _Verdict.DISCARD


def is_connection_error(exc: BaseException) -> bool:
    """Return True when exc means the member could not be asked, rather than
    an answer about the path."""
    return isinstance(exc, MemberUnavailable) or _verdict(exc) >= _Verdict.PROBE


def _close_quietly(obj) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


class _Deadline:
    """One armed timer in the watchdog: fire() runs unless cancelled first."""

    __slots__ = ("fire", "cancelled")

    def __init__(self, fire: Callable[[], None]):
        self.fire = fire
        self.cancelled = False


class _Watchdog:
    """One thread that runs callbacks at deadlines, in place of a
    threading.Timer (a thread each) per bounded channel request.

    A callback runs on a short-lived thread of its own, started only when a
    deadline passes: closing a channel on a hung connection can itself block
    (paramiko's close writes to the transport), and that must not hold up
    the deadlines of other members. Deadlines that pass are the failure
    path, not the rate of requests. If no thread can be started for one, the
    callback runs on the watchdog thread itself: a late deadline beats a
    lost one, and a lost one would leave every later request unbounded.
    """

    # Cancelled entries stay in the heap until their deadline or a compaction;
    # compact once they outnumber the live ones by this much.
    _COMPACT_AT = 64

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.pid = os.getpid()
        self._logger = logger or logging.getLogger(__name__)
        self._cond = threading.Condition()
        self._heap: List[Tuple[float, int, _Deadline]] = []
        self._order = itertools.count()
        self._cancelled = 0
        self._thread: Optional[threading.Thread] = None

    def arm(self, timeout: float, fire: Callable[[], None]) -> _Deadline:
        """Run fire in timeout seconds unless cancelled. Raises RuntimeError
        when the watchdog thread cannot be started, as Timer.start would."""
        entry = _Deadline(fire)
        with self._cond:
            if self._thread is None or not self._thread.is_alive():
                thread = threading.Thread(
                    target=self._run, name="ssh-pool-watchdog", daemon=True)
                thread.start()
                self._thread = thread
            heapq.heappush(self._heap,
                           (time.monotonic() + timeout, next(self._order), entry))
            self._cond.notify()
        return entry

    def cancel(self, entry: _Deadline) -> None:
        with self._cond:
            if entry.cancelled:
                return
            entry.cancelled = True
            self._cancelled += 1
            if (self._cancelled >= self._COMPACT_AT
                    and self._cancelled * 2 >= len(self._heap)):
                self._heap = [item for item in self._heap if not item[2].cancelled]
                heapq.heapify(self._heap)
                self._cancelled = 0

    def _run(self) -> None:
        while True:
            with self._cond:
                while True:
                    while self._heap and self._heap[0][2].cancelled:
                        heapq.heappop(self._heap)
                        self._cancelled -= 1
                    if not self._heap:
                        self._cond.wait()
                        continue
                    wait = self._heap[0][0] - time.monotonic()
                    if wait <= 0:
                        entry = heapq.heappop(self._heap)[2]
                        entry.cancelled = True  # a late cancel() finds it spent
                        break
                    self._cond.wait(wait)
            self._expire(entry)

    def _expire(self, entry: _Deadline) -> None:
        try:
            threading.Thread(target=entry.fire, name="ssh-pool-expire",
                             daemon=True).start()
        except Exception:
            self._logger.warning(
                "no thread for an expired SSH request deadline; running it on the watchdog",
                exc_info=True)
            try:
                entry.fire()
            except Exception:
                self._logger.exception("an SSH request deadline callback failed")


_watchdog = _Watchdog()


def _current_watchdog() -> _Watchdog:
    """The process's watchdog, replaced after a fork.

    A forked child (libfuse daemonizing forks in C, which Python's own fork
    hooks do not see) has only the forking thread: the watchdog thread is
    gone and its lock may be held. Start afresh; the thread starts on the
    first request that needs it.
    """
    global _watchdog
    if _watchdog.pid != os.getpid():
        _watchdog = _Watchdog()
    return _watchdog


def run_bounded(chan, timeout: float, fn):
    """Run fn, closing chan if it has not returned after timeout seconds.

    paramiko waits without a bound for the reply to a channel request
    (invoke_subsystem, exec_command). Closing the channel wakes that wait;
    the call then raises socket.timeout.

    Expiry and completion are decided once, under one lock: a deadline that
    comes due after fn has returned never closes the channel, and one that
    came due first makes the call raise socket.timeout even if fn then
    returned.
    """
    decision = threading.Lock()
    state = ["pending"]  # -> "done" or "expired", once

    def expire():
        with decision:
            if state[0] != "pending":
                return
            state[0] = "expired"
        _close_quietly(chan)

    def settle() -> bool:
        """Mark fn finished; True if the deadline got there first."""
        with decision:
            if state[0] == "pending":
                state[0] = "done"
            return state[0] == "expired"

    watchdog = _current_watchdog()
    deadline = watchdog.arm(timeout, expire)
    try:
        result = fn()
    except Exception as exc:
        if settle():
            raise socket.timeout(
                f"no reply to a channel request within {timeout}s") from exc
        raise
    finally:
        watchdog.cancel(deadline)
    if settle():
        raise socket.timeout(f"no reply to a channel request within {timeout}s")
    return result


def _is_open_timeout(exc: BaseException) -> bool:
    """paramiko reports a channel open that got no reply as an SSHException."""
    return isinstance(exc, socket.timeout) or (
        isinstance(exc, paramiko.SSHException) and "timeout" in str(exc).lower())


class _State(enum.Enum):
    DISCONNECTED = "disconnected"  # no transport; the next lease connects
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DOWN = "down"                  # the last connect failed, or marked down


class SFTPLease:
    """One SFTP channel, held by one operation (and its nested calls)."""

    __slots__ = ("client", "generation", "interactive", "owner", "depth",
                 "verdict", "error", "stalled_since", "acquired_at", "_timeouts")

    def __init__(self, client, generation: int, interactive: bool, owner: int):
        self.client = client
        self.generation = generation
        self.interactive = interactive
        self.owner = owner
        self.depth = 0
        self.verdict = _Verdict.HEALTHY
        # The first connection error a call on this lease raised, kept so the
        # outermost release acts on it even if an outer caller swallowed it.
        self.error: Optional[BaseException] = None
        # When the request that timed out was sent, if error is a timeout.
        self.stalled_since: Optional[float] = None
        self.acquired_at = time.monotonic()
        self._timeouts: List[Optional[float]] = []

    def _set_timeout(self, timeout: float) -> None:
        chan = self.client.get_channel()
        self._timeouts.append(chan.gettimeout())
        chan.settimeout(timeout)

    def _request_start(self) -> float:
        """When the request that just timed out was sent, approximately.

        Called as the lease is released with the timeout: the channel gave up
        after its timeout without a reply, so the request went out about one
        timeout ago. An operation that closes a file after a timed-out read
        sends one more request, a timeout later, and that is the one dated.
        Never earlier than the lease, whose first request cannot predate it.
        """
        try:
            timeout = self.client.get_channel().gettimeout()
        except Exception:
            timeout = None
        started = time.monotonic() - (timeout or 0.0)
        return max(started, self.acquired_at)

    def _restore_timeout(self) -> None:
        previous = self._timeouts.pop() if self._timeouts else None
        try:
            self.client.get_channel().settimeout(previous)
        except Exception:
            pass


# Command output above this is refused: the commands run here print a digest
# or an attribute value.
MAX_COMMAND_OUTPUT = 1024 * 1024


class MemberConnection:
    """The transport and channel pool of one storage member."""

    def __init__(
        self,
        name: str,
        connect: Callable[[], paramiko.SSHClient],
        *,
        connect_timeout: float,
        retry_interval: float,
        bulk_wait: Optional[float] = None,
        sftp_channels: int = 4,
        interactive_reserve: int = 1,
        exec_channels: int = 2,
        stall_limit: int = 2,
        on_availability: Optional[Callable[[bool], None]] = None,
        sftp_client_factory: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            connect: returns a connected, authenticated SSHClient; called
                once per transport, from one thread at a time.
            connect_timeout: bound on channel opens and on how long an
                interactive request waits for a channel or a connect.
            retry_interval: how long the member stays down after a failed
                connect before one caller may try again.
            bulk_wait: how long a background request waits for a channel
                (connect_timeout by default). Nobody is waiting on these, and
                a channel can stay busy for a whole transfer.
            stall_limit: consecutive timed-out SFTP requests on one transport
                that still answers probes before the member is marked down.
                Exec commands do not count: a checksum of a large file may
                simply take longer than its timeout.
        """
        if sftp_channels < 1 or exec_channels < 1:
            raise ValueError("a member needs at least one SFTP and one exec channel")
        self.name = name
        self._connect = connect
        self.connect_timeout = float(connect_timeout)
        self.retry_interval = float(retry_interval)
        self.bulk_wait = max(self.connect_timeout,
                             float(bulk_wait) if bulk_wait is not None else 0.0)
        self._interactive_reserve = max(0, interactive_reserve)
        self._stall_limit = max(1, stall_limit)
        self._on_availability = on_availability
        self._sftp_client_factory = sftp_client_factory
        self.logger = logger or logging.getLogger(__name__)

        self._cond = threading.Condition()
        self._state = _State.DISCONNECTED
        self._closed = False
        # A connect in progress that started from DOWN: a reconnect probe,
        # which other callers do not wait for.
        self._probing = False
        self._client: Optional[paramiko.SSHClient] = None
        self._transport = None
        # Bumped when a transport is installed and when it is dropped, so a
        # lease from an older transport is recognised on return.
        self._generation = 0
        # Bumped by close() and mark_down(), so a connect they raced is
        # discarded rather than installed over them.
        self._epoch = 0
        self._next_retry_at = 0.0
        # SFTP requests that timed out on the current transport while it went
        # on answering probes, since the last request that succeeded; and
        # when the last one was counted (a stall counts only if its request
        # was sent after that, so a burst of concurrent timeouts is one).
        self._stalls = 0
        self._last_stall_at = float("-inf")
        # The transport generation the lost-reserve warning was last logged
        # for, so it is said once per transport, not per refused channel.
        self._reserve_warned_generation = -1

        self._sftp_channels = sftp_channels
        self._exec_channels = exec_channels
        if self._reserve_lost(sftp_channels):
            self.logger.warning(
                "SSH member %s: %d SFTP channel(s) configured, so none can be kept "
                "for interactive requests; a background transfer in progress "
                "delays them", name, sftp_channels)
        # Caps shrink when the server refuses a channel, and are reset for
        # each new transport.
        self._sftp_cap = sftp_channels
        self._exec_cap = exec_channels
        # Channels of the current generation: SFTP channels open or being
        # opened (idle ones included), bulk leases out, exec channels open.
        self._sftp_open = 0
        self._bulk_leased = 0
        self._exec_open = 0
        self._idle: List[SFTPLease] = []
        self._held: Dict[int, SFTPLease] = {}

        self.connects = 0  # successful connects, for tests and diagnostics

    # ------------------------------------------------------------------ #
    # Availability
    # ------------------------------------------------------------------ #
    def available(self) -> bool:
        """Transport connected, or the retry window has passed."""
        with self._cond:
            if self._closed:
                return False
            if self._state is _State.DOWN:
                return time.monotonic() >= self._next_retry_at
            if self._state is _State.CONNECTING:
                return not self._probing
            return True

    def is_current(self, generation: int) -> bool:
        """Whether generation is still the installed transport."""
        with self._cond:
            return generation == self._generation and self._state is _State.CONNECTED

    def mark_down(self) -> None:
        """Drop the transport and fail fast until the retry window passes."""
        with self._cond:
            self._epoch += 1
            closables = self._drop_locked()
            self._probing = False
            self._set_down_locked()
            self._cond.notify_all()
        self._close_all(closables)
        self._notify_availability(False)

    def clear_down(self) -> None:
        """Let the next lease connect at once."""
        with self._cond:
            if self._closed:
                return
            if self._state is _State.DOWN:
                self._state = _State.DISCONNECTED
            self._next_retry_at = 0.0
            self._cond.notify_all()
        self._notify_availability(True)

    def close(self) -> None:
        """Close the transport and every channel, for good.

        Waiters and later leases raise MemberUnavailable; nothing reconnects.
        """
        with self._cond:
            self._closed = True
            self._epoch += 1
            closables = self._drop_locked()
            self._probing = False
            self._set_down_locked()
            self._cond.notify_all()
        self._close_all(closables)
        self._notify_availability(False)

    def _notify_availability(self, available: bool) -> None:
        if self._on_availability is not None:
            self._on_availability(available)

    def _set_down_locked(self) -> None:
        self._state = _State.DOWN
        self._next_retry_at = max(self._next_retry_at,
                                  time.monotonic() + self.retry_interval)

    def _drop_locked(self) -> list:
        """Forget the current transport; return what to close outside the lock."""
        closables = [lease.client for lease in self._idle]
        if self._client is not None:
            closables.append(self._client)
        self._idle = []
        self._client = None
        self._transport = None
        self._generation += 1
        self._sftp_open = 0
        self._bulk_leased = 0
        self._exec_open = 0
        if self._state is _State.CONNECTED:
            self._state = _State.DISCONNECTED
        return closables

    @staticmethod
    def _close_all(closables) -> None:
        for obj in closables:
            _close_quietly(obj)

    def _deadline(self, interactive: bool) -> float:
        wait = self.connect_timeout if interactive else self.bulk_wait
        return time.monotonic() + wait

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #
    def _transport_locked(self, deadline: float):
        """Return (transport, generation), connecting if needed.

        Holds self._cond on entry and on return; releases it while
        connecting or waiting. Raises MemberUnavailable when the member is
        closed, down inside its retry window, or a reconnect probe is already
        running, or when another caller's connect does not finish by
        deadline; raises the connect's own error to the caller that ran it.
        """
        while True:
            if self._closed:
                raise MemberUnavailable(f"SSH member {self.name} is closed")
            state = self._state
            if state is _State.CONNECTED:
                if self._transport is not None and self._transport.is_active():
                    return self._transport, self._generation
                closables = self._drop_locked()
                self._cond.notify_all()
                self._cond.release()
                try:
                    self._close_all(closables)
                finally:
                    self._cond.acquire()
                continue
            if state is _State.CONNECTING:
                if self._probing:
                    raise MemberUnavailable(
                        f"SSH member {self.name}: reconnect probe already in progress")
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cond.wait(remaining):
                    if self._state is _State.CONNECTING:
                        raise MemberUnavailable(
                            f"SSH member {self.name}: connect still in progress")
                continue
            if state is _State.DOWN and time.monotonic() < self._next_retry_at:
                raise MemberUnavailable(
                    f"SSH member {self.name} is temporarily unavailable after a disconnect")
            self._probing = state is _State.DOWN
            self._state = _State.CONNECTING
            epoch = self._epoch
            self._cond.release()
            try:
                self._run_connect(epoch)
            finally:
                self._cond.acquire()

    def _run_connect(self, epoch: int) -> None:
        """Connect and install the transport; called without the lock."""
        try:
            client = self._connect()
            transport = client.get_transport()
            if transport is None or not transport.is_active():
                _close_quietly(client)
                raise paramiko.SSHException("connect returned no active transport")
        except BaseException:
            with self._cond:
                if self._epoch == epoch:
                    self._probing = False
                    self._set_down_locked()
                self._cond.notify_all()
            self._notify_availability(False)
            raise
        with self._cond:
            stale = self._epoch != epoch
            if not stale:
                self._client = client
                self._transport = transport
                self._generation += 1
                self._sftp_open = 0
                self._bulk_leased = 0
                self._exec_open = 0
                self._idle = []
                self._sftp_cap = self._sftp_channels
                self._exec_cap = self._exec_channels
                self._stalls = 0
                self._last_stall_at = float("-inf")
                self._state = _State.CONNECTED
                self._probing = False
                self._next_retry_at = 0.0
                self.connects += 1
            self._cond.notify_all()
        if stale:
            _close_quietly(client)
            raise MemberUnavailable(
                f"SSH member {self.name} was closed or marked down while connecting")
        self._notify_availability(True)

    def _fail_transport(self, generation: int, exc: BaseException) -> None:
        """Close the transport of generation, if it is still the current one."""
        with self._cond:
            if generation != self._generation or self._state is not _State.CONNECTED:
                return
            closables = self._drop_locked()
            self._cond.notify_all()
        self.logger.warning("SSH member %s: connection lost, reconnecting on next use: %r",
                            self.name, exc)
        self._close_all(closables)

    def _probe(self, generation: int, exc: BaseException,
               stalled_since: Optional[float] = None) -> None:
        """Decide whether a timeout or channel error means the transport is dead.

        stalled_since, for an SFTP request that timed out, is when that
        request was sent: if the transport answers, the request counts as a stall,
        and enough of them in a row mark the member down.
        """
        with self._cond:
            if generation != self._generation or self._state is not _State.CONNECTED:
                return
            transport = self._transport
        try:
            chan = transport.open_session(timeout=self.connect_timeout)
        except paramiko.ChannelException:
            chan = None  # refused, but the server answered
        except Exception as probe_exc:
            self._fail_transport(generation, probe_exc)
            return
        _close_quietly(chan)
        if stalled_since is None:
            return
        closables = None
        with self._cond:
            # The transport may have been replaced while the probe ran; a
            # stall on the old one says nothing about the new one.
            if generation != self._generation or self._state is not _State.CONNECTED:
                return
            if stalled_since <= self._last_stall_at:
                return  # part of a burst already counted
            self._stalls += 1
            self._last_stall_at = time.monotonic()
            stalls = self._stalls
            if stalls >= self._stall_limit:
                self._epoch += 1
                closables = self._drop_locked()
                self._set_down_locked()
                self._cond.notify_all()
        if closables is None:
            self.logger.debug("SSH member %s: transport answered a probe after %r",
                              self.name, exc)
            return
        self.logger.warning(
            "SSH member %s: %d requests in a row timed out while the connection "
            "answered (last: %r); marking it unavailable", self.name, stalls, exc)
        self._close_all(closables)
        self._notify_availability(False)

    def check(self, reason: BaseException) -> None:
        """Probe the current transport; close it if it does not answer.

        For failures seen outside the pool (an rsync over its own ssh timing
        out): the member's transport is judged by its own reply, not marked
        down on another connection's account.
        """
        with self._cond:
            generation = self._generation
        self._probe(generation, reason)

    def _after_error(self, generation: int, verdict: _Verdict, exc: BaseException,
                     stalled_since: Optional[float] = None) -> None:
        if verdict is _Verdict.TRANSPORT:
            self._fail_transport(generation, exc)
        elif verdict is _Verdict.PROBE:
            self._probe(generation, exc, stalled_since)

    def _shrink_locked(self, generation: int, kind: str, pending: int = 0) -> None:
        """A channel was refused: cap the pool at what the server allowed.

        pending counts the refused channel if it is still in the open counts.
        """
        if generation != self._generation:
            return
        allowed = max(1, self._sftp_open + self._exec_open - pending)
        sftp_cap = max(1, min(self._sftp_cap, allowed - 1))
        exec_cap = max(1, min(self._exec_cap, allowed - sftp_cap))
        if (sftp_cap, exec_cap) == (self._sftp_cap, self._exec_cap):
            return
        self.logger.warning(
            "SSH member %s: the server refused a %s channel with %d open; "
            "now using at most %d SFTP and %d exec channels",
            self.name, kind, allowed, sftp_cap, exec_cap)
        self._sftp_cap = sftp_cap
        self._exec_cap = exec_cap
        if (self._reserve_lost(sftp_cap)
                and self._reserve_warned_generation != generation):
            # _bulk_limit never goes below one, so background transfers can
            # hold the only channel and an interactive request waits for it.
            self._reserve_warned_generation = generation
            self.logger.warning(
                "SSH member %s: the server's session limit leaves %d SFTP "
                "channel(s), so none can be kept for interactive requests; "
                "a background transfer in progress delays them",
                self.name, sftp_cap)

    def _trim_idle_locked(self) -> list:
        """Close idle SFTP channels above the cap; return them to close."""
        closables = []
        while self._idle and self._sftp_open > self._sftp_cap:
            closables.append(self._idle.pop().client)
            self._sftp_open -= 1
        return closables

    # ------------------------------------------------------------------ #
    # SFTP leases
    # ------------------------------------------------------------------ #
    def _bulk_limit(self) -> int:
        return max(1, self._sftp_cap - self._interactive_reserve)

    def _reserve_lost(self, cap: int) -> bool:
        """Whether, with cap SFTP channels, background transfers can hold them
        all: the bulk limit never goes below one."""
        return self._interactive_reserve > 0 and max(1, cap - self._interactive_reserve) >= cap

    def _start_sftp(self, chan):
        """Start the SFTP subsystem on an open channel."""
        run_bounded(chan, self.connect_timeout, lambda: chan.invoke_subsystem("sftp"))
        chan.settimeout(self.connect_timeout)
        factory = self._sftp_client_factory or paramiko.SFTPClient
        return factory(chan)

    def acquire_sftp(self, timeout: float) -> SFTPLease:
        """Lease an SFTP channel for one operation; release it with release().

        timeout is the channel timeout for the operation. A thread that
        already holds a lease on this member gets the same lease back.
        """
        owner = threading.get_ident()
        with self._cond:
            held = self._held.get(owner)
            if held is not None:
                if held.generation != self._generation:
                    # The caller's channel went with its transport; so would
                    # this call. Fail it plainly; the outer call decides.
                    raise MemberUnavailable(
                        f"SSH member {self.name}: the connection under this "
                        f"operation was lost")
                held.depth += 1
                held._set_timeout(timeout)
                return held
        interactive = is_interactive_request()
        lease = self._acquire_new_sftp(interactive, owner)
        lease.depth = 1
        try:
            lease._set_timeout(timeout)
        except BaseException as exc:
            self.release(lease, exc)
            raise
        return lease

    def _acquire_new_sftp(self, interactive: bool, owner: int) -> SFTPLease:
        deadline = self._deadline(interactive)
        # Transports this acquisition may see installed before giving up: a
        # server that drops every new connection must not loop forever.
        connects_left = 2
        refusals_left = 2
        while True:
            with self._cond:
                generation_before = self._generation
                transport, generation = self._transport_locked(deadline)
                if generation != generation_before:
                    connects_left -= 1
                    if connects_left < 0:
                        raise MemberUnavailable(
                            f"SSH member {self.name}: the connection keeps dropping")
                lease = self._take_idle_locked(interactive, owner)
                if lease is not None:
                    return lease
                if not self._can_open_locked(interactive):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._cond.wait(remaining):
                        if not self._can_open_locked(interactive) \
                                and not self._idle_usable_locked(interactive):
                            raise MemberUnavailable(
                                f"SSH member {self.name}: no SFTP channel came free")
                    continue
                self._sftp_open += 1
                if not interactive:
                    self._bulk_leased += 1

            chan = None
            try:
                chan = transport.open_session(timeout=self.connect_timeout)
                client = self._start_sftp(chan)
            except BaseException as exc:
                _close_quietly(chan)
                refused = isinstance(exc, paramiko.ChannelException)
                with self._cond:
                    closables = []
                    if generation == self._generation:
                        self._sftp_open -= 1
                        if not interactive:
                            self._bulk_leased -= 1
                        if refused:
                            self._shrink_locked(generation, "SFTP")
                            closables = self._trim_idle_locked()
                    self._cond.notify_all()
                self._close_all(closables)
                if refused:
                    refusals_left -= 1
                    if refusals_left <= 0:
                        raise MemberUnavailable(
                            f"SSH member {self.name}: the server refused an SFTP channel") from exc
                    continue
                if not isinstance(exc, Exception):
                    raise
                if chan is not None:
                    # The channel opened, so the transport answered just now;
                    # a stalled subsystem start is settled by a probe, and
                    # only a transport the probe dropped is worth a reconnect.
                    if not is_connection_error(exc):
                        raise
                    self._probe(generation, exc)
                    if self.is_current(generation):
                        raise
                    continue
                if transport.is_active() and not _is_open_timeout(exc):
                    raise  # the transport is fine; the open was rejected
                # The open got no reply in time, or found the transport gone.
                self._fail_transport(generation, exc)
                continue

            with self._cond:
                stale = generation != self._generation
                if not stale:
                    lease = SFTPLease(client, generation, interactive, owner)
                    self._held[owner] = lease
            if stale:
                _close_quietly(client)
                continue
            return lease

    def _idle_usable_locked(self, interactive: bool) -> bool:
        return bool(self._idle) and (interactive or self._bulk_leased < self._bulk_limit())

    def _take_idle_locked(self, interactive: bool, owner: int) -> Optional[SFTPLease]:
        if not self._idle_usable_locked(interactive):
            return None
        lease = self._idle.pop()
        lease.interactive = interactive
        lease.owner = owner
        lease.verdict = _Verdict.HEALTHY
        lease.error = None
        lease.stalled_since = None
        lease.acquired_at = time.monotonic()
        if not interactive:
            self._bulk_leased += 1
        self._held[owner] = lease
        return lease

    def _can_open_locked(self, interactive: bool) -> bool:
        if self._sftp_open >= self._sftp_cap:
            return False
        return interactive or self._bulk_leased < self._bulk_limit()

    def release(self, lease: SFTPLease, exc: Optional[BaseException] = None) -> None:
        """Return a lease; exc is what the operation raised, if anything."""
        verdict = _verdict(exc)
        closables = []
        with self._cond:
            if verdict > lease.verdict:
                lease.verdict = verdict
            if verdict >= _Verdict.PROBE and lease.error is None:
                lease.error = exc
                if isinstance(exc, socket.timeout):
                    lease.stalled_since = lease._request_start()
            lease.depth -= 1
            lease._restore_timeout()
            if lease.depth > 0:
                return
            if self._held.get(lease.owner) is lease:
                del self._held[lease.owner]
            if lease.generation == self._generation:
                if not lease.interactive:
                    self._bulk_leased -= 1
                if lease.verdict is not _Verdict.HEALTHY or self._sftp_open > self._sftp_cap:
                    self._sftp_open -= 1
                    closables.append(lease.client)
                else:
                    self._idle.append(lease)
            else:
                closables.append(lease.client)
            if exc is None and lease.verdict is _Verdict.HEALTHY:
                self._stalls = 0
            final_verdict = lease.verdict
            final_error = lease.error
            self._cond.notify_all()
        self._close_all(closables)
        if final_verdict >= _Verdict.PROBE and final_error is not None:
            self._after_error(lease.generation, final_verdict, final_error,
                              lease.stalled_since)

    @contextmanager
    def sftp(self, timeout: float) -> Iterator[paramiko.SFTPClient]:
        """Lease an SFTP channel for the body of a with statement."""
        lease = self.acquire_sftp(timeout)
        try:
            yield lease.client
        except BaseException as exc:
            self.release(lease, exc)
            raise
        self.release(lease)

    # ------------------------------------------------------------------ #
    # Exec channels
    # ------------------------------------------------------------------ #
    @contextmanager
    def exec_channel(self, timeout: float) -> Iterator[paramiko.Channel]:
        """Open a session channel for one command; it is closed afterwards.

        A refused channel shrinks the pool and raises MemberUnavailable; the
        member stays available.
        """
        deadline = self._deadline(is_interactive_request())
        with self._cond:
            while True:
                transport, generation = self._transport_locked(deadline)
                if self._exec_open < self._exec_cap:
                    self._exec_open += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cond.wait(remaining):
                    if self._exec_open >= self._exec_cap:
                        raise MemberUnavailable(
                            f"SSH member {self.name}: no exec channel came free")
        chan = None
        body_error: Optional[BaseException] = None
        try:
            try:
                chan = transport.open_session(timeout=self.connect_timeout)
            except paramiko.ChannelException as exc:
                with self._cond:
                    self._shrink_locked(generation, "exec", pending=1)
                    closables = self._trim_idle_locked()
                self._close_all(closables)
                raise MemberUnavailable(
                    f"SSH member {self.name}: the server refused an exec channel") from exc
            except Exception as exc:
                if not transport.is_active() or _is_open_timeout(exc):
                    body_error = exc
                raise
            chan.settimeout(timeout)
            try:
                yield chan
            except BaseException as exc:
                body_error = exc
                raise
        finally:
            _close_quietly(chan)
            with self._cond:
                if generation == self._generation:
                    self._exec_open -= 1
                self._cond.notify_all()
            if body_error is not None:
                verdict = _verdict(body_error)
                if chan is None:
                    # The open itself got no reply or found the transport gone.
                    verdict = _Verdict.TRANSPORT
                if verdict >= _Verdict.PROBE:
                    self._after_error(generation, verdict, body_error)

    def run_command(self, command: str, timeout: float) -> Tuple[Optional[int], bytes]:
        """Run command on an exec channel; return (exit status or None, stdout).

        The whole command, its output and its exit status are bounded by one
        timeout, and output above MAX_COMMAND_OUTPUT is refused. Stderr is not
        read: callers redirect it.
        """
        end = time.monotonic() + timeout

        def remaining() -> float:
            left = end - time.monotonic()
            if left <= 0:
                raise socket.timeout(f"command did not finish within {timeout}s")
            return left

        with self.exec_channel(timeout) as chan:
            run_bounded(chan, remaining(), lambda: chan.exec_command(command))
            chunks = []
            size = 0
            while True:
                chan.settimeout(remaining())
                data = chan.recv(32768)
                if not data:
                    break
                size += len(data)
                if size > MAX_COMMAND_OUTPUT:
                    raise RuntimeError(
                        f"command output exceeds {MAX_COMMAND_OUTPUT} bytes")
                chunks.append(data)
            status = chan.exit_status if chan.status_event.wait(remaining()) else None
            return status, b"".join(chunks)

    # ------------------------------------------------------------------ #
    # Introspection for tests and diagnostics
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        with self._cond:
            return {
                "state": self._state.value,
                "closed": self._closed,
                "generation": self._generation,
                "sftp_open": self._sftp_open,
                "idle": len(self._idle),
                "bulk_leased": self._bulk_leased,
                "exec_open": self._exec_open,
                "sftp_cap": self._sftp_cap,
                "exec_cap": self._exec_cap,
                "stalls": self._stalls,
                "connects": self.connects,
            }
