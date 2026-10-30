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
operation is ambiguous, so the transport is probed with a channel open
before deciding.

Leases do not depend on which thread holds them beyond one call: libfuse's
worker threads give Python a fresh thread state per callback
(tests/test_fuse_thread_state.py), so nothing here is kept per thread
between operations. A lease is reentrant within a thread while it is held,
so an operation that calls another on the same member cannot deadlock a
pool of one.
"""

import enum
import errno
import logging
import socket
import threading
import time
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import paramiko

from .base import is_interactive_request


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
    """Return True when an OSError likely indicates connection loss."""
    if exc.errno in _TRANSIENT_ERRNOS:
        return True
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_MARKERS)


class _Verdict(enum.IntEnum):
    """What an exception from an operation says about its channel."""
    HEALTHY = 0    # a path or permission error: the channel is fine
    DISCARD = 1    # the channel's state is unknown: close it
    PROBE = 2      # a timeout or channel error: close it, probe the transport
    TRANSPORT = 3  # the connection is gone: close the transport


def _verdict(exc: Optional[BaseException]) -> _Verdict:
    if exc is None:
        return _Verdict.HEALTHY
    if isinstance(exc, socket.timeout):
        return _Verdict.PROBE
    if isinstance(exc, EOFError):
        return _Verdict.TRANSPORT
    if isinstance(exc, paramiko.SSHException):
        return _Verdict.PROBE
    if isinstance(exc, OSError):
        return _Verdict.TRANSPORT if is_transient_oserror(exc) else _Verdict.HEALTHY
    # Anything else (an interrupt, a bug) may have left a reply unread.
    return _Verdict.DISCARD


def is_connection_error(exc: BaseException) -> bool:
    """Return True when exc came from the connection rather than the path."""
    return _verdict(exc) in (_Verdict.PROBE, _Verdict.TRANSPORT)


def _close_quietly(obj) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


def run_bounded(chan, timeout: float, fn):
    """Run fn, closing chan if it has not returned after timeout seconds.

    paramiko waits without a bound for the reply to a channel request
    (invoke_subsystem, exec_command). Closing the channel wakes that wait;
    the call then raises socket.timeout.
    """
    fired = threading.Event()

    def expire():
        fired.set()
        _close_quietly(chan)

    timer = threading.Timer(timeout, expire)
    timer.daemon = True
    timer.start()
    try:
        result = fn()
    except Exception as exc:
        if fired.is_set():
            raise socket.timeout(
                f"no reply to a channel request within {timeout}s") from exc
        raise
    finally:
        timer.cancel()
    if fired.is_set():
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
    DOWN = "down"                  # the last connect failed


class SFTPLease:
    """One SFTP channel, held by one operation (and its nested calls)."""

    __slots__ = ("client", "generation", "interactive", "owner", "depth",
                 "verdict", "_timeouts")

    def __init__(self, client, generation: int, interactive: bool, owner: int):
        self.client = client
        self.generation = generation
        self.interactive = interactive
        self.owner = owner
        self.depth = 0
        self.verdict = _Verdict.HEALTHY
        self._timeouts: List[Optional[float]] = []

    def _set_timeout(self, timeout: float) -> None:
        chan = self.client.get_channel()
        self._timeouts.append(chan.gettimeout())
        chan.settimeout(timeout)

    def _restore_timeout(self) -> None:
        previous = self._timeouts.pop() if self._timeouts else None
        try:
            self.client.get_channel().settimeout(previous)
        except Exception:
            pass


class MemberConnection:
    """The transport and channel pool of one storage member."""

    def __init__(
        self,
        name: str,
        connect: Callable[[], paramiko.SSHClient],
        *,
        connect_timeout: float,
        retry_interval: float,
        sftp_channels: int = 4,
        interactive_reserve: int = 1,
        exec_channels: int = 2,
        on_availability: Optional[Callable[[bool], None]] = None,
        sftp_client_factory: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        if sftp_channels < 1 or exec_channels < 1:
            raise ValueError("a member needs at least one SFTP and one exec channel")
        self.name = name
        self._connect = connect
        self.connect_timeout = float(connect_timeout)
        self.retry_interval = float(retry_interval)
        self._interactive_reserve = max(0, interactive_reserve)
        self._on_availability = on_availability
        self._sftp_client_factory = sftp_client_factory
        self.logger = logger or logging.getLogger(__name__)

        self._cond = threading.Condition()
        self._state = _State.DISCONNECTED
        # A connect in progress that started from DOWN: a reconnect probe,
        # which other callers do not wait for.
        self._probing = False
        self._client: Optional[paramiko.SSHClient] = None
        self._transport = None
        # Bumped when a transport is installed and when it is dropped, so a
        # lease from an older transport is recognised on return.
        self._generation = 0
        # Bumped by close(), so a connect that close() raced is discarded.
        self._epoch = 0
        self._next_retry_at = 0.0

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
            if self._state is _State.DOWN:
                return time.monotonic() >= self._next_retry_at
            if self._state is _State.CONNECTING:
                return not self._probing
            return True

    def mark_down(self) -> None:
        """Drop the transport and fail fast until the retry window passes."""
        with self._cond:
            closables = self._drop_locked()
            self._set_down_locked()
            self._cond.notify_all()
        self._close_all(closables)
        self._notify_availability(False)

    def clear_down(self) -> None:
        """Let the next lease connect at once."""
        with self._cond:
            if self._state is _State.DOWN:
                self._state = _State.DISCONNECTED
            self._next_retry_at = 0.0
            self._cond.notify_all()
        self._notify_availability(True)

    def close(self) -> None:
        """Close the transport and every channel; wake every waiter.

        Waiters raise ConnectionError, and the member stays down for the retry
        window; a lease after that reconnects.
        """
        with self._cond:
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

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #
    def _transport_locked(self, deadline: float):
        """Return (transport, generation), connecting if needed.

        Holds self._cond on entry and on return; releases it while
        connecting or waiting. Raises ConnectionError when the member is down
        inside its retry window, when a reconnect probe is already running,
        or when another caller's connect does not finish by deadline; raises
        the connect's own error to the caller that ran it.
        """
        while True:
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
                    raise ConnectionError(
                        f"SSH member {self.name}: reconnect probe already in progress")
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cond.wait(remaining):
                    if self._state is _State.CONNECTING:
                        raise ConnectionError(
                            f"SSH member {self.name}: connect still in progress "
                            f"after {self.connect_timeout}s")
                continue
            if state is _State.DOWN and time.monotonic() < self._next_retry_at:
                raise ConnectionError(
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
                self._state = _State.CONNECTED
                self._probing = False
                self._next_retry_at = 0.0
                self.connects += 1
            self._cond.notify_all()
        if stale:
            _close_quietly(client)
            raise ConnectionError(f"SSH member {self.name} was closed while connecting")
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

    def _probe(self, generation: int, exc: BaseException) -> None:
        """Decide whether a timeout or channel error means the transport is dead."""
        with self._cond:
            if generation != self._generation or self._state is not _State.CONNECTED:
                return
            transport = self._transport
        try:
            chan = transport.open_session(timeout=self.connect_timeout)
        except paramiko.ChannelException:
            return  # the server answered
        except Exception as probe_exc:
            self._fail_transport(generation, probe_exc)
            return
        _close_quietly(chan)
        self.logger.debug("SSH member %s: transport answered a probe after %r",
                          self.name, exc)

    def _after_error(self, generation: int, verdict: _Verdict, exc: BaseException) -> None:
        if verdict is _Verdict.TRANSPORT:
            self._fail_transport(generation, exc)
        elif verdict is _Verdict.PROBE:
            self._probe(generation, exc)

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

    def _sftp_client(self, chan):
        factory = self._sftp_client_factory or paramiko.SFTPClient
        return factory(chan)

    def _open_sftp(self, transport):
        chan = transport.open_session(timeout=self.connect_timeout)
        try:
            run_bounded(chan, self.connect_timeout, lambda: chan.invoke_subsystem("sftp"))
            chan.settimeout(self.connect_timeout)
            return self._sftp_client(chan)
        except BaseException:
            _close_quietly(chan)
            raise

    def acquire_sftp(self, timeout: float) -> SFTPLease:
        """Lease an SFTP channel for one operation; release it with release().

        timeout is the channel timeout for the operation. A thread that
        already holds a lease on this member gets the same lease back.
        """
        owner = threading.get_ident()
        with self._cond:
            held = self._held.get(owner)
            if held is not None:
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
        deadline = time.monotonic() + self.connect_timeout
        connects_left = 2
        refusals_left = 2
        while True:
            with self._cond:
                generation_before = self._generation
                transport, generation = self._transport_locked(deadline)
                if generation != generation_before:
                    connects_left -= 1
                lease = self._take_idle_locked(interactive, owner)
                if lease is not None:
                    return lease
                if not self._can_open_locked(interactive):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._cond.wait(remaining):
                        if not self._can_open_locked(interactive) and not self._idle_usable_locked(interactive):
                            raise ConnectionError(
                                f"SSH member {self.name}: no SFTP channel free "
                                f"within {self.connect_timeout}s")
                    continue
                self._sftp_open += 1
                if not interactive:
                    self._bulk_leased += 1

            try:
                client = self._open_sftp(transport)
            except BaseException as exc:
                with self._cond:
                    if generation == self._generation:
                        self._sftp_open -= 1
                        if not interactive:
                            self._bulk_leased -= 1
                        if isinstance(exc, paramiko.ChannelException):
                            self._shrink_locked(generation, "SFTP")
                            closables = self._trim_idle_locked()
                        else:
                            closables = []
                    else:
                        closables = []
                    self._cond.notify_all()
                self._close_all(closables)
                if isinstance(exc, paramiko.ChannelException):
                    refusals_left -= 1
                    if refusals_left <= 0:
                        raise ConnectionError(
                            f"SSH member {self.name}: the server refused an SFTP channel") from exc
                    continue
                if not isinstance(exc, Exception):
                    raise
                if transport.is_active() and not _is_open_timeout(exc):
                    raise  # the transport is fine; the request was rejected
                self._fail_transport(generation, exc)
                if connects_left <= 0:
                    raise
                continue

            with self._cond:
                if generation != self._generation:
                    stale = True
                else:
                    stale = False
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
            final_verdict = lease.verdict
            self._cond.notify_all()
        self._close_all(closables)
        if exc is not None and final_verdict >= _Verdict.PROBE:
            self._after_error(lease.generation, final_verdict, exc)

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

        A refused channel shrinks the pool and raises ConnectionError; the
        member stays available.
        """
        deadline = time.monotonic() + self.connect_timeout
        with self._cond:
            while True:
                transport, generation = self._transport_locked(deadline)
                if self._exec_open < self._exec_cap:
                    self._exec_open += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cond.wait(remaining):
                    if self._exec_open >= self._exec_cap:
                        raise ConnectionError(
                            f"SSH member {self.name}: no exec channel free "
                            f"within {self.connect_timeout}s")
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
                raise ConnectionError(
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

        Every request and read is bounded by timeout. Stderr is not read:
        callers redirect it.
        """
        with self.exec_channel(timeout) as chan:
            run_bounded(chan, timeout, lambda: chan.exec_command(command))
            chunks = []
            while True:
                data = chan.recv(32768)
                if not data:
                    break
                chunks.append(data)
            status = chan.exit_status if chan.status_event.wait(timeout) else None
            return status, b"".join(chunks)

    # ------------------------------------------------------------------ #
    # Introspection for tests and diagnostics
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        with self._cond:
            return {
                "state": self._state.value,
                "generation": self._generation,
                "sftp_open": self._sftp_open,
                "idle": len(self._idle),
                "bulk_leased": self._bulk_leased,
                "exec_open": self._exec_open,
                "sftp_cap": self._sftp_cap,
                "exec_cap": self._exec_cap,
                "connects": self.connects,
            }
