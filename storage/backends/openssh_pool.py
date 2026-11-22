"""Mount-scoped session admission for an owned OpenSSH connection."""

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
import logging
import os
import select
import threading
import time

import paramiko

from .base import current_capacity_cancel_probe, is_interactive_request
from .openssh import MasterLost, OpenSSHMaster, SessionRefused
from .ssh_pool import (
    BulkCapacityCancelled, BulkCapacityTimeout, CommandTimeout,
    DEFAULT_EXEC_CHANNELS, DEFAULT_SFTP_CHANNELS, MAX_COMMAND_OUTPUT,
    MemberUnavailable, SFTPLease, is_connection_error,
)


# Match the default Paramiko pool's total nominal session count.
_SESSION_LIMIT = DEFAULT_SFTP_CHANNELS + DEFAULT_EXEC_CHANNELS
_INTERACTIVE_BURST = 3
_POLL_INTERVAL = 0.05
logger = logging.getLogger(__name__)


class WatcherUnsupported(MemberUnavailable):
    """This generation cannot sustain the watcher and both request classes."""


@dataclass
class _Budget:
    generation: object
    limit: int
    interactive: int = 0
    bulk: int = 0
    watcher: int = 0
    interactive_turns: int = 0
    idle: object = None
    watcher_warned: bool = False


@dataclass
class _Reservation:
    generation: object
    client: object = None


class OpenSSHSessionPool:
    """Count every session against the same generation's proven capacity."""

    def __init__(self, config):
        self.name = config.host if config.port is None else f"{config.host}:{config.port}"
        self.connect_timeout = config.connect_timeout
        self.retry_interval = max(1.0, min(5.0, self.connect_timeout / 2.0))
        self.capacity_wait = config.bulk_capacity_wait_timeout or None
        self._master = OpenSSHMaster(
            config.username, config.host, config.port, config.key_filename,
            connect_timeout=self.connect_timeout,
        )
        self._cond = threading.Condition()
        self._closed = False
        self._initializing = False
        self._retry_at = 0.0
        self._budget = None
        self._held = {}
        self._watching = False
        self._waiters = {kind: deque() for kind in ("interactive", "bulk", "watcher")}

    def _root_available_locked(self, root):
        return root is None or (not root.closed and time.monotonic() >= root.manual_retry_at)

    def _check_locked(self, root, cancel):
        if self._closed or not self._root_available_locked(root):
            raise MemberUnavailable(f"SSH member on {self.name} is unavailable")
        if cancel is not None and cancel():
            raise BulkCapacityCancelled(f"SSH capacity wait cancelled on {self.name}")

    def _prepare(self):
        generation = self._master.generation()
        # Hold simultaneous session opens so the typed refusal gives a
        # ceiling. Only the first session needs a full SFTP negotiation.
        deadline = time.monotonic() + self.connect_timeout
        streams = []
        try:
            for _ in range(_SESSION_LIMIT):
                try:
                    stream = generation.open_sftp_stream(max(0.0, deadline - time.monotonic()))
                except SessionRefused as exc:
                    if not streams:
                        raise MemberUnavailable(f"OpenSSH {self.name} refused the first session") from exc
                    break
                streams.append(stream)
            first = streams[0]
            first.settimeout(max(0.0, deadline - time.monotonic()))
            warm_client = paramiko.SFTPClient(first)
            limit = len(streams)
            streams.pop(0)
        finally:
            for stream in streams:
                stream.close()
        if limit == 1:
            logger.warning("OpenSSH %s shares its sole session between foreground and bulk work",
                           self.name)
        return _Budget(generation, limit, idle=warm_client)

    def _get_budget(self, root, cancel):
        deadline = time.monotonic() + self.connect_timeout
        with self._cond:
            while True:
                self._check_locked(root, cancel)
                budget = self._budget
                if budget is not None and budget.generation.is_active():
                    return budget
                if time.monotonic() < self._retry_at:
                    raise MemberUnavailable(f"OpenSSH {self.name} is reconnecting")
                if not self._initializing:
                    self._initializing = True
                    self._budget = None
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MemberUnavailable(f"OpenSSH {self.name} startup is still in progress")
                self._cond.wait(min(remaining, _POLL_INTERVAL))
        try:
            if budget is not None and budget.idle is not None:
                budget.idle.close()
            budget = self._prepare()
        except BaseException:
            with self._cond:
                self._retry_at = time.monotonic() + 1.0
            raise
        else:
            with self._cond:
                closed = self._closed
                if not closed:
                    self._budget = budget
                    self._check_locked(root, cancel)
                    return budget
            budget.idle.close()
            raise MemberUnavailable(f"OpenSSH {self.name} is closed")
        finally:
            with self._cond:
                self._initializing = False
                self._cond.notify_all()

    def _can_admit(self, budget, kind):
        count = budget.interactive + budget.bulk + budget.watcher
        if count >= budget.limit:
            return False
        if kind == "watcher":
            return budget.watcher == 0 and budget.bulk < budget.limit - 1
        if kind == "bulk":
            watcher_slot = self._watching and budget.limit >= 3
            if budget.bulk >= max(1, budget.limit - 1 - watcher_slot):
                return False
        if budget.limit == 1:
            if kind == "bulk":
                return (not self._waiters["interactive"]
                        or budget.interactive_turns >= _INTERACTIVE_BURST)
            return not self._waiters["bulk"] or budget.interactive_turns < _INTERACTIVE_BURST
        return True

    @contextmanager
    def _permit(self, kind, *, root=None, cancel=None, sftp=False):
        wait = self.connect_timeout if kind != "bulk" else self.capacity_wait
        ticket = object()
        idle = None
        with self._cond:
            self._waiters[kind].append(ticket)
        try:
            budget = self._get_budget(root, cancel)
            deadline = None if wait is None else time.monotonic() + wait
            while True:
                with self._cond:
                    self._check_locked(root, cancel)
                    if self._budget is budget and budget.generation.is_active():
                        if kind == "watcher" and budget.limit < 3:
                            if not budget.watcher_warned:
                                logger.warning(
                                    "OpenSSH %s allows %d sessions. Remote watching is unsupported "
                                    "at this capacity; listings use pull revalidation.",
                                    self.name, budget.limit)
                                budget.watcher_warned = True
                            raise WatcherUnsupported(
                                f"OpenSSH {self.name} requires three sessions for watching")
                        if (self._waiters[kind][0] is ticket and self._can_admit(budget, kind)):
                            setattr(budget, kind, getattr(budget, kind) + 1)
                            if kind == "interactive" and self._waiters["bulk"]:
                                budget.interactive_turns += 1
                            elif kind != "watcher":
                                budget.interactive_turns = 0
                            used = budget.interactive + budget.bulk + budget.watcher
                            if sftp or used >= budget.limit:
                                idle, budget.idle = budget.idle, None
                            break
                        remaining = _POLL_INTERVAL if deadline is None else deadline - time.monotonic()
                        if remaining <= 0:
                            raise BulkCapacityTimeout(f"No OpenSSH session available on {self.name}")
                        self._cond.wait(min(remaining, _POLL_INTERVAL))
                        continue
                budget = self._get_budget(root, cancel)
        finally:
            with self._cond:
                self._waiters[kind].remove(ticket)
                self._cond.notify_all()
        reservation = _Reservation(budget.generation, client=idle if sftp else None)
        try:
            if idle is not None and not sftp:
                idle.close()
            yield reservation
        except MasterLost:
            # A command can lose its own session while the master still
            # serves siblings. Only dead masters warrant replacing the group.
            if not budget.generation.is_active():
                self._master.reset(budget.generation)
            raise
        finally:
            with self._cond:
                setattr(budget, kind, getattr(budget, kind) - 1)
                if (reservation.client is not None and budget.idle is None
                        and self._budget is budget and not self._closed
                        and budget.generation.is_active()):
                    budget.idle, reservation.client = reservation.client, None
                self._cond.notify_all()
            if reservation.client is not None:
                reservation.client.close()

    def _open(self, open_session, generation, *args):
        # A prior close can reach sshd slightly after the local mux endpoint
        # closes. Keep this reservation while the server processes that close.
        deadline = time.monotonic() + self.connect_timeout
        while True:
            try:
                return open_session(*args, max(0, deadline - time.monotonic()))
            except SessionRefused as exc:
                if not generation.is_active():
                    raise MasterLost(f"OpenSSH {self.name} lost during session open") from exc
                if time.monotonic() >= deadline:
                    raise MemberUnavailable(f"OpenSSH {self.name} refused an admitted session") from exc
                time.sleep(_POLL_INTERVAL)

    def acquire_sftp(self, timeout, *, root=None):
        owner = threading.get_ident()
        with self._cond:
            self._check_locked(root, None)
            held = self._held.get(owner)
            if held is not None:
                lease, _permit, _reservation = held
                if not self.is_current(lease.generation):
                    raise MemberUnavailable(f"OpenSSH {self.name} lost during a nested lease")
                lease._set_timeout(timeout)
                lease.depth += 1
                return lease
        interactive = is_interactive_request()
        permit = self._permit("interactive" if interactive else "bulk",
                              root=root, cancel=current_capacity_cancel_probe())
        reservation = permit.__enter__()
        generation = reservation.generation
        client, reservation.client = reservation.client, None
        try:
            if client is None:
                client = self._open(generation.open_sftp, generation)
            try:
                lease = SFTPLease(client, generation, interactive, owner)
                lease._set_timeout(timeout)
                lease.depth = 1
            except BaseException:
                client.close()
                raise
        except BaseException as exc:
            permit.__exit__(type(exc), exc, exc.__traceback__)
            raise
        with self._cond:
            self._held[owner] = lease, permit, reservation
        return lease

    def release(self, lease, exc=None, *, root=None):
        lease._restore_timeout()
        if lease.error is None and exc is not None and is_connection_error(exc):
            lease.error = exc
        lease.depth -= 1
        if lease.depth:
            return
        with self._cond:
            _, permit, reservation = self._held.pop(lease.owner)
        try:
            if lease.error is None and (exc is None or isinstance(exc, OSError)):
                reservation.client = lease.client
            else:
                lease.client.close()
        finally:
            permit.__exit__(type(exc) if exc is not None else None, exc,
                            exc.__traceback__ if exc is not None else None)

    def is_current(self, generation):
        with self._cond:
            return (self._budget is not None and self._budget.generation is generation
                    and generation.is_active())

    def nominal_bulk_capacity(self):
        with self._cond:
            return _SESSION_LIMIT - 1 - self._watching

    def bulk_capacity(self):
        with self._cond:
            if self._budget is None:
                return self.nominal_bulk_capacity()
            return max(1, self._budget.limit - 1 - self._watching)

    def available(self, root=None):
        with self._cond:
            return (not self._closed and time.monotonic() >= self._retry_at
                    and self._root_available_locked(root))

    def check_root(self, root):
        with self._cond:
            self._check_locked(root, None)

    def set_root_available(self, root, available):
        with self._cond:
            root.manual_retry_at = 0.0 if available else time.monotonic() + self.retry_interval
            self._cond.notify_all()

    def close_root(self, root):
        with self._cond:
            root.closed = True
            self._cond.notify_all()

    def set_watching(self, watching):
        with self._cond:
            self._watching = watching
            self._cond.notify_all()

    def close(self):
        with self._cond:
            self._closed = True
            budget, self._budget = self._budget, None
            self._cond.notify_all()
        self._master.close()
        if budget is not None and budget.idle is not None:
            budget.idle.close()

    def check(self, _reason):
        with self._cond:
            budget = self._budget
        if budget is not None and not budget.generation.is_active():
            self._master.reset(budget.generation)

    @contextmanager
    def command(self, cmd, stdin_fd, stdout_fd, stderr_fd, *, root=None, watcher_stop=None):
        kind = ("watcher" if watcher_stop is not None else
                "interactive" if is_interactive_request() else "bulk")
        cancel = watcher_stop if watcher_stop is not None else current_capacity_cancel_probe()
        with self._permit(kind, root=root, cancel=cancel) as reservation:
            generation = reservation.generation
            session = self._open(generation.open_command, generation, cmd,
                                 stdin_fd, stdout_fd, stderr_fd)
            try:
                yield session
            finally:
                session.close()

    def run_command(self, cmd, timeout, *, root=None):
        deadline = None

        def remaining():
            nonlocal deadline
            if deadline is None:
                deadline = time.monotonic() + timeout
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError
            return left

        try:
            return run_mux_command(partial(self.command, root=root), cmd, remaining)
        except TimeoutError as exc:
            if deadline is None:
                raise
            raise CommandTimeout(f"OpenSSH command on {self.name} timed out") from exc

    @contextmanager
    def rsync_options(self, *, root=None):
        with self._permit("bulk", root=root, cancel=current_capacity_cancel_probe()) as reservation:
            yield reservation.generation.attach_options()


def run_mux_command(open_command, command, remaining, *, output_limit=MAX_COMMAND_OUTPUT):
    """Drain a finite mux command without spooling arbitrarily large output."""
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, "rb", buffering=0) as out, os.fdopen(write_fd, "wb") as child, \
            open(os.devnull, "rb") as inp, open(os.devnull, "wb") as err:
        with open_command(command, inp.fileno(), child.fileno(), err.fileno()) as session:
            child.close()
            data = bytearray()
            poller = select.poll()
            poller.register(out, select.POLLIN)
            while True:
                if not poller.poll(1000 * remaining()):
                    raise TimeoutError
                chunk = out.read(min(65536, output_limit + 1 - len(data)))
                if not chunk:
                    return session.wait(remaining()), bytes(data)
                data.extend(chunk)
                if len(data) > output_limit:
                    raise ValueError("OpenSSH command exceeded output limit")
