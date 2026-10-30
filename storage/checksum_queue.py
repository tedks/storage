"""Per-member background queues for the conflict view's member-side hashes.

One worker thread per member, so a member never runs more than one view
hash at a time (an SSH member runs it on one exec channel, a local member
reads one file at a time). Each member's queue is bounded and served newest
first: the directory listed last is the one being looked at, so its hashes
run before those of a directory listed earlier, and when the queue is full
the oldest waiting hash is dropped (its future cancelled). A dropped hash
leaves its name unverified; the next listing of its directory schedules it
again.

Workers are daemon threads, started on a member's first submit, which is
after FUSE's daemonizing fork (a submit in a forked child starts fresh
workers: threads do not survive a fork). Their priority is relative to the
member, not the CPU: they run in a fresh context, never a FUSE request's,
so their member calls do not count as interactive (see
backends.base.interactive_request), wait longer for a channel and cannot
take the one a FUSE request has reserved. The threads are not reniced: a
worker may be the caller that reconnects an SSH member, and paramiko's
transport thread, which then carries every request to that member, would
inherit the nice value.
"""

from __future__ import annotations

import collections
import concurrent.futures
import logging
import os
import threading
from typing import Callable, Deque, Dict, Tuple

DEFAULT_QUEUE_LIMIT = 256

_Item = Tuple[concurrent.futures.Future, Callable[[], object]]


class MemberHashQueues:
    def __init__(self, limit: int = DEFAULT_QUEUE_LIMIT, logger: logging.Logger = None):
        self.limit = max(1, int(limit))
        self.logger = logger or logging.getLogger(__name__)
        self._cond = threading.Condition()
        self._queues: Dict[str, Deque[_Item]] = {}
        self._workers: Dict[str, threading.Thread] = {}
        # The item each worker is running, so a forked child can finish
        # what the parent's (vanished) workers had started.
        self._running: Dict[str, concurrent.futures.Future] = {}
        self._pid = os.getpid()
        self._shut_down = False

    def submit(self, member: str, fn: Callable[[], object]) -> concurrent.futures.Future:
        """Queue fn on member's worker; RuntimeError after shutdown().

        The future of the oldest waiting item is cancelled when the queue
        overflows. Its done callbacks run in this thread, outside the
        queue's lock, so they may take the caller's locks only if the
        caller does not hold them in an order the callback would invert.
        """
        future: concurrent.futures.Future = concurrent.futures.Future()
        dropped = None
        orphaned = []
        started = []
        with self._cond:
            if self._shut_down:
                raise RuntimeError("checksum queues shut down")
            if os.getpid() != self._pid:
                # Forked: the parent's workers did not come along.
                self._pid = os.getpid()
                self._workers.clear()
                orphaned = [item[0] for queue in self._queues.values() for item in queue]
                started = list(self._running.values())
                self._queues.clear()
                self._running.clear()
            queue = self._queues.setdefault(member, collections.deque())
            queue.append((future, fn))
            if len(queue) > self.limit:
                # Items cancelled while waiting (invalidated, superseded)
                # go first; only then is a live one dropped.
                live = [item for item in queue if not item[0].cancelled()]
                if len(live) != len(queue):
                    queue.clear()
                    queue.extend(live)
            if len(queue) > self.limit:
                dropped = queue.popleft()[0]
            if member not in self._workers:
                worker = threading.Thread(
                    target=self._run, args=(member,), name=f"checksum-{member}", daemon=True,
                )
                self._workers[member] = worker
                worker.start()
            self._cond.notify_all()
        # Cancelled outside the queue's lock: their done callbacks take the
        # caller's locks.
        for stale in orphaned:
            stale.cancel()
        for stale in started:
            # Running, so not cancellable: finished with no result, which
            # the caller reads as "not hashed" (FS records nothing).
            try:
                stale.set_result(None)
            except concurrent.futures.InvalidStateError:
                pass
        if dropped is not None:
            dropped.cancel()
        return future

    def pending(self, member: str) -> int:
        with self._cond:
            return len(self._queues.get(member, ()))

    def shutdown(self):
        """Cancel every waiting item; workers exit after the item they run.

        A worker blocked in member I/O is not interrupted; it ends at that
        call's timeout. Workers are daemon threads, so none holds up exit.
        """
        with self._cond:
            self._shut_down = True
            waiting = [item[0] for queue in self._queues.values() for item in queue]
            self._queues.clear()
            self._cond.notify_all()
        for future in waiting:
            future.cancel()

    def _run(self, member: str):
        while True:
            with self._cond:
                while not self._shut_down and not self._queues.get(member):
                    self._cond.wait()
                if self._shut_down:
                    return
                future, fn = self._queues[member].pop()
                if not future.set_running_or_notify_cancel():
                    continue
                self._running[member] = future
            try:
                result = fn()
            except BaseException as exc:
                outcome = (future.set_exception, exc)
            else:
                outcome = (future.set_result, result)
            # Finished before it is untracked, outside the lock (its done
            # callbacks take the caller's locks): a fork in between leaves
            # the child a tracked future it finds already done.
            try:
                outcome[0](outcome[1])
            except concurrent.futures.InvalidStateError:
                pass  # already finished by a fork reset
            with self._cond:
                if self._running.get(member) is future:
                    del self._running[member]
