"""MemberHashQueues: one worker per member, newest first, bounded."""

import threading
import unittest

from storage.checksum_queue import MemberHashQueues


class TestMemberHashQueues(unittest.TestCase):
    def setUp(self):
        self.queues = MemberHashQueues(limit=2)
        self.addCleanup(self.queues.shutdown)

    def _block(self, member):
        """Occupy member's worker until the returned event is set."""
        started, release = threading.Event(), threading.Event()

        def hold():
            started.set()
            release.wait(5)

        future = self.queues.submit(member, hold)
        self.assertTrue(started.wait(5))
        return release, future

    def test_newest_first_and_oldest_dropped_when_full(self):
        release, blocker = self._block("m")
        order = []
        futures = [self.queues.submit("m", lambda i=i: order.append(i)) for i in range(4)]
        # Limit 2: the two oldest waiting items were dropped.
        self.assertTrue(futures[0].cancelled())
        self.assertTrue(futures[1].cancelled())
        release.set()
        for future in futures[2:]:
            future.result(timeout=5)
        self.assertEqual(order, [3, 2])
        blocker.result(timeout=5)

    def test_one_worker_per_member_and_members_independent(self):
        release, blocker = self._block("slow")
        # Another member is not held up by the busy one.
        self.assertEqual(self.queues.submit("fast", lambda: 7).result(timeout=5), 7)
        self.assertEqual(self.queues.pending("slow"), 0)
        waiting = self.queues.submit("slow", lambda: 8)
        self.assertEqual(self.queues.pending("slow"), 1)
        self.assertFalse(waiting.done())
        release.set()
        self.assertEqual(waiting.result(timeout=5), 8)
        blocker.result(timeout=5)

    def test_exception_reaches_the_future(self):
        def boom():
            raise ValueError("x")

        with self.assertRaises(ValueError):
            self.queues.submit("m", boom).result(timeout=5)
        # The worker survives it.
        self.assertEqual(self.queues.submit("m", lambda: 1).result(timeout=5), 1)

    def test_shutdown_cancels_waiting_and_refuses_new(self):
        release, blocker = self._block("m")
        waiting = self.queues.submit("m", lambda: 1)
        self.queues.shutdown()
        self.assertTrue(waiting.cancelled())
        with self.assertRaises(RuntimeError):
            self.queues.submit("m", lambda: 1)
        release.set()
        blocker.result(timeout=5)

    def test_workers_start_lazily(self):
        queues = MemberHashQueues()
        self.assertEqual(queues._workers, {})
        queues.submit("m", lambda: None).result(timeout=5)
        self.assertEqual(list(queues._workers), ["m"])
        queues.shutdown()


if __name__ == "__main__":
    unittest.main()
