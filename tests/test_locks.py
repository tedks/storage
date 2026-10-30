"""NamespaceLock and PathLocks (storage/locks.py)."""

import threading
import unittest

from storage.locks import NamespaceLock, PathLocks

WAIT = 5.0
BLOCKED = 0.2


def passing_through(factory):
    """A callable that enters and leaves factory()'s context."""
    def run():
        with factory():
            pass
    return run


def start(fn):
    done = threading.Event()

    def run():
        fn()
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done


class TestNamespaceLock(unittest.TestCase):
    def setUp(self):
        self.lock = NamespaceLock()

    def test_readers_share(self):
        with self.lock.shared():
            entered = start(passing_through(self.lock.shared))
            self.assertTrue(entered.wait(WAIT))

    def test_writer_waits_for_readers_and_excludes_them(self):
        release = threading.Event()
        holding = threading.Event()

        def reader():
            with self.lock.shared():
                holding.set()
                release.wait(WAIT)

        reader_done = start(reader)
        self.assertTrue(holding.wait(WAIT))
        writer_in = threading.Event()
        writer_release = threading.Event()

        def writer():
            with self.lock.exclusive():
                writer_in.set()
                writer_release.wait(WAIT)

        writer_done = start(writer)
        self.assertFalse(writer_in.wait(BLOCKED))
        # A new reader queues behind the waiting writer.
        late_reader = start(passing_through(self.lock.shared))
        self.assertFalse(late_reader.wait(BLOCKED))
        release.set()
        self.assertTrue(reader_done.wait(WAIT))
        self.assertTrue(writer_in.wait(WAIT))
        self.assertFalse(late_reader.wait(BLOCKED))
        with self.lock.try_shared() as got:
            self.assertFalse(got)
        writer_release.set()
        self.assertTrue(writer_done.wait(WAIT))
        self.assertTrue(late_reader.wait(WAIT))

    def test_reader_reenters_past_a_waiting_writer(self):
        holding = threading.Event()
        reentered = threading.Event()
        proceed = threading.Event()

        def reader():
            with self.lock.shared():
                holding.set()
                proceed.wait(WAIT)
                # A writer is waiting now; re-entering must not deadlock.
                with self.lock.shared():
                    with self.lock.try_shared() as got:
                        if got:
                            reentered.set()

        reader_done = start(reader)
        self.assertTrue(holding.wait(WAIT))
        writer_done = start(passing_through(self.lock.exclusive))
        self.assertFalse(writer_done.wait(BLOCKED))
        proceed.set()
        self.assertTrue(reentered.wait(WAIT))
        self.assertTrue(reader_done.wait(WAIT))
        self.assertTrue(writer_done.wait(WAIT))

    def test_writer_reenters_both_ways(self):
        with self.lock.exclusive():
            with self.lock.exclusive():
                with self.lock.shared():
                    pass
            with self.lock.try_shared() as got:
                self.assertTrue(got)
        with self.lock.try_shared() as got:
            self.assertTrue(got)

    def test_shared_holder_cannot_upgrade(self):
        with self.lock.shared():
            with self.assertRaises(RuntimeError):
                with self.lock.exclusive():
                    pass
        # Still usable afterwards.
        with self.lock.exclusive():
            pass


    def test_try_shared(self):
        with self.lock.try_shared() as got:
            self.assertTrue(got)
        holding = threading.Event()
        release = threading.Event()

        def writer():
            with self.lock.exclusive():
                holding.set()
                release.wait(WAIT)

        done = start(writer)
        self.assertTrue(holding.wait(WAIT))
        with self.lock.try_shared() as got:
            self.assertFalse(got)
        release.set()
        self.assertTrue(done.wait(WAIT))
        with self.lock.try_shared() as got:
            self.assertTrue(got)


class TestPathLocks(unittest.TestCase):
    def setUp(self):
        self.locks = PathLocks()

    def test_distinct_paths_do_not_block_and_entries_are_released(self):
        release_b = threading.Event()
        holding_b = threading.Event()

        def hold_b():
            with self.locks.hold("/b"):
                holding_b.set()
                release_b.wait(WAIT)

        with self.locks.hold("/a"):
            b_done = start(hold_b)
            self.assertTrue(holding_b.wait(WAIT))
            self.assertEqual(2, len(self.locks))
        # Only /a left the table.
        self.assertEqual(1, len(self.locks))
        release_b.set()
        self.assertTrue(b_done.wait(WAIT))
        self.assertEqual(0, len(self.locks))

    def test_same_path_blocks_until_released(self):
        release = threading.Event()
        holding = threading.Event()

        def holder():
            with self.locks.hold("/a"):
                holding.set()
                release.wait(WAIT)

        start(holder)
        self.assertTrue(holding.wait(WAIT))
        waiter = start(passing_through(lambda: self.locks.hold("/a")))
        self.assertFalse(waiter.wait(BLOCKED))
        with self.locks.try_hold("/a") as got:
            self.assertFalse(got)
        release.set()
        self.assertTrue(waiter.wait(WAIT))

    def test_reentrant_and_refcounted(self):
        with self.locks.hold("/a"):
            with self.locks.hold("/a", "/a"):
                with self.locks.try_hold("/a") as got:
                    self.assertTrue(got)
            self.assertEqual(1, len(self.locks))
        self.assertEqual(0, len(self.locks))
        with self.locks.try_hold("/a") as got:
            self.assertTrue(got)
        self.assertEqual(0, len(self.locks))

    def test_two_paths_in_either_order_do_not_deadlock(self):
        barrier = threading.Barrier(2)
        done = []

        def worker(paths):
            for _ in range(200):
                barrier.wait(WAIT)
                with self.locks.hold(*paths):
                    pass
            done.append(paths)

        a = start(lambda: worker(("/x", "/y")))
        b = start(lambda: worker(("/y", "/x")))
        self.assertTrue(a.wait(WAIT * 4))
        self.assertTrue(b.wait(WAIT * 4))
        self.assertEqual(0, len(self.locks))


if __name__ == "__main__":
    unittest.main()
