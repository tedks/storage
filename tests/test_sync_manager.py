import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.backends.local import LocalBackend
from storage.backends.base import BackendType
from storage.cache import CacheManager
from storage.file_config import FileConfig, set_file_config
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.replication import ReplicationManager
from storage.state import StateManager
from storage.sync import DirOperation, DirSyncTask, SyncConfig, SyncManager, SyncTask, SyncWorker
from storage.sync_executor import RetryScheduler, SyncExecutor


class TestSyncManager(unittest.TestCase):
    """Integration tests for SyncManager"""

    def setUp(self):
        """Set up test environment"""
        # Create temp directories
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        # Initialize components
        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 100 * 1024 * 1024)

        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir), speed_priority=0),
            "backend2": LocalBackend("backend2", str(self.backend2_dir), speed_priority=10),
        }

        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=2
        )

    def tearDown(self):
        """Clean up test environment"""
        self.sync_manager.stop()
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sync_manager_start_stop(self):
        """Test starting and stopping sync manager"""
        self.sync_manager.start()
        self.assertTrue(len(self.sync_manager.workers) > 0)

        self.sync_manager.stop()
        # Workers should be stopped
        for worker in self.sync_manager.workers:
            self.assertFalse(worker.is_alive())

    def test_stop_returns_promptly_and_leaves_an_idle_queue(self):
        """stop() must not wait out the monitor sleep or a worker's poll."""
        self.sync_manager.config.monitor_interval = 30.0
        self.sync_manager.start()
        time.sleep(0.2)  # monitor asleep, workers idle in their queue wait
        started = time.monotonic()
        self.sync_manager.stop()
        # Under the 1 s worker poll and the 30 s monitor sleep, with headroom for load.
        self.assertLess(time.monotonic() - started, 0.9)
        self.assertEqual(self.sync_manager.get_queue_size(), 0)
        self.assertTrue(self.sync_manager.wait_for_completion(timeout=0.1))

    def test_stop_with_a_busy_worker_leaves_no_wake_tasks(self):
        """A worker that exits without taking its wake must not leave it queued."""
        release = threading.Event()
        taken = threading.Event()

        self.sync_manager.start()
        for worker in self.sync_manager.workers:
            def slow(task):
                taken.set()
                release.wait(5.0)
                return True
            worker.executor.process = slow
        self.sync_manager.queue_sync("/busy.txt")
        self.assertTrue(taken.wait(5.0))
        threading.Timer(0.3, release.set).start()
        self.sync_manager.stop()
        self.assertEqual(self.sync_manager.get_queue_size(), 0)
        self.assertTrue(self.sync_manager.wait_for_completion(timeout=0.1))

    def test_restart_after_stop_processes_work(self):
        self.sync_manager.start()
        self.sync_manager.stop()
        self.sync_manager.start()
        self.assertTrue(all(w.is_alive() for w in self.sync_manager.workers))
        done = threading.Event()
        for worker in self.sync_manager.workers:
            worker.executor.process = lambda task: done.set() or True
        self.sync_manager.queue_sync("/again.txt")
        self.assertTrue(done.wait(5.0))
        self.sync_manager.stop()

    def test_adaptive_workers_overlap_distinct_tasks_and_retire(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=1, max_workers=4, idle_timeout=0.1,
            config=SyncConfig(monitor_interval=30.0),
        )
        release = threading.Event()
        entered = threading.Condition()
        active = 0
        peak = 0

        def blocked_process(executor, _task):
            nonlocal active, peak
            with entered:
                active += 1
                peak = max(peak, active)
                entered.notify_all()
            try:
                release.wait(5.0)
                executor._stats['synced'] += 1
                return True
            finally:
                with entered:
                    active -= 1
                    entered.notify_all()

        with patch.object(SyncExecutor, "process", blocked_process):
            self.sync_manager.start()
            try:
                for index in range(8):
                    self.sync_manager.queue_sync(f"/queued-{index}")
                self.sync_manager.queue_sync("/queued-0")
                with entered:
                    self.assertTrue(entered.wait_for(lambda: active == 4, timeout=5.0))
                self.assertEqual(4, peak)
                self.assertEqual(4, len(self.sync_manager.workers))
            finally:
                release.set()
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
            deadline = time.monotonic() + 3.0
            while len(self.sync_manager.workers) != 1 and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertEqual(1, len(self.sync_manager.workers))
            self.assertEqual(8, self.sync_manager.get_stats()['total_synced'])
            self.sync_manager.stop()

    def test_adaptive_growth_after_dequeue_accounting_gap(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=1, max_workers=2,
            config=SyncConfig(monitor_interval=30.0),
        )
        original_get = self.sync_manager.task_queue.get
        taken = threading.Event()
        allow_get_to_return = threading.Event()
        release_work = threading.Event()
        entered = threading.Condition()
        active = 0

        def held_get(*args, **kwargs):
            task = original_get(*args, **kwargs)
            if getattr(task, 'virtual_path', None) == '/first':
                taken.set()
                allow_get_to_return.wait(5.0)
            return task

        def blocked_process(_executor, _task):
            nonlocal active
            with entered:
                active += 1
                entered.notify_all()
            release_work.wait(5.0)
            return True

        self.sync_manager.task_queue.get = held_get
        with patch.object(SyncExecutor, "process", blocked_process):
            self.sync_manager.start()
            try:
                self.sync_manager.queue_sync('/first')
                self.assertTrue(taken.wait(5.0))
                self.sync_manager.queue_sync('/second')
                self.assertEqual(1, len(self.sync_manager.workers))
                allow_get_to_return.set()
                with entered:
                    self.assertTrue(entered.wait_for(lambda: active == 2, timeout=5.0))
            finally:
                allow_get_to_return.set()
                release_work.set()
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
            self.sync_manager.stop()

    def test_fixed_pool_stays_fixed_under_queue_pressure(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=2, idle_timeout=0.05,
            config=SyncConfig(monitor_interval=30.0),
        )
        release = threading.Event()
        entered = threading.Condition()
        active = 0

        def blocked_process(_executor, _task):
            nonlocal active
            with entered:
                active += 1
                entered.notify_all()
            release.wait(5.0)
            return True

        with patch.object(SyncExecutor, "process", blocked_process):
            self.sync_manager.start()
            try:
                for index in range(6):
                    self.sync_manager.queue_sync(f'/fixed-{index}')
                with entered:
                    self.assertTrue(entered.wait_for(lambda: active == 2, timeout=5.0))
                self.assertEqual(2, len(self.sync_manager.workers))
                self.assertEqual(4, self.sync_manager.get_queue_size())
            finally:
                release.set()
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
            self.sync_manager.stop()

    def test_adaptive_stop_preserves_queued_tasks_for_restart(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=1, max_workers=2,
            config=SyncConfig(monitor_interval=30.0),
        )
        release = threading.Event()
        entered = threading.Condition()
        active = 0

        def blocked_process(_executor, _task):
            nonlocal active
            with entered:
                active += 1
                entered.notify_all()
            release.wait(5.0)
            return True

        with patch.object(SyncExecutor, "process", blocked_process):
            self.sync_manager.start()
            for index in range(3):
                self.sync_manager.queue_sync(f"/pending-{index}")
            with entered:
                self.assertTrue(entered.wait_for(lambda: active == 2, timeout=5.0))
            stopping = threading.Thread(target=self.sync_manager.stop)
            stopping.start()
            deadline = time.monotonic() + 5.0
            while self.sync_manager._running and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertFalse(self.sync_manager._running)
            release.set()
            stopping.join(timeout=5.0)
            self.assertFalse(stopping.is_alive())
        self.assertEqual(1, self.sync_manager.get_queue_size())
        self.sync_manager.start()
        self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
        self.sync_manager.stop()

    def test_adaptive_worker_limits_are_bounded_before_start(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed 32"):
            SyncManager(
                self.state_manager, self.cache_manager, self.replication_manager,
                num_workers=1, max_workers=33,
            )
        self.replication_manager.backends['remote'] = type(
            'RemoteStub', (), {'backend_type': BackendType.SSH}
        )()
        with self.assertRaisesRegex(ValueError, "cannot exceed 8"):
            SyncManager(
                self.state_manager, self.cache_manager, self.replication_manager,
                num_workers=1, max_workers=9,
            )
        self.replication_manager.backends['remote'].backend_type = BackendType.RSYNC
        with self.assertRaisesRegex(ValueError, "cannot exceed 8"):
            SyncManager(
                self.state_manager, self.cache_manager, self.replication_manager,
                num_workers=1, max_workers=9,
            )

    def test_restart_waits_for_worker_that_outlived_stop(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=1, max_workers=2,
            config=SyncConfig(monitor_interval=30.0),
        )
        entered = threading.Event()
        release = threading.Event()

        def blocked_process(_executor, _task):
            entered.set()
            release.wait(5.0)
            return True

        with patch.object(SyncExecutor, "process", blocked_process):
            self.sync_manager.start()
            self.sync_manager.queue_sync("/busy")
            self.assertTrue(entered.wait(5.0))
            try:
                # Simulate the five-second deadline without spending five
                # seconds in this test; the worker remains in its task.
                with patch.object(SyncWorker, "join"):
                    self.sync_manager.stop()
                with self.assertRaisesRegex(RuntimeError, "still finishing"):
                    self.sync_manager.start()
            finally:
                release.set()
            self.sync_manager._lingering_workers[0].join(timeout=5.0)
        self.sync_manager.start()
        self.sync_manager.stop()

    def test_failed_initial_worker_start_cleans_up_partial_pool(self):
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=2, config=SyncConfig(monitor_interval=30.0),
        )
        original_start = SyncWorker.start
        attempts = 0

        def fail_second_start(worker):
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise RuntimeError('synthetic thread exhaustion')
            return original_start(worker)

        with patch.object(SyncWorker, 'start', fail_second_start):
            with self.assertRaisesRegex(RuntimeError, 'synthetic thread exhaustion'):
                self.sync_manager.start()
        self.assertFalse(self.sync_manager._running)
        self.assertEqual([], self.sync_manager.workers)
        self.assertEqual(0, self.sync_manager.get_queue_size())
        self.sync_manager.start()
        self.assertEqual(2, len(self.sync_manager.workers))
        self.sync_manager.stop()

    def test_queue_sync(self):
        """Test queuing a sync task"""
        self.sync_manager.start()

        # Create a file in cache
        test_file = "/test.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("test content")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("test content"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=2,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config for replication
        config = FileConfig(replica_count=2)
        set_file_config(cache_path, config)

        # Queue sync
        self.sync_manager.queue_sync(test_file, priority=100)

        # Wait for sync to complete
        time.sleep(2.0)

        # Check that file was synced
        locations = self.state_manager.get_file_locations(test_file)
        self.assertGreater(len(locations), 0, "File should be synced to at least one backend")

        # Check that backends have the file
        self.assertTrue(self.backends["backend1"].exists(test_file) or
                       self.backends["backend2"].exists(test_file))

    def test_sync_with_two_replicas(self):
        """Test syncing file with 2-way replication"""
        self.sync_manager.start()

        # Create a file in cache
        test_file = "/important.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        test_content = "important data"
        cache_path.write_text(test_content)

        # Create metadata with 2 replicas
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len(test_content),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=2,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=2)
        set_file_config(cache_path, config)

        # Queue sync and wait
        self.sync_manager.queue_sync(test_file, priority=100)
        time.sleep(3.0)

        # Both backends should have the file
        self.assertTrue(self.backends["backend1"].exists(test_file))
        self.assertTrue(self.backends["backend2"].exists(test_file))

        # Content should match
        backend1_content = (self.backend1_dir / test_file.lstrip('/')).read_text()
        backend2_content = (self.backend2_dir / test_file.lstrip('/')).read_text()
        self.assertEqual(backend1_content, test_content)
        self.assertEqual(backend2_content, test_content)

        # Metadata should be CLEAN
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CLEAN)

    def test_monitor_skips_busy_paths_until_callback_allows_sync(self):
        """Dirty files should not be enqueued while the filesystem still reports them busy."""
        busy_paths = {"/busy.txt"}

        self.sync_manager.stop()
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=0,
            config=SyncConfig(monitor_interval=0.05),
            should_sync_path_cb=lambda path: path not in busy_paths,
        )

        test_file = "/busy.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("busy")

        metadata = FileMetadata(
            virtual_path=test_file,
            size=4,
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(metadata)

        self.sync_manager.start()
        time.sleep(0.2)
        self.assertEqual(0, self.sync_manager.get_queue_size())

        busy_paths.clear()
        time.sleep(0.2)
        self.assertEqual(1, self.sync_manager.get_queue_size())

    def test_sync_now_synchronous(self):
        """Test synchronous sync operation"""
        self.sync_manager.start()

        # Create a file
        test_file = "/sync_now.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("sync now test")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("sync now test"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        set_file_config(cache_path, config)

        # Sync now (synchronous)
        result = self.sync_manager.sync_now(test_file, timeout=5.0)
        self.assertTrue(result, "Sync should complete successfully")

        # File should be synced
        self.assertTrue(self.backends["backend1"].exists(test_file) or
                       self.backends["backend2"].exists(test_file))

        # Status should be CLEAN
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CLEAN)

    def test_wait_for_completion_timeout(self):
        """wait_for_completion should honor timeout when work is unfinished."""
        # Queue an item without starting workers.
        self.sync_manager.task_queue.put(SyncTask("/never-runs.txt", priority=1))

        start = time.time()
        completed = self.sync_manager.wait_for_completion(timeout=0.2)
        elapsed = time.time() - start

        self.assertFalse(completed)
        self.assertGreaterEqual(elapsed, 0.2)
        self.assertLess(elapsed, 1.0)

        # Drain and mark done so queue has no unfinished tasks.
        _task = self.sync_manager.task_queue.get_nowait()
        self.sync_manager.task_queue.task_done()
        self.assertTrue(self.sync_manager.wait_for_completion(timeout=0.1))

    def test_dirty_file_monitoring(self):
        """Test that sync manager automatically picks up dirty files"""
        self.sync_manager.start()

        # Create a dirty file
        test_file = "/monitored.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("monitored content")

        # Create metadata as dirty
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("monitored content"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        set_file_config(cache_path, config)

        # Wait for monitor to pick it up (monitor runs every 5 seconds)
        time.sleep(7.0)

        # File should eventually be synced
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        # Should be either CLEAN or SYNCING
        self.assertIn(updated_metadata.sync_status, [SyncStatus.CLEAN, SyncStatus.SYNCING])


class TestGrowthWithFailingRenameRecords(unittest.TestCase):
    """Saturate a grown pool with rename tasks whose repair record fails.

    #182 never replays a queued member rename: it records it for repair,
    and retries a task whose record cannot be written without dropping it,
    its backoff held by the queue's RetryScheduler rather than a worker.
    With max_sync_workers such tasks all in flight at once, an unrelated
    file sync must still finish while the writes are failing; once they
    succeed, each task leaves one record, no member is renamed, nothing
    stays deferred, and the pool returns to its floor.
    """

    FLOOR, CEILING = 1, 3

    # The same members, cache and index as TestSyncManager, without
    # rerunning its tests.
    setUp = TestSyncManager.setUp
    tearDown = TestSyncManager.tearDown

    def test_healthy_sync_finishes_while_records_fail_then_pool_settles(self):
        import errno
        from storage.backends.local import LocalBackend as _Local
        from storage.member_repairs import MemberRepairs

        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=self.FLOOR, max_workers=self.CEILING, idle_timeout=0.1,
            # A long backoff: were it held in a worker (not the scheduler),
            # the healthy sync below could not finish inside its deadline.
            config=SyncConfig(monitor_interval=30.0, retry_delay=30.0, max_retries=3),
        )
        repairs = self.cache_manager.member_repairs
        names = [(f"/old-{i}", f"/new-{i}") for i in range(self.CEILING)]
        for old, _new in names:
            for directory in (self.backend1_dir, self.backend2_dir):
                (directory / old.lstrip("/")).write_bytes(b"member bytes " + old.encode())

        failing = threading.Event()
        failing.set()
        failures = []
        lock = threading.Lock()
        # Every failing task is inside record() at once, in its own worker,
        # and the test thread is the last party: the pool is at its ceiling
        # and every worker holds a distinct failing task. A broken barrier
        # in a worker would otherwise vanish into the retry path, so it is
        # kept and re-raised here.
        first_pass, arrived, barrier_errors = set(), [], []
        pool_at_rendezvous = []
        # The action runs once every party has arrived and before any is
        # released, so the pool size it reads is the size while all the
        # workers are still inside record(); read later, idle workers may
        # already have retired (idle_timeout is short).
        saturated = threading.Barrier(
            self.CEILING + 1, timeout=10.0,
            action=lambda: pool_at_rendezvous.append(len(self.sync_manager.workers)),
        )
        real_record, real_write = MemberRepairs.record, MemberRepairs._write_new

        def record(repairs_self, old_path, *args, **kwargs):
            with lock:
                first = old_path not in first_pass
                first_pass.add(old_path)
            if first:
                with lock:
                    arrived.append((old_path, threading.current_thread().name))
                try:
                    saturated.wait()
                except threading.BrokenBarrierError as exc:
                    with lock:
                        barrier_errors.append(exc)
                    raise
            return real_record(repairs_self, old_path, *args, **kwargs)

        def write_new(repairs_self, *args, **kwargs):
            if failing.is_set():
                with lock:
                    failures.append(time.monotonic())
                raise OSError(errno.ENOSPC, "injected: repair store full")
            return real_write(repairs_self, *args, **kwargs)

        renamed = []

        def no_rename(backend, old, new):
            renamed.append((backend.name, old, new))
            raise AssertionError("a member rename was replayed")

        healthy = "/healthy.txt"
        payload = b"healthy bytes \x00\x01 " * 64

        def on_members():
            return [d for d in (self.backend1_dir, self.backend2_dir)
                    if (d / "healthy.txt").exists()]

        with patch.object(MemberRepairs, "record", record), \
                patch.object(MemberRepairs, "_write_new", write_new), \
                patch.object(_Local, "rename", no_rename):
            self.sync_manager.start()
            for old, new in names:
                self.sync_manager.queue_dir_sync(old, DirOperation.RENAME, new_path=new)
            saturated.wait()          # every worker now holds a failing task
            self.assertEqual([], barrier_errors)
            with lock:
                self.assertEqual(sorted(old for old, _ in names),
                                 sorted(old for old, _ in arrived))
                self.assertEqual(self.CEILING, len({worker for _, worker in arrived}))
            self.assertEqual([self.CEILING], pool_at_rendezvous)

            # Only now is the healthy file made and marked dirty: the
            # monitor's pass at start() cannot have synced it early.
            self.assertEqual([], on_members())
            cache_path = self.cache_manager.get_cache_path(healthy)
            cache_path.write_bytes(payload)
            self.state_manager.set_file_metadata(FileMetadata(
                virtual_path=healthy, size=len(payload), checksum=None,
                last_access=time.time(), last_modified=time.time(),
                sync_status=SyncStatus.DIRTY, replica_count=1, recomputable=False,
                recompute_cmd=None, encrypted=False, encryption_key_id=None,
            ))
            set_file_config(cache_path, FileConfig(replica_count=1))
            self.sync_manager.queue_sync(healthy, priority=100)
            def healthy_clean():
                row = self.state_manager.get_file_metadata(healthy)
                return row is not None and row.sync_status == SyncStatus.CLEAN

            deadline = time.monotonic() + 10.0
            while not healthy_clean() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(healthy_clean(), "the healthy sync never finished")
            members = on_members()
            self.assertEqual(1, len(members))
            self.assertEqual(payload, (members[0] / "healthy.txt").read_bytes())
            # Still failing: no record was written, and the failing
            # retries are held in the queue's RetryScheduler, not in a
            # worker (CTO RRS-CTO-BACKOFF).
            done_at = time.monotonic()
            self.assertTrue(failing.is_set())
            self.assertEqual([], repairs.list())
            scheduler = RetryScheduler.for_queue(self.sync_manager.task_queue)
            deadline = time.monotonic() + 10.0
            seen = scheduler.pending()
            while seen != self.CEILING and time.monotonic() < deadline:
                time.sleep(0.01)
                seen = scheduler.pending()
            self.assertEqual(self.CEILING, seen)
            # A further round, delivered now, still fails.
            scheduler.deliver_all_now()
            deadline = time.monotonic() + 10.0
            while not any(t > done_at for t in failures) and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(any(t > done_at for t in failures))

            failing.clear()
            scheduler.deliver_all_now()
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=15.0))
        self.assertEqual([], barrier_errors)
        self.assertEqual(0, scheduler.pending())

        records = repairs.list()
        self.assertEqual(sorted(names), sorted((r.old_path, r.new_path) for r in records))
        self.assertEqual(len(names), len(records))      # one per task
        for r in records:
            self.assertEqual(("backend1", "backend2"), tuple(sorted(r.members)))
        self.assertEqual([], renamed)
        for old, new in names:
            for directory in (self.backend1_dir, self.backend2_dir):
                self.assertEqual(b"member bytes " + old.encode(),
                                 (directory / old.lstrip("/")).read_bytes())
                self.assertFalse((directory / new.lstrip("/")).exists())
        deadline = time.monotonic() + 5.0
        while len(self.sync_manager.workers) != self.FLOOR and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.FLOOR, len(self.sync_manager.workers))
        self.sync_manager.stop()

    def test_a_delivered_rename_retry_starts_a_worker_with_a_zero_floor(self):
        # workers = 0 is accepted with max_sync_workers: a retry delivered
        # after the pool retired to nothing must start a worker.
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=0, max_workers=1, idle_timeout=0.1,
            config=SyncConfig(monitor_interval=30.0, retry_delay=60.0, max_retries=3),
        )
        attempts, done = [], threading.Event()

        def process(executor, task):
            attempts.append(task.virtual_path)
            if len(attempts) == 1:
                raise RuntimeError("injected: repair store refused")
            done.set()
            return True

        scheduler = RetryScheduler.for_queue(self.sync_manager.task_queue)
        with patch.object(SyncExecutor, "process", process):
            self.sync_manager.start()
            self.sync_manager.queue_dir_sync("/old", DirOperation.RENAME, new_path="/new")
            deadline = time.monotonic() + 5.0
            while ((scheduler.pending() != 1 or self.sync_manager.workers)
                   and time.monotonic() < deadline):
                time.sleep(0.02)
            self.assertEqual(1, scheduler.pending())
            self.assertEqual([], self.sync_manager.workers)
            scheduler.deliver_all_now()
            self.assertTrue(done.wait(5.0), "the delivered retry found no worker")
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
            self.sync_manager.stop()

    def test_a_delivered_rename_retry_grows_the_pool_past_a_busy_floor(self):
        # The scheduler puts a deferred retry back from its own thread. With
        # the only floor worker busy, the pool must grow for it then, not
        # wait for some later queue event.
        self.sync_manager = SyncManager(
            self.state_manager, self.cache_manager, self.replication_manager,
            num_workers=self.FLOOR, max_workers=2, idle_timeout=0.1,
            config=SyncConfig(monitor_interval=30.0, retry_delay=60.0, max_retries=3),
        )
        release, blocked = threading.Event(), threading.Event()
        attempts, done = [], threading.Event()

        def process(executor, task):
            if task.virtual_path == "/blocker":
                blocked.set()
                release.wait(10.0)
                return True
            attempts.append(threading.current_thread().name)
            if len(attempts) == 1:
                raise RuntimeError("injected: repair store refused")
            done.set()
            return True

        scheduler = RetryScheduler.for_queue(self.sync_manager.task_queue)
        with patch.object(SyncExecutor, "process", process):
            self.sync_manager.start()
            try:
                self.sync_manager.queue_sync("/blocker")
                self.assertTrue(blocked.wait(5.0))
                self.sync_manager.queue_dir_sync("/old", DirOperation.RENAME, new_path="/new")
                deadline = time.monotonic() + 5.0
                while ((scheduler.pending() != 1 or len(self.sync_manager.workers) != self.FLOOR)
                       and time.monotonic() < deadline):
                    time.sleep(0.02)
                # Deferred, and the grown worker has retired: only the busy
                # floor worker is left.
                self.assertEqual(1, scheduler.pending())
                self.assertEqual(self.FLOOR, len(self.sync_manager.workers))
                scheduler.deliver_all_now()
                self.assertTrue(done.wait(5.0), "the delivered retry waited behind the busy floor")
                self.assertFalse(release.is_set())
                self.assertEqual(2, len(attempts))
            finally:
                release.set()
            self.assertTrue(self.sync_manager.wait_for_completion(timeout=5.0))
            self.sync_manager.stop()


class TestSyncConflicts(unittest.TestCase):
    """Tests for conflict detection during sync"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 100 * 1024 * 1024)

        self.backends = {
            "backend": LocalBackend("backend", str(self.backend_dir), speed_priority=0),
        }

        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=1
        )

    def tearDown(self):
        """Clean up"""
        self.sync_manager.stop()
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_conflict_detection(self):
        """Test that conflicts are detected"""
        self.sync_manager.start()

        test_file = "/conflict.txt"

        # Create file in backend with different content
        backend_path = self.backend_dir / test_file.lstrip('/')
        backend_path.parent.mkdir(parents=True, exist_ok=True)
        backend_path.write_text("backend version")

        # Record location with old checksum
        old_checksum = self.state_manager.compute_checksum(backend_path)
        location = FileLocation(
            virtual_path=test_file,
            backend_name="backend",
            checksum=old_checksum,
            last_synced=time.time(),
            is_cached=False
        )
        self.state_manager.add_file_location(location)

        # Now modify the backend file (simulating external change)
        backend_path.write_text("modified externally")

        # Create different content in cache
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("cache version")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("cache version"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        set_file_config(cache_path, config)

        # Try to sync - should detect conflict
        self.sync_manager.sync_now(test_file, timeout=3.0)

        # Should be marked as CONFLICT
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CONFLICT)

class TestDirQueueDedup(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.cache_dir.mkdir()
        self.backend_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1_048_576)
        self.replication_manager = ReplicationManager(
            self.state_manager,
            {"backend": LocalBackend("backend", str(self.backend_dir))},
        )
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=0,
        )

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_queue_dir_sync_dedupes_same_backend(self):
        self.sync_manager.queue_dir_sync("/dedupe", DirOperation.MKDIR, backend_hint="backend")
        size_before = self.sync_manager.task_queue.qsize()
        self.sync_manager.queue_dir_sync("/dedupe", DirOperation.MKDIR, backend_hint="backend")
        self.assertEqual(self.sync_manager.task_queue.qsize(), size_before)

    def test_queue_dir_sync_allows_different_backends(self):
        self.sync_manager.replication_manager.backends['backend2'] = LocalBackend(
            "backend2", str(self.backend_dir)
        )
        self.sync_manager.queue_dir_sync("/dir", DirOperation.MKDIR, backend_hint="backend")
        self.sync_manager.queue_dir_sync("/dir", DirOperation.MKDIR, backend_hint="backend2")
        self.assertEqual(self.sync_manager.task_queue.qsize(), 2)

    def test_task_complete_releases_dir_key(self):
        task = DirSyncTask("/dir", DirOperation.MKDIR, backend_hint="backend")
        self.sync_manager.queue_dir_sync(task.virtual_path, task.operation, backend_hint=task.backend_hint)
        self.sync_manager._task_complete(task)
        self.sync_manager.queue_dir_sync(task.virtual_path, task.operation, backend_hint=task.backend_hint)
        self.assertEqual(self.sync_manager.task_queue.qsize(), 2)


if __name__ == '__main__':
    unittest.main()


class TestRequeueOnCompletion(unittest.TestCase):
    """A task flagged by the executor is replaced under the queue lock."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.cache_dir.mkdir()
        self.backend_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)
        backends = {"backend": LocalBackend("backend", str(self.backend_dir))}
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            ReplicationManager(self.state_manager, backends),
            num_workers=1,
        )
        # Not started: the queue and dedup set are driven directly.

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_flagged_task_hands_its_dedup_entry_to_a_fresh_task(self):
        self.sync_manager.queue_sync("/a")
        task = self.sync_manager.task_queue.get_nowait()
        task.requeue = True

        self.sync_manager._task_complete(task)

        self.assertIn("/a", self.sync_manager._queued_tasks)
        self.assertEqual(self.sync_manager.task_queue.qsize(), 1)
        replacement = self.sync_manager.task_queue.get_nowait()
        self.assertEqual(replacement.virtual_path, "/a")
        self.assertIsNot(replacement, task)
        self.assertFalse(replacement.requeue)
        self.assertIsNone(replacement.generation)
        # The entry still deduplicates ordinary requests against the fresh task.
        self.sync_manager.queue_sync("/a")
        self.assertEqual(self.sync_manager.task_queue.qsize(), 0)

    def test_unflagged_task_releases_its_dedup_entry(self):
        self.sync_manager.queue_sync("/a")
        task = self.sync_manager.task_queue.get_nowait()

        self.sync_manager._task_complete(task)

        self.assertNotIn("/a", self.sync_manager._queued_tasks)
        self.assertEqual(self.sync_manager.task_queue.qsize(), 0)
