import unittest

from storage.sync_tasks import DirOperation, DirSyncTask, SyncTask


class TestSyncTask(unittest.TestCase):
    def test_sync_task_creation(self):
        task = SyncTask("/test.txt", priority=50)
        self.assertEqual(task.virtual_path, "/test.txt")
        self.assertEqual(task.priority, 50)
        self.assertEqual(task.retry_count, 0)

    def test_sync_task_priority(self):
        task1 = SyncTask("/a.txt", priority=10)
        task2 = SyncTask("/b.txt", priority=50)
        task3 = SyncTask("/c.txt", priority=30)

        self.assertTrue(task2 < task1)
        self.assertTrue(task2 < task3)
        self.assertFalse(task1 < task2)


class TestDirSyncTask(unittest.TestCase):
    def test_dir_sync_task_creation(self):
        task = DirSyncTask("/dir", DirOperation.MKDIR, priority=100)
        self.assertEqual(task.virtual_path, "/dir")
        self.assertEqual(task.operation, DirOperation.MKDIR)
        self.assertEqual(task.priority, 100)

    def test_dir_sync_task_rename(self):
        task = DirSyncTask("/old", DirOperation.RENAME, new_path="/new", backend_hint="backend1")
        self.assertEqual(task.new_path, "/new")
        self.assertEqual(task.backend_hint, "backend1")

    def test_dir_sync_task_priority(self):
        fast = DirSyncTask("/fast", DirOperation.RMDIR, priority=50)
        slow = DirSyncTask("/slow", DirOperation.RMDIR, priority=10)
        self.assertTrue(fast < slow)


if __name__ == '__main__':
    unittest.main()
