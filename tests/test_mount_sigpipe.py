"""mount_storage leaves SIGPIPE ignored once FUSE() returns."""

import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.backends.local import LocalBackend
from storage.fs import mount_storage


def sigpipe_ignored() -> bool:
    """The kernel's view; signal.getsignal() only reports Python's cache."""
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("SigIgn:"):
            return bool(int(line.split()[1], 16) & (1 << (signal.SIGPIPE - 1)))
    raise AssertionError("no SigIgn line in /proc/self/status")


def libfuse_teardown(*args, **kwargs):
    """Stand in for libfuse 2's fuse_remove_signal_handlers, which resets an
    ignored SIGPIPE to the default. (tests/test_fuse_thread_state.py checks
    the real libfuse does it.)"""
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


class TestMountRestoresSigpipe(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        for name in ("cache", "mount", "member"):
            (root / name).mkdir()
        self.root = root
        self.saved = signal.getsignal(signal.SIGPIPE)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)  # as Python starts
        self.addCleanup(signal.signal, signal.SIGPIPE, self.saved)
        self.addCleanup(self.tmp.cleanup)

    def _mount(self):
        mount_storage(
            str(self.root / "mount"), self.root / "cache", self.root / "cache" / "state.db",
            {"local0": LocalBackend("local0", str(self.root / "member"))},
            1024 * 1024, foreground=True, allow_other=False,
        )

    def test_sigpipe_is_ignored_again_after_fuse_returns(self):
        with patch("storage.fs.FUSE", side_effect=libfuse_teardown):
            self._mount()
        self.assertTrue(sigpipe_ignored())


if __name__ == "__main__":
    unittest.main()
