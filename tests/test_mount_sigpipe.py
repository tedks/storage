"""mount_storage keeps SIGPIPE from killing the process through FUSE()'s teardown."""

import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.backends.local import LocalBackend
from storage.fs import mount_storage


def _sigpipe_bit(field: str) -> bool:
    """The kernel's view of SIGPIPE; signal.getsignal() only reports Python's cache."""
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith(field + ":"):
            return bool(int(line.split()[1], 16) & (1 << (signal.SIGPIPE - 1)))
    raise AssertionError(f"no {field} line in /proc/self/status")


def sigpipe_ignored() -> bool:
    return _sigpipe_bit("SigIgn")


def sigpipe_caught() -> bool:
    return _sigpipe_bit("SigCgt")


class FakeLibfuse:
    """Stand-in for FUSE(): libfuse 2 resets an ignored SIGPIPE to the default
    in fuse_remove_signal_handlers, before it runs destroy(), and leaves any
    other disposition alone. (tests/test_fuse_thread_state.py checks the real
    libfuse does the first.) Records what destroy() would see."""

    def __init__(self, fail=None):
        self.fail = fail
        self.seen_in_destroy = None

    def __call__(self, *args, **kwargs):
        if sigpipe_ignored():
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        self.seen_in_destroy = (sigpipe_ignored(), sigpipe_caught())
        if self.fail:
            raise self.fail


class TestMountKeepsSigpipeHarmless(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        for name in ("cache", "mount", "member"):
            (root / name).mkdir()
        self.root = root
        self.addCleanup(self.tmp.cleanup)
        saved = signal.getsignal(signal.SIGPIPE)
        self.addCleanup(signal.signal, signal.SIGPIPE, saved)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)  # as Python starts

    def _mount(self, fake):
        with patch("storage.fs.FUSE", side_effect=fake):
            mount_storage(
                str(self.root / "mount"), self.root / "cache", self.root / "cache" / "state.db",
                {"local0": LocalBackend("local0", str(self.root / "member"))},
                1024 * 1024, foreground=True, allow_other=False,
            )

    def test_destroy_runs_with_sigpipe_caught_not_default(self):
        fake = FakeLibfuse()
        self._mount(fake)
        self.assertEqual(fake.seen_in_destroy, (False, True))

    def test_sigpipe_is_ignored_again_after_fuse_returns(self):
        self._mount(FakeLibfuse())
        self.assertTrue(sigpipe_ignored())
        self.assertIs(signal.getsignal(signal.SIGPIPE), signal.SIG_IGN)

    def test_sigpipe_is_restored_when_fuse_raises(self):
        with self.assertRaises(RuntimeError):
            self._mount(FakeLibfuse(fail=RuntimeError("mount failed")))
        self.assertTrue(sigpipe_ignored())

    def test_a_callers_own_disposition_comes_back(self):
        def handler(signum, frame):
            pass
        signal.signal(signal.SIGPIPE, handler)
        self._mount(FakeLibfuse())
        self.assertIs(signal.getsignal(signal.SIGPIPE), handler)

    def test_a_default_disposition_comes_back_as_default(self):
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        self._mount(FakeLibfuse())
        self.assertIs(signal.getsignal(signal.SIGPIPE), signal.SIG_DFL)


if __name__ == "__main__":
    unittest.main()
