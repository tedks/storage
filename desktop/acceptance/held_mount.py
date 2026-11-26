"""Serve two disposable roots. Release the second listing from the UI test."""

import os
import sys
import time
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.fs import mount_storage


def main():
    root = Path(sys.argv[1])
    for directory in ("mount", "cache", "first", "second"):
        (root / directory).mkdir()
    (root / "first" / "first-visible.txt").write_bytes(b"first")

    class HeldListing(LocalBackend):
        def listdir_attrs(self, virtual_path):
            if virtual_path == "/":
                (root / "held").touch()
                deadline = time.monotonic() + 45
                while not (root / "release").exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("UI did not release the test member")
                    time.sleep(0.05)
            return super().listdir_attrs(virtual_path)

    members = {
        "first": LocalBackend("first", str(root / "first")),
        "second": HeldListing("second", str(root / "second")),
    }
    os.environ["STORAGE_EXPERIMENTAL_DESKTOP_ADDITIONS"] = "1"
    mount_storage(
        str(root / "mount"), root / "cache", root / "index", members,
        16 << 20, foreground=True, fuse_threads=True, allow_other=False,
        progressive_listings=True, streaming_reads=True, negative_timeout=0, readdir_ttl=0.35,
    )


if __name__ == "__main__":
    main()
