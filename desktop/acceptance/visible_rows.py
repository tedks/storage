"""Nautilus accessibility acceptance test. All GUI processes use a private bus."""

import argparse
import os
import secrets
import select
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


def until(probe, message, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = probe()
        if value:
            return value
        time.sleep(0.08)
    raise AssertionError(message)


@contextmanager
def child(args, **options):
    with subprocess.Popen(args, **options) as process:
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()


def start_case(root, args, enabled):
    home = root / "home"
    runtime = root / "runtime"
    runtime.mkdir(mode=0o700)
    home.mkdir()
    env = dict(os.environ)
    for key in ("AT_SPI_BUS_ADDRESS", "DBUS_SESSION_BUS_ADDRESS", "DISPLAY",
                "WAYLAND_DISPLAY", "GTK_MODULES", "LD_PRELOAD", "LD_LIBRARY_PATH",
                "PYTHONPATH", "GIO_EXTRA_MODULES", "GSETTINGS_SCHEMA_DIR"):
        env.pop(key, None)
    env.update(HOME=str(home), XDG_RUNTIME_DIR=str(runtime), XDG_CACHE_HOME=str(home / ".cache"),
               XDG_CONFIG_HOME=str(home / ".config"), XDG_DATA_HOME=str(home / ".local/share"),
               XDG_STATE_HOME=str(home / ".local/state"), XDG_DATA_DIRS="/usr/local/share:/usr/share",
               XDG_CONFIG_DIRS="/etc/xdg", GSETTINGS_BACKEND="memory", GTK_A11Y="atspi", GIO_USE_VFS="local",
               NO_AT_BRIDGE="0", GDK_BACKEND="x11", LANG="C.UTF-8", PATH="/usr/bin:/bin")
    # One private cookie. The wildcard display field permits -displayfd to select a free display.
    auth = root / "xauthority"
    fields = (b"", b"", b"MIT-MAGIC-COOKIE-1", secrets.token_bytes(16))
    auth.write_bytes(struct.pack(">H", 65535) + b"".join(
        struct.pack(">H", len(field)) + field for field in fields))
    env["XAUTHORITY"] = str(auth)
    read_fd, write_fd = os.pipe()
    try:
        with child([args.xvfb, "-displayfd", str(write_fd), "-nolisten", "tcp",
                    "-auth", str(auth), "-screen", "0", "1024x768x24"],
                   pass_fds=(write_fd,), env=env):
            os.close(write_fd)
            write_fd = None
            if not select.select([read_fd], [], [], 8)[0]:
                raise RuntimeError("Xvfb did not announce its display")
            display = os.read(read_fd, 32).strip()
            if not display.isdigit():
                raise RuntimeError("Xvfb failed to allocate a display")
            env["DISPLAY"] = ":" + display.decode()
            session = ["/usr/bin/dbus-run-session", "--", sys.executable, __file__,
                       "--session", str(root)]
            if enabled:
                session.extend(["--library", args.library])
            subprocess.run(session, check=True, env=env, timeout=45)
    finally:
        os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)


def observe(root, library):
    import gi
    gi.require_version("Atspi", "2.0")
    from gi.repository import Atspi, GLib

    env = dict(os.environ)
    if library:
        env.update(LD_PRELOAD=library, STORAGE_NAUTILUS_MOUNT=str(root / "mount"),
                   STORAGE_NAUTILUS_SOCKET=str(root / "cache/.storage.control.sock"))
    with child(["/usr/bin/nautilus", "--new-window", (root / "mount").as_uri()],
               env=env) as process:
        desktop = Atspi.get_desktop(0)

        def application():
            for i in range(desktop.get_child_count()):
                app = desktop.get_child_at_index(i)
                if app and app.get_process_id() == process.pid and app.get_child_count():
                    return app
            return None

        app = until(application, "private Nautilus window did not appear")
        frame = app.get_child_at_index(0)

        def contains(name, view=frame):
            GLib.MainContext.default().iteration(False)
            todo = [view]
            while todo:
                node = todo.pop()
                if not node:
                    continue
                if name in (node.get_name() or "").splitlines():
                    return True
                todo.extend(node.get_child_at_index(i) for i in range(node.get_child_count()))
            return False

        until(lambda: contains("first-visible.txt"), "first row did not appear")
        assert (root / "held").exists(), "second member never entered the held listing"
        assert not contains("late-visible.txt"), "late row appeared before member release"
        # Focus another location before release. Only the existing original view should change.
        quiet = root / "elsewhere"
        quiet.mkdir()
        (quiet / "quiet-window.txt").touch()
        subprocess.run(["/usr/bin/nautilus", "--new-window", quiet.as_uri()],
                       env=env, timeout=8, check=True)

        def quiet_frame():
            for i in range(app.get_child_count()):
                candidate = app.get_child_at_index(i)
                if candidate and contains("quiet-window.txt", candidate):
                    return candidate
            return None

        other = until(quiet_frame, "unrelated window did not render while the member was held")
        assert not contains("first-visible.txt", other), "original row leaked into the unrelated view"
        (root / "release").touch()
        # A second client can read the committed union. It sends no actions to the GUI.
        until(lambda: "late-visible.txt" in os.listdir(root / "mount"), "late union did not publish")
        if library:
            until(lambda: contains("late-visible.txt"), "late addition did not appear in this window")
            assert contains("first-visible.txt"), "first row disappeared"
        else:
            end = time.monotonic() + 3
            while time.monotonic() < end:
                assert not contains("late-visible.txt"), "disabled bridge also delivered the row"
                time.sleep(0.1)
        assert not contains("late-visible.txt", other), "late row leaked into the unrelated view"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=Path)
    parser.add_argument("--library")
    parser.add_argument("--fixture")
    parser.add_argument("--xvfb", default=shutil.which("Xvfb"))
    parser.add_argument("--control-only", action="store_true")
    args = parser.parse_args()
    if args.session:
        observe(args.session, args.library)
        return
    if not args.xvfb or not args.fixture or (not args.control_only and not args.library):
        parser.error("supply --xvfb and --library (or --control-only for the baseline)")
    if args.library:
        args.library = str(Path(args.library).resolve(strict=True))
    for enabled in ((False,) if args.control_only else (False, True)):
        root = Path(tempfile.mkdtemp(prefix="storage-ui-"))
        try:
            with child([args.fixture, str(root)]) as fixture:
                try:
                    until(lambda: os.path.ismount(root / "mount"), "synthetic mount did not start")
                    # Prime a missing lookup, then seed the member before opening Nautilus.
                    assert not (root / "mount/late-visible.txt").exists()
                    (root / "second/late-visible.txt").write_bytes(b"second")
                    start_case(root, args, enabled)
                finally:
                    (root / "release").touch()
                    if os.path.ismount(root / "mount"):
                        subprocess.run(["fusermount", "-u", str(root / "mount")], check=True)
                    fixture.wait(timeout=8)
        finally:
            if os.path.ismount(root / "mount"):
                raise RuntimeError(f"Owned test mount still attached under {root}")
            shutil.rmtree(root)
        print("Visible rows verified, bridge " + ("enabled" if enabled else "disabled"))


if __name__ == "__main__":
    main()
