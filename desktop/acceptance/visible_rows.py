"""Nautilus accessibility acceptance test. All GUI processes use a private bus."""

import argparse
import os
import secrets
import select
import shlex
import signal
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
                "LD_AUDIT", "BASH_ENV", "ENV", "STORAGE_NAUTILUS_MOUNT",
                "STORAGE_NAUTILUS_SOCKET", "PYTHONPATH", "GIO_EXTRA_MODULES",
                "GSETTINGS_SCHEMA_DIR"):
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
            if args.nix_bash:
                session.extend(["--nix-bash", args.nix_bash, "--caller-preload", args.caller_preload])
            subprocess.run(session, check=True, env=env, timeout=45)
    finally:
        os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)


def child_launches(root, quiet, nix_bash, caller_preload):
    # Two distinct preload objects, one sharing the bridge's basename.
    callers = [root / "caller-preload.so", root / "libstorage-nautilus.so"]
    for path in callers:
        shutil.copyfile(caller_preload, path)
    applications = Path(os.environ["XDG_DATA_HOME"]) / "applications"
    applications.mkdir(parents=True)
    config = Path(os.environ["XDG_CONFIG_HOME"])
    config.mkdir(parents=True, exist_ok=True)
    defaults = ["[Default Applications]"]
    cases = []
    for name, interpreter, filename, mime, payload in (
        ("nix", nix_bash, "nix-child.txt", "text/plain", b"synthetic text\n"),
        ("system", "/usr/bin/bash", "system-child.pdf", "application/pdf", b"%PDF-1.4\n"),
    ):
        target = quiet / filename
        target.write_bytes(payload)
        script = root / f"{name}-child"
        result = root / f"{name}-child.result"
        script.write_text(
            f"#!{interpreter}\n"
            f"exec > {shlex.quote(str(result))}\n"
            "printf '%s\\n' \"$BASH\" \"$1\" \"${LD_PRELOAD-}\" "
            "\"${STORAGE_NAUTILUS_MOUNT+x}${STORAGE_NAUTILUS_SOCKET+x}\"\n"
            f"for library in {shlex.join(map(str, callers))}; do\n"
            "    while IFS= read -r mapping; do\n"
            "        if [[ $mapping == *\" $library\" ]]; then\n"
            "            printf '%s\\n' \"$library\"\n"
            "            break\n"
            "        fi\n"
            "    done < /proc/$$/maps\n"
            "done\n"
            "printf '%s\\n' done\n"
        )
        script.chmod(0o700)
        app_id = f"storage-test-{name}.desktop"
        (applications / app_id).write_text(
            f"[Desktop Entry]\nType=Application\nName=Storage test {name}\n"
            f'Exec="{script}" %f\nMimeType={mime};\n'
        )
        defaults.append(f"{mime}={app_id};")
        cases.append((target, interpreter, result))
    (config / "mimeapps.list").write_text("\n".join(defaults) + "\n")
    subprocess.run(["/usr/bin/update-desktop-database", applications], check=True)
    return callers, cases


def observe(root, library, nix_bash, caller_preload):
    import gi
    gi.require_version("Atspi", "2.0")
    from gi.repository import Atspi, GLib

    quiet = root / "elsewhere"
    quiet.mkdir()
    (quiet / "quiet-window.txt").touch()
    env = dict(os.environ)
    if nix_bash:
        callers, cases = child_launches(root, quiet, nix_bash, caller_preload)
        env["LD_PRELOAD"] = " : ".join(map(str, callers))
    command = "/usr/bin/nautilus"
    if library:
        # Match the existing launcher: prepend the bridge, then exec native Nautilus.
        wrapper = root / "start-nautilus"
        wrapper.write_text(
            "#!/bin/sh\n"
            f"export STORAGE_NAUTILUS_MOUNT={shlex.quote(str(root / 'mount'))}\n"
            f"export STORAGE_NAUTILUS_SOCKET={shlex.quote(str(root / 'cache/.storage.control.sock'))}\n"
            f"export LD_PRELOAD={shlex.quote(library)}${{LD_PRELOAD:+:$LD_PRELOAD}}\n"
            'exec /usr/bin/nautilus "$@"\n'
        )
        wrapper.chmod(0o700)
        command = str(wrapper)
    with child([command, "--new-window", (root / "mount").as_uri()],
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

        def row(name, view=frame):
            GLib.MainContext.default().iteration(False)
            todo = [view]
            while todo:
                node = todo.pop()
                if not node:
                    continue
                if name in (node.get_name() or "").splitlines():
                    return node
                todo.extend(node.get_child_at_index(i) for i in range(node.get_child_count()))
            return None

        until(lambda: row("first-visible.txt"), "first row did not appear")
        assert (root / "held").exists(), "second member never entered the held listing"
        assert not row("late-visible.txt"), "late row appeared before member release"
        # Focus another location before release. Only the existing original view should change.
        subprocess.run(["/usr/bin/nautilus", "--new-window", quiet.as_uri()],
                       timeout=8, check=True)

        def quiet_frame():
            for i in range(app.get_child_count()):
                candidate = app.get_child_at_index(i)
                if candidate and row("quiet-window.txt", candidate):
                    return candidate
            return None

        other = until(quiet_frame, "unrelated window did not render while the member was held")
        assert not row("first-visible.txt", other), "original row leaked into the unrelated view"
        if nix_bash:
            for target, interpreter, result in cases:
                item = until(lambda: row(target.name, other), f"{target.name} did not render")
                # Xvfb has no window manager. GTK4 reports positions within the window.
                bounds = item.get_extents(Atspi.CoordType.WINDOW)
                assert bounds.width > 0 and bounds.height > 0, f"{target.name} is offscreen"
                assert Atspi.generate_mouse_event(bounds.x + bounds.width // 2,
                                                  bounds.y + bounds.height // 2, "b1d")

                def receipt():
                    value = result.read_text().splitlines() if result.exists() else []
                    return value if value[-1:] == ["done"] else None

                actual = until(receipt, f"Nautilus did not launch {target.name}", timeout=6)
                loaded = list(map(str, callers))
                assert actual[:2] == [interpreter, str(target)], actual
                assert actual[2] == env["LD_PRELOAD"], actual
                assert actual[3:] == ["", *loaded, "done"], actual
                print(f"Nautilus launched {target.name} with only the caller preloads", flush=True)
        (root / "release").touch()
        # A second client can read the committed union. It sends no actions to the GUI.
        until(lambda: "late-visible.txt" in os.listdir(root / "mount"), "late union did not publish")
        if library:
            until(lambda: row("late-visible.txt"), "late addition did not appear in this window")
            assert row("first-visible.txt"), "first row disappeared"
            # Let two published revisions pass while this exact private
            # Nautilus cannot poll its control socket.
            process.send_signal(signal.SIGSTOP)
            try:
                (root / "first/first-visible.txt").unlink()
                until(lambda: "first-visible.txt" not in os.listdir(root / "mount"),
                      "member removal never published")
                (root / "second/000-resync-added.txt").write_bytes(b"new generation")
                for index in range(130):
                    (root / "second" / f".gap-{index:03}").touch()
                until(lambda: "000-resync-added.txt" in os.listdir(root / "mount"),
                      "oversized revision never published")
            finally:
                process.send_signal(signal.SIGCONT)
            until(lambda: row("000-resync-added.txt") and not row("first-visible.txt"),
                  "the open window did not reconcile an oversized change")
            assert not row("000-resync-added.txt", other), "refresh leaked into another view"
        else:
            end = time.monotonic() + 3
            while time.monotonic() < end:
                assert not row("late-visible.txt"), "disabled bridge also delivered the row"
                time.sleep(0.1)
        assert not row("late-visible.txt", other), "late row leaked into the unrelated view"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=Path)
    parser.add_argument("--library")
    parser.add_argument("--fixture")
    parser.add_argument("--monitor-probe", type=Path)
    parser.add_argument("--nix-bash")
    parser.add_argument("--caller-preload")
    parser.add_argument("--xvfb", default=shutil.which("Xvfb"))
    parser.add_argument("--control-only", action="store_true")
    args = parser.parse_args()
    if bool(args.nix_bash) != bool(args.caller_preload):
        parser.error("supply both --nix-bash and --caller-preload for the child probe")
    if args.session:
        observe(args.session, args.library, args.nix_bash, args.caller_preload)
        return
    if args.monitor_probe and not args.library:
        parser.error("the monitor probe needs --library")
    if (
        not args.fixture
        or (not args.monitor_probe and not args.xvfb)
        or (not args.control_only and not args.library)
    ):
        parser.error("supply --xvfb and --library (or --control-only for the baseline)")
    if args.library:
        args.library = str(Path(args.library).resolve(strict=True))
    if args.nix_bash:
        args.caller_preload = str(Path(args.caller_preload).resolve(strict=True))
        if not args.nix_bash.startswith("/nix/store/") or not os.access(args.nix_bash, os.X_OK):
            parser.error("--nix-bash needs an absolute executable in /nix/store")
    for enabled in ((False,) if args.control_only else (False, True)):
        root = Path(tempfile.mkdtemp(prefix="storage-ui-"))
        try:
            with child([args.fixture, str(root)]) as fixture:
                try:
                    until(lambda: os.path.ismount(root / "mount"), "synthetic mount did not start")
                    # Prime a missing lookup, then seed the member before opening Nautilus.
                    assert not (root / "mount/late-visible.txt").exists()
                    (root / "second/late-visible.txt").write_bytes(b"second")
                    if args.monitor_probe:
                        quiet_env = dict(os.environ, LD_PRELOAD=args.library, GIO_USE_VFS="local",
                                         STORAGE_NAUTILUS_MOUNT=str(root / "mount"),
                                         STORAGE_NAUTILUS_SOCKET=str(root / "cache/.storage.control.sock"))
                        probe = args.monitor_probe
                        if enabled:
                            alias = root / "bridge-alias.so"
                            alias.symlink_to(args.library)
                            quiet_env["LD_PRELOAD"] += f":{alias}"
                        else:
                            # Keep the preload but change the executable name.
                            probe = Path(shutil.copy2(probe, root / "other-monitor"))
                        subprocess.run([probe, root, str(int(enabled))], env=quiet_env,
                                       check=True, timeout=10)
                        until(lambda: "late-visible.txt" in os.listdir(root / "mount"),
                              "late union did not publish")
                    else:
                        start_case(root, args, enabled)
                finally:
                    (root / "release").touch()
                    if os.path.ismount(root / "mount"):
                        subprocess.run(["fusermount", "-u", str(root / "mount")], check=True)
                    fixture.wait(timeout=20)
        finally:
            if os.path.ismount(root / "mount"):
                raise RuntimeError(f"Owned test mount still attached under {root}")
            shutil.rmtree(root)
        if args.monitor_probe:
            print("Native monitor scope and lifetime verified, bridge " +
                  ("active" if enabled else "inactive for another executable"))
        else:
            print("Visible rows verified, bridge " + ("enabled" if enabled else "disabled"))


if __name__ == "__main__":
    main()
