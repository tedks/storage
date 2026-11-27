load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "held_mount",
    legacy_create_init = 0,
    srcs = ["held_mount.py"],
    deps = ["//:storage_lib"],
)

# Requires a native Nautilus/GIO desktop stack and an Xvfb binary.
sh_test(
    name = "visible_rows",
    env_inherit = ["FUSE_LIBRARY_PATH"],
    srcs = ["visible_rows.sh"],
    data = [
        ":held_mount",
        "visible_rows.py",
    ],
    tags = ["local", "manual"],
    timeout = "moderate",
)

genrule(
    name = "caller_preload",
    srcs = ["caller_preload.c"],
    outs = ["caller-preload.so"],
    cmd = "/usr/bin/cc -shared -fPIC -nostdlib -Wall -Wextra -Werror -o $@ $<",
    local = True,
    tags = ["manual"],
)

sh_test(
    name = "child_preloads",
    env_inherit = ["FUSE_LIBRARY_PATH"],
    srcs = ["visible_rows.sh"],
    args = ["--caller-preload", "caller-preload.so"],
    data = [
        ":caller_preload",
        ":held_mount",
        "visible_rows.py",
    ],
    tags = ["local", "manual"],
    timeout = "moderate",
)

genrule(
    name = "monitor_probe_binary",
    srcs = ["monitor_probe.c"],
    # The bridge attaches only inside a process named nautilus.
    outs = ["probe/nautilus"],
    cmd = "PATH=/usr/bin:/bin " +
          "\"$${STORAGE_NAUTILUS_CC:-/usr/bin/cc}\" -Wall -Wextra -Werror -O2 -o $@ $< " +
          "$$(pkg-config --cflags --libs gio-2.0)",
    local = True,
    tags = ["manual"],
)

sh_test(
    name = "monitor_lifecycle",
    env_inherit = ["FUSE_LIBRARY_PATH"],
    srcs = ["visible_rows.sh"],
    args = ["--monitor-probe", "probe/nautilus"],
    data = [
        ":held_mount",
        ":monitor_probe_binary",
        "visible_rows.py",
    ],
    tags = ["local", "manual"],
    timeout = "moderate",
)
