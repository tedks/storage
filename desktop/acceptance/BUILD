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
