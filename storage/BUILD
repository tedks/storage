load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = ["//visibility:public"])

# Keep this list limited to modules in the storage package. Backends are a
# separate library so their sources and third-party dependencies stay explicit.
py_library(
    name = "storage_lib",
    srcs = [
        "__init__.py",
        "cache.py",
        "cache_metadata.py",
        "checksum_queue.py",
        "checksums.py",
        "cli.py",
        "cli_conflicts.py",
        "cli_debug.py",
        "cli_mount.py",
        "conflicts.py",
        "crypto.py",
        "file_config.py",
        "fs.py",
        "index_audit.py",
        "links.py",
        "locks.py",
        "models.py",
        "mount_profiles.py",
        "namespace.py",
        "pathing.py",
        "recompute.py",
        "replication.py",
        "spacemgmt.py",
        "state.py",
        "streaming.py",
        "sync.py",
        "sync_executor.py",
        "sync_tasks.py",
        "watcher.py",
    ],
    deps = [
        "//storage/backends:storage_backends",
        "@pypi//click",
        "@pypi//cryptography",
        "@pypi//fusepy",
        "@pypi//pyyaml",
        "@pypi//xxhash",
    ],
)
