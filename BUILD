load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

# Sources are listed explicitly, not globbed: a new module must be added
# here, or it is not built (storage/) or not run (tests/).

py_library(
    name = "storage_lib",
    srcs = [
        "storage/__init__.py",
        "storage/backends/__init__.py",
        "storage/backends/base.py",
        "storage/backends/local.py",
        "storage/backends/rsync.py",
        "storage/backends/ssh.py",
        "storage/cache.py",
        "storage/cache_metadata.py",
        "storage/checksums.py",
        "storage/cli.py",
        "storage/cli_conflicts.py",
        "storage/cli_debug.py",
        "storage/cli_mount.py",
        "storage/conflicts.py",
        "storage/crypto.py",
        "storage/file_config.py",
        "storage/fs.py",
        "storage/index_audit.py",
        "storage/models.py",
        "storage/mount_profiles.py",
        "storage/namespace.py",
        "storage/pathing.py",
        "storage/recompute.py",
        "storage/replication.py",
        "storage/spacemgmt.py",
        "storage/state.py",
        "storage/streaming.py",
        "storage/sync.py",
        "storage/sync_executor.py",
        "storage/sync_tasks.py",
        "storage/watcher.py",
    ],
    deps = [
        "@pypi//click",
        "@pypi//cryptography",
        "@pypi//fusepy",
        "@pypi//paramiko",
        "@pypi//pyyaml",
        "@pypi//xxhash",
    ],
)

py_binary(
    name = "storage_cli",
    srcs = ["storage_cli_main.py"],
    main = "storage_cli_main.py",
    deps = [":storage_lib"],
)

# Tests
#
# Each tests/test_*.py module belongs to exactly one py_test below. The
# gates are test_suites over those py_tests, so `bazel test` on a gate runs
# each py_test once even when two gates share it. Every py_test runs
# tests/pytest_main.py with its own modules as arguments, so a module list
# is written once and used for both srcs and args. They set
# precompile = "disabled" because they share that main: rules_python would
# otherwise emit one precompile action per target for the same
# tests/__pycache__/pytest_main.*.pyc, which Bazel rejects as conflicting.
#
# Most modules import fuse, directly or through storage.fs, so every test
# target needs libfuse 2. fusepy reads FUSE_LIBRARY_PATH to find it before
# falling back to find_library("fuse"). The flake's dev shell sets it when
# the host has no libfuse 2; Bazel test actions do not inherit the client
# environment unless asked.
FUSE_TEST_ENV_INHERIT = ["FUSE_LIBRARY_PATH"]

TEST_DEPS = [
    ":storage_lib",
    "@pypi//pytest",
    "@pypi//pytest_timeout",
]

# Modules that never mount a FUSE filesystem run inside Bazel's sandbox.
# A module that starts mounting fails here with "fusermount: mount failed:
# Operation not permitted"; move it to its own mounted target.
SANDBOXED_TEST_MODULES = [
    "tests/test_allow_other.py",
    "tests/test_cache.py",
    "tests/test_cache_metadata.py",
    "tests/test_cli_debug.py",
    "tests/test_cli_mount.py",
    "tests/test_crypto.py",
    "tests/test_file_config.py",
    "tests/test_fs_core_integration.py",
    "tests/test_fs_namespace_integration.py",
    "tests/test_fs_streaming.py",
    "tests/test_local_backend.py",
    "tests/test_member_capabilities.py",
    "tests/test_models.py",
    "tests/test_mount_lock.py",
    "tests/test_mount_profiles.py",
    "tests/test_namespace.py",
    "tests/test_pathing.py",
    "tests/test_recompute.py",
    "tests/test_replication.py",
    "tests/test_rsync_backend.py",
    "tests/test_ssh_backend.py",
    "tests/test_state.py",
    "tests/test_state_rename.py",
    "tests/test_sync_dir_ops.py",
    "tests/test_sync_executor.py",
    "tests/test_sync_manager.py",
    "tests/test_sync_tasks.py",
    "tests/test_watcher.py",
]

py_test(
    name = "storage_tests_sandboxed",
    srcs = ["tests/pytest_main.py"] + SANDBOXED_TEST_MODULES,
    args = SANDBOXED_TEST_MODULES,
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = FUSE_TEST_ENV_INHERIT,
)

# Sandboxed too; a separate target so the resilience gate can include it
# without the rest of the sandboxed modules.
py_test(
    name = "storage_tests_resilience_gradient",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_resilience_gradient.py",
    ],
    args = ["tests/test_resilience_gradient.py"],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = FUSE_TEST_ENV_INHERIT,
)

# Modules that mount real FUSE filesystems, one target each. Mounting goes
# through the host's setuid fusermount, which cannot mount inside Bazel's
# linux-sandbox. "local" runs these tests unsandboxed and keeps them off
# remote executors and remote caches: they need /dev/fuse and a setuid
# fusermount on the machine that runs them, and a pass elsewhere says
# nothing about this host.
MOUNTED_TEST_TAGS = ["local"]

py_test(
    name = "storage_tests_conflicts_integration",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_conflicts_integration.py",
    ],
    args = ["tests/test_conflicts_integration.py"],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = MOUNTED_TEST_TAGS,
)

py_test(
    name = "storage_tests_streaming_system",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_streaming_system.py",
    ],
    args = ["tests/test_streaming_system.py"],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = MOUNTED_TEST_TAGS,
)

py_test(
    name = "storage_tests_sync_system",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_sync_system.py",
    ],
    args = ["tests/test_sync_system.py"],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = MOUNTED_TEST_TAGS,
)

# Gates. Leave `tags` unset on these test_suites: on a test_suite, tags
# filter which member tests run.

# Everything that runs sandboxed.
test_suite(
    name = "storage_tests_fast_gate",
    tests = [
        ":storage_tests_resilience_gradient",
        ":storage_tests_sandboxed",
    ],
)

# Everything that mounts FUSE.
test_suite(
    name = "storage_tests_mounted_system_gate",
    tests = [
        ":storage_tests_conflicts_integration",
        ":storage_tests_streaming_system",
        ":storage_tests_sync_system",
    ],
)

test_suite(
    name = "storage_tests_resilience_gate",
    tests = [
        ":storage_tests_resilience_gradient",
        ":storage_tests_streaming_system",
    ],
)

# The whole suite: the fast and mounted gates together cover every module.
test_suite(
    name = "storage_tests",
    tests = [
        ":storage_tests_fast_gate",
        ":storage_tests_mounted_system_gate",
    ],
)
