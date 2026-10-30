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
        "storage/backends/ssh_pool.py",
        "storage/cache.py",
        "storage/locks.py",
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
# Each tests/test_*.py module is in the srcs of exactly one py_test, and the
# gates are test_suites over those py_tests. Every py_test runs
# tests/pytest_main.py, which collects the test modules in its runfiles:
# exactly its srcs. //:storage_build_srcs_check fails when a module on disk
# is not listed.
#
# The py_tests set precompile = "disabled" because they share that main:
# Bazel copies each target's tags into its actions, so a "local" and an
# untagged target would emit conflicting precompile actions for the same
# tests/__pycache__/pytest_main.*.pyc.
#
# Most modules import fuse, directly or through storage.fs. fusepy finds
# libfuse 2 through FUSE_LIBRARY_PATH, which the dev shell sets when the host
# has none; Bazel passes it to a test only through env_inherit.

TEST_DEPS = [
    ":storage_lib",
    "@pypi//pytest",
    "@pypi//pytest_timeout",
]

# Modules that do not mount FUSE run inside Bazel's sandbox. One that starts
# mounting fails here with "fusermount: mount failed: Operation not
# permitted"; give it its own mounted py_test.
py_test(
    name = "storage_tests_sandboxed",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_allow_other.py",
        "tests/test_cache.py",
        "tests/test_cache_metadata.py",
        "tests/test_cli_debug.py",
        "tests/test_cli_mount.py",
        "tests/test_crypto.py",
        "tests/test_file_config.py",
        "tests/test_fs_core_integration.py",
        "tests/test_fs_namespace_integration.py",
        "tests/test_fs_revalidate.py",
        "tests/test_fs_streaming.py",
        "tests/test_fs_threading.py",
        "tests/test_local_backend.py",
        "tests/test_locks.py",
        "tests/test_member_capabilities.py",
        "tests/test_models.py",
        "tests/test_mount_lock.py",
        "tests/test_mount_profiles.py",
        "tests/test_namespace.py",
        "tests/test_pathing.py",
        "tests/test_recompute.py",
        "tests/test_replication.py",
        "tests/test_rsync_backend.py",
        "tests/test_run_gates.py",
        "tests/test_ssh_backend.py",
        "tests/test_state.py",
        "tests/test_state_rename.py",
        "tests/test_sync_dir_ops.py",
        "tests/test_sync_executor.py",
        "tests/test_sync_manager.py",
        "tests/test_sync_tasks.py",
        "tests/test_watcher.py",
    ],
    data = ["scripts/run-gates.sh"],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    # About 200s on a fast host; the default 300s leaves little headroom.
    timeout = "long",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
)

# Sandboxed too; its own target so the resilience gate can include it alone.
py_test(
    name = "storage_tests_resilience_gradient",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_resilience_gradient.py",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
)

# Modules that mount real FUSE filesystems, one target each. The host's
# setuid fusermount cannot mount inside linux-sandbox, so these are "local":
# unsandboxed, and never remotely executed or cached, since they need
# /dev/fuse and fusermount on the machine that runs them.
py_test(
    name = "storage_tests_conflicts_integration",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_conflicts_integration.py",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
    tags = ["local"],
)

# Records what Python thread state survives between libfuse callbacks, with
# and without libfuse's worker threads.
py_test(
    name = "storage_tests_fuse_thread_state",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_fuse_thread_state.py",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
    tags = ["local"],
)

py_test(
    name = "storage_tests_streaming_system",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_streaming_system.py",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
    tags = ["local"],
)

py_test(
    name = "storage_tests_sync_system",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_sync_system.py",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    deps = TEST_DEPS,
    env_inherit = ["FUSE_LIBRARY_PATH"],
    tags = ["local"],
)

# Real remotes: sshd in Docker containers built from tests/remote/. "local"
# because it talks to the Docker daemon, which the sandbox does not reach, and
# its mounted tests need FUSE mounts, which fail inside linux-sandbox.
# "manual" keeps it out of //... and storage_tests, so hosts without Docker
# are unaffected; //:storage_tests_remote_gate names it and runs it, and
# there it fails rather than skips when Docker is unavailable.
py_test(
    name = "storage_tests_remote_system",
    srcs = [
        "tests/pytest_main.py",
        "tests/test_remote_system.py",
    ],
    data = [
        "tests/remote/Dockerfile",
        "tests/remote/entrypoint.sh",
        "tests/remote/sshd_config",
    ],
    main = "tests/pytest_main.py",
    precompile = "disabled",
    # The first run builds the image.
    timeout = "long",
    deps = TEST_DEPS,
    env_inherit = [
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "FUSE_LIBRARY_PATH",
        "STORAGE_TEST_REMOTE_PUBLISHED",
    ],
    tags = [
        "local",
        "manual",
    ],
)

# What the gates and storage_lib list, one label per line. It names
# storage_tests_nonmounting instead of the fast gate: the fast gate contains
# the check that reads this, and suite membership is a dependency (a cycle).
genquery(
    name = "build_srcs",
    expression = "labels(srcs, tests(//:storage_tests_nonmounting) + tests(//:storage_tests_mounted_system_gate) + tests(//:storage_tests_remote_gate) + //:storage_lib)",
    scope = [
        ":storage_lib",
        ":storage_tests_mounted_system_gate",
        ":storage_tests_nonmounting",
        ":storage_tests_remote_gate",
    ],
)

# Reads the source tree, not just its inputs: "local" to see it, "external"
# so a cached pass cannot hide a newly added file.
py_test(
    name = "storage_build_srcs_check",
    srcs = ["tests/build_srcs_check.py"],
    args = ["$(rootpath :build_srcs)"],
    data = [
        "MODULE.bazel",
        ":build_srcs",
    ],
    main = "tests/build_srcs_check.py",
    tags = [
        "external",
        "local",
    ],
)

# Gates. Leave `tags` unset on these test_suites: on a test_suite, tags
# filter which member tests run.

test_suite(
    name = "storage_tests_nonmounting",
    tests = [
        ":storage_tests_resilience_gradient",
        ":storage_tests_sandboxed",
    ],
)

# Keep storage_tests_nonmounting here; the srcs check cannot see this suite.
test_suite(
    name = "storage_tests_fast_gate",
    tests = [
        ":storage_build_srcs_check",
        ":storage_tests_nonmounting",
    ],
)

test_suite(
    name = "storage_tests_mounted_system_gate",
    tests = [
        ":storage_tests_conflicts_integration",
        ":storage_tests_fuse_thread_state",
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

# Not in storage_tests: it needs Docker. See AGENTS.md. The one exception to
# leaving tags unset: "manual" keeps //... from expanding this suite (and so
# its manual test); it does not filter the suite's own members.
test_suite(
    name = "storage_tests_remote_gate",
    tags = ["manual"],
    tests = [
        ":storage_tests_remote_system",
    ],
)

# Every gate, including the remote one: what scripts/run-gates.sh runs on a
# host with Docker. "manual" for the same reason as the remote gate above.
test_suite(
    name = "storage_tests_all_gates",
    tags = ["manual"],
    tests = [
        ":storage_tests_fast_gate",
        ":storage_tests_mounted_system_gate",
        ":storage_tests_remote_gate",
        ":storage_tests_resilience_gate",
    ],
)

test_suite(
    name = "storage_tests",
    tests = [
        ":storage_tests_fast_gate",
        ":storage_tests_mounted_system_gate",
    ],
)
