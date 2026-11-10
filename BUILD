load("@rules_python//python:defs.bzl", "py_binary", "py_test")

package(default_visibility = ["//visibility:public"])

exports_files([
    "MODULE.bazel",
    "scripts/merge-gated.sh",
    "scripts/run-gates.sh",
])

alias(
    name = "storage_lib",
    actual = "//storage:storage_lib",
)

py_binary(
    name = "storage_cli",
    srcs = ["storage_cli_main.py"],
    main = "storage_cli_main.py",
    deps = [":storage_lib"],
)

alias(
    name = "storage_tests_resilience_gradient",
    actual = "//tests:storage_tests_resilience_gradient",
)
alias(
    name = "storage_tests_conflicts_integration",
    actual = "//tests:storage_tests_conflicts_integration",
)
alias(
    name = "storage_tests_fuse_thread_state",
    actual = "//tests:storage_tests_fuse_thread_state",
)
alias(
    name = "storage_tests_streaming_system",
    actual = "//tests:storage_tests_streaming_system",
)
alias(
    name = "storage_tests_sync_system",
    actual = "//tests:storage_tests_sync_system",
)
alias(
    name = "storage_tests_remote_system",
    actual = "//tests:storage_tests_remote_system",
)
py_test(
    name = "storage_build_srcs_check",
    srcs = ["//tests:build_srcs_check.py"],
    args = ["$(rootpath :build_srcs)"],
    data = [
        ":MODULE.bazel",
        ":build_srcs",
    ],
    main = "//tests:build_srcs_check.py",
    tags = ["external", "local"],
)

test_suite(
    name = "storage_tests_sandboxed",
    tests = ["//tests:storage_test_%s" % name for name in [
        "allow_other",
        "cache",
        "cache_metadata",
        "checksum_queue",
        "cli_debug",
        "cli_mount",
        "crypto",
        "file_config",
        "fs_core_integration",
        "fs_namespace_integration",
        "fs_revalidate",
        "fs_streaming",
        "fs_threading",
        "links",
        "local_backend",
        "locks",
        "member_capabilities",
        "models",
        "merge_gated",
        "mount_lock",
        "mount_sigpipe",
        "mount_profiles",
        "namespace",
        "pathing",
        "recompute",
        "replication",
        "rsync_backend",
        "run_gates",
        "ssh_backend",
        "ssh_mux",
        "state",
        "state_rename",
        "sync_dir_ops",
        "sync_executor",
        "sync_manager",
        "sync_races",
        "sync_tasks",
        "watcher",
    ]],
)

# The check sees sources under the nonmounting, mounted, and remote gates.
genquery(
    name = "build_srcs",
    expression = "labels(srcs, tests(//:storage_tests_nonmounting) + //tests:storage_tests_resilience_gradient + //tests:storage_tests_conflicts_integration + //tests:storage_tests_fuse_thread_state + //tests:storage_tests_streaming_system + //tests:storage_tests_sync_system + //tests:storage_tests_remote_system + //storage:storage_lib + //storage:storage_core + //storage/backends:storage_backends)",
    scope = [
        "//storage:storage_lib",
        "//storage:storage_core",
        "//storage/backends:storage_backends",
        ":storage_tests_nonmounting",
        "//tests:storage_tests_resilience_gradient",
        "//tests:storage_tests_conflicts_integration",
        "//tests:storage_tests_fuse_thread_state",
        "//tests:storage_tests_streaming_system",
        "//tests:storage_tests_sync_system",
        "//tests:storage_tests_remote_system",
    ],
)

test_suite(
    name = "storage_tests_nonmounting",
    tests = [
        "//tests:storage_tests_resilience_gradient",
        ":storage_tests_sandboxed",
    ],
)

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
        "//tests:storage_tests_conflicts_integration",
        "//tests:storage_tests_fuse_thread_state",
        "//tests:storage_tests_streaming_system",
        "//tests:storage_tests_sync_system",
    ],
    tags = ["manual"],
)

test_suite(
    name = "storage_tests_resilience_gate",
    tests = [
        "//tests:storage_tests_resilience_gradient",
        "//tests:storage_tests_streaming_system",
    ],
    tags = ["manual"],
)

test_suite(
    name = "storage_tests_remote_gate",
    tags = ["manual"],
    tests = ["//tests:storage_tests_remote_system"],
)

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
    tags = ["manual"],
)
