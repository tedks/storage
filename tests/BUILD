load("@rules_python//python:defs.bzl", "py_test")
load(":storage_test.bzl", "storage_test")

package(default_visibility = ["//visibility:public"])

exports_files(["build_srcs_check.py"])

# Each test module is one target. The same pytest entry point is shared with
# precompile disabled to avoid conflicting pyc outputs across those targets.
storage_test("allow_other")
storage_test("cache", tags = ["no-remote"])
storage_test("cache_metadata", tags = ["no-remote"])
storage_test("checksum_queue")
storage_test("cli_debug", tags = ["no-remote"])
storage_test("cli_mount")
storage_test(
    "conflicts_integration",
    target = "storage_tests_conflicts_integration",
    extra_srcs = ["fuse_mount.py"],
    tags = ["local", "manual"],
    timeout = "long",
)
storage_test("crypto")
storage_test("file_config")
storage_test("fs_core_integration", tags = ["no-remote"])
storage_test("fs_namespace_integration", tags = ["no-remote"])
storage_test("fs_revalidate", tags = ["no-remote"])
storage_test("fs_streaming", tags = ["no-remote"])
storage_test("fs_threading")
storage_test(
    "fuse_thread_state",
    target = "storage_tests_fuse_thread_state",
    tags = ["local", "manual"],
)
storage_test("links")
storage_test("local_backend", tags = ["no-remote"])
storage_test("locks")
storage_test("member_capabilities")
storage_test("merge_gated", data = ["//:scripts/merge-gated.sh"])
storage_test("models")
storage_test("mount_lock")
storage_test("mount_profiles")
storage_test("mount_sigpipe")
storage_test("namespace")
storage_test("pathing")
storage_test("recompute")
storage_test(
    "remote_system",
    target = "storage_tests_remote_system",
    data = [
        "remote/Dockerfile",
        "remote/entrypoint.sh",
        "remote/sshd_config",
    ],
    tags = ["local", "manual"],
    timeout = "long",
)
storage_test("replication")
storage_test(
    "resilience_gradient",
    target = "storage_tests_resilience_gradient",
    tags = ["no-remote"],
)
storage_test("rsync_backend")
storage_test("run_gates", data = ["//:scripts/run-gates.sh"])
storage_test("ssh_backend")
storage_test("ssh_mux")
storage_test("state")
storage_test("state_rename")
storage_test(
    "streaming_system",
    target = "storage_tests_streaming_system",
    extra_srcs = ["fuse_mount.py"],
    tags = ["local", "manual"],
    timeout = "long",
)
storage_test("sync_dir_ops")
storage_test("sync_executor")
storage_test("sync_manager")
storage_test("sync_races", extra_srcs = ["test_fs_threading.py"])
storage_test(
    "sync_system",
    target = "storage_tests_sync_system",
    extra_srcs = ["fuse_mount.py"],
    tags = ["local", "manual"],
    timeout = "long",
)
storage_test("sync_tasks")
storage_test("watcher")
