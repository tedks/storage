load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

# Each storage_tests* target collects at least one test module that imports
# fuse and mounts a real FUSE filesystem (the fast gate too, via
# test_conflicts_integration.py), so the whole target needs the following.
#
# fusepy reads FUSE_LIBRARY_PATH to find libfuse 2 before falling back to
# find_library("fuse"). The flake's dev shell sets it when the host has no
# libfuse 2; Bazel test actions do not inherit the client environment
# unless asked.
FUSE_TEST_ENV_INHERIT = ["FUSE_LIBRARY_PATH"]

# Mounting goes through the host's setuid fusermount, which cannot mount
# inside Bazel's linux-sandbox ("fusermount: mount failed: Operation not
# permitted"). "local" runs these tests unsandboxed and keeps them off
# remote executors and remote caches: they need /dev/fuse and a setuid
# fusermount on the machine that runs them, and a pass elsewhere says
# nothing about this host.
FUSE_TEST_TAGS = ["local"]

py_library(
    name = "storage_lib",
    srcs = glob([
        "storage/*.py",
        "storage/**/*.py",
    ]),
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

py_test(
    name = "storage_tests",
    srcs = ["tests/pytest_main.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = FUSE_TEST_TAGS,
)


py_test(
    name = "storage_tests_fast_gate",
    srcs = ["tests/pytest_main_fast.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_fast.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = FUSE_TEST_TAGS,
)

py_test(
    name = "storage_tests_mounted_system_gate",
    srcs = ["tests/pytest_main_mounted_system.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_mounted_system.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = FUSE_TEST_TAGS,
)

py_test(
    name = "storage_tests_resilience_gate",
    srcs = ["tests/pytest_main_resilience.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_resilience.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
    env_inherit = FUSE_TEST_ENV_INHERIT,
    tags = FUSE_TEST_TAGS,
)
