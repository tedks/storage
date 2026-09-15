load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

# Every storage_tests* target collects test modules that import fuse and
# mount real FUSE filesystems (the fast gate too, e.g.
# test_conflicts_integration.py).
#
# fusepy reads FUSE_LIBRARY_PATH to find libfuse 2 before falling back to
# find_library("fuse"). The flake's dev shell sets it; Bazel test actions
# do not inherit the client environment unless asked.
FUSE_TEST_ENV_INHERIT = ["FUSE_LIBRARY_PATH"]

# Mounting goes through the host's setuid fusermount, which cannot mount
# inside Bazel's linux-sandbox ("fusermount: mount failed: Operation not
# permitted"). Run these tests unsandboxed; they still need /dev/fuse and a
# setuid fusermount on the machine that runs them.
FUSE_TEST_TAGS = ["no-sandbox"]

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
