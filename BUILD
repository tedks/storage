load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

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
)
