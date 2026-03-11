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
    name = "storage_tests_tier_a",
    srcs = ["tests/pytest_main_tier_a.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_tier_a.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
)

py_test(
    name = "storage_tests_tier_b",
    srcs = ["tests/pytest_main_tier_b.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_tier_b.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
)

py_test(
    name = "storage_tests_tier_c",
    srcs = ["tests/pytest_main_tier_c.py"] + glob(["tests/test_*.py"]),
    main = "tests/pytest_main_tier_c.py",
    deps = [
        ":storage_lib",
        "@pypi//pytest",
        "@pypi//pytest_timeout",
    ],
)
