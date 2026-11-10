load("@rules_python//python:defs.bzl", "py_test")

TEST_DEPS = [
    "//:storage_lib",
    "@pypi//pytest",
    "@pypi//pytest_timeout",
]

def storage_test(module, target = None, tags = [], extra_srcs = [], data = [], timeout = "moderate"):
    name = target or "storage_test_" + module
    py_test(
        name = name,
        srcs = ["__init__.py", "pytest_main.py"] + extra_srcs + ["test_" + module + ".py"],
        main = "pytest_main.py",
        args = ["tests/test_" + module + ".py"],
        precompile = "disabled",
        timeout = timeout,
        deps = TEST_DEPS,
        data = data,
        env_inherit = ["FUSE_LIBRARY_PATH"],
        tags = tags,
    )
