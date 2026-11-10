load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = ["//visibility:public"])

py_library(
    name = "storage_backends",
    srcs = [
        "__init__.py",
        "base.py",
        "local.py",
        "rsync.py",
        "ssh.py",
        "ssh_mux.py",
        "ssh_pool.py",
    ],
    deps = [
        "@pypi//paramiko",
        "@pypi//xxhash",
    ],
)
