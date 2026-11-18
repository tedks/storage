package(default_visibility = ["//visibility:public"])

# Placeholder target to illustrate how vendored dependencies are exposed.
filegroup(
    name = "all_files",
    srcs = glob(
        ["**/*"],
        exclude = ["BUILD"],
        # Bazel 8 rejects an empty glob; this placeholder package has no files.
        allow_empty = True,
    ),
)
