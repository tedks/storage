# Verify updates in a real Nautilus window

This opt-in Linux test opens two windows in a fresh Nautilus process under a private X display, D-Bus session and home directory. It serves two temporary Storage members and holds one listing until the first row appears. It then opens another location, releases the member, confirms a separate client can enumerate the late child and checks both original windows through AT-SPI. The test repeats with the adapter enabled. With the adapter disabled the late row must stay absent during the observation window. With it enabled the fixture then tests a removal and a batch larger than the original 128-event window, without reloading or changing the open directory.

The host needs Nautilus with AT-SPI support, system Python with PyGObject, Xvfb and the usual FUSE test dependencies. Build the native bridge against the installed desktop ABI. From this checkout, with Xvfb on PATH or with an explicit absolute path:

```sh
nix develop --command bazel build //desktop:nautilus_bridge
nix develop --command bazel test //desktop/acceptance:visible_rows \
  --nocache_test_results --test_output=errors \
  --test_arg=--library="$PWD/bazel-bin/desktop/libstorage-nautilus.so" \
  --test_arg=--xvfb="$(command -v Xvfb)"
```

Use `--test_arg=--control-only` in place of the library argument to run the unmodified desktop baseline alone. The fixture runs with a threaded FUSE loop, progressive listings and a zero negative-entry timeout. It primes a negative lookup before adding the late child to the slow backing filesystem. It never requires an existing mount or a desktop session.

Run the GIO lifecycle check without Xvfb or AT-SPI:

```sh
nix develop --command bazel test //desktop/acceptance:monitor_lifecycle \
  --nocache_test_results --test_output=errors \
  --test_arg=--library="$PWD/bazel-bin/desktop/libstorage-nautilus.so"
```

The lifecycle test observes a late addition, a removal and an attribute update on a private GLib main context. It also verifies that the cancelled monitor and another executable inheriting the preload receive none of those changes.
The opted-in probe starts with the bridge and a symlink to it in
`LD_PRELOAD`, then verifies that the variable is unset before using GIO.

## Verify Nautilus application launches

`child_preloads` uses the same private desktop and synthetic mount. It registers
two file associations, then opens their files through Nautilus. A Nix Bash
and a system Bash record which preloads they received and loaded. The test
checks that two caller preloads survive in order, including one with the
bridge's basename, and that the bridge and its mount settings stay out of
the child environment. It repeats the directory update checks in the same
opted-in Nautilus process.

With `update-desktop-database` installed and Xvfb on PATH, run:

```sh
nix develop --command bazel build //desktop:nautilus_bridge
nix develop --command bash -c '
  bazel test //desktop/acceptance:child_preloads \
    --nocache_test_results --test_output=errors \
    --test_arg=--library="$PWD/bazel-bin/desktop/libstorage-nautilus.so" \
    --test_arg=--xvfb="$(command -v Xvfb)" --test_arg=--nix-bash="$BASH"
'
```
