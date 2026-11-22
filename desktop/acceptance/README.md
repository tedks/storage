# Verify late additions in a real Nautilus window

This opt-in Linux test opens two windows in a fresh Nautilus process under a private X display, D-Bus session and home directory. It serves two temporary Storage members and holds one listing until the first row appears. It then opens another location, releases the member, confirms a separate client can enumerate the late child and checks both original windows through AT-SPI. The test repeats with the adapter enabled. With the adapter disabled the late row must stay absent during the observation window.

The host needs Nautilus with AT-SPI support, system Python with PyGObject, Xvfb and the usual FUSE test dependencies. Build the native bridge against the installed desktop ABI. From this checkout, with Xvfb on PATH or with an explicit absolute path:

```sh
nix develop --command bazel build //desktop:nautilus_bridge
nix develop --command bazel test //desktop/acceptance:visible_rows \
  --nocache_test_results --test_output=errors \
  --test_arg=--library="$PWD/bazel-bin/desktop/libstorage-nautilus.so" \
  --test_arg=--xvfb="$(command -v Xvfb)"
```

Use `--test_arg=--control-only` in place of the library argument to run the unmodified desktop baseline alone. The fixture runs with a threaded FUSE loop, progressive listings and a zero negative-entry timeout. It primes a negative lookup before adding the late child to the slow backing filesystem. It never requires an existing mount or a desktop session.
