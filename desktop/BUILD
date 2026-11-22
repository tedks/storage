# Opt-in host ABI build. The shared object loads into the installed Nautilus,
# so use the matching native GIO/JSON-GLib development packages.
genrule(
    name = "nautilus_bridge",
    srcs = ["nautilus_additions.c"],
    outs = ["libstorage-nautilus.so"],
    cmd = "\"$${STORAGE_NAUTILUS_CC:-/usr/bin/cc}\" -shared -fPIC -Wall -Wextra -Werror -O2 -o $@ $< " +
          "$$(pkg-config --cflags --libs gio-2.0 json-glib-1.0) -ldl",
    local = True,
    tags = ["manual"],
)
