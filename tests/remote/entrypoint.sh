#!/bin/sh
# Install the per-run keys mounted read-only at /keys with the ownership and
# modes sshd's StrictModes requires, then run sshd in the foreground.
set -eu
install -m 0600 /keys/ssh_host_ed25519_key /etc/ssh/ssh_host_ed25519_key
install -m 0644 /keys/ssh_host_ed25519_key.pub /etc/ssh/ssh_host_ed25519_key.pub
install -d -o storage -g storage -m 0700 /home/storage/.ssh
install -o storage -g storage -m 0600 /keys/authorized_keys /home/storage/.ssh/authorized_keys
exec /usr/sbin/sshd -D -e
