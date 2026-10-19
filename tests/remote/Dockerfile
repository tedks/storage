# A synthetic storage member for //:storage_tests_remote_gate: sshd with the
# tools the SSH backend, rsync over SSH and the remote watcher run on the far
# side. tests/test_remote_system.py builds it, tagged by a hash of this
# directory, and mounts the per-run keys at /keys.
FROM debian:bookworm-slim

# useradd leaves the account locked ("!"), which sshd refuses without PAM;
# "*" means no password login but an unlocked account.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-server rsync inotify-tools xxhash procps \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/ssh/ssh_host_* \
    && useradd --create-home --shell /bin/bash storage \
    && usermod --password "*" storage \
    && install -d -o storage -g storage -m 0755 /srv/storage \
    && install -d -m 0755 /run/sshd

COPY sshd_config /etc/ssh/sshd_config
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/bin/sh", "/usr/local/bin/entrypoint.sh"]
