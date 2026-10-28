# Storage

Storage is a meta-filesystem that multiplexes an abstract filesystem over multiple underlying filesystems. Unlike other network storage systems like Dropbox or Google Drive, which function like big USB sticks, or local storage systems like RAIDs, which create big USB sticks, Storage is a single filesystem that is split over multiple different substrates.

## Features

- **Local Cache with LRU**: Local cache mirrors remote files with automatic LRU eviction
- **Streaming Reads**: Uncached files are read straight from a remote member with background prefetch, so large files start without a full download
- **N-way Replication**: Configure per-directory replication (1 to N copies, or -1 for all storage members)
- **Multiple Backend Types**: Local filesystems and SSH remotes (SFTP for reads, rsync for writes)
- **Background Sync**: Writes go to cache first, sync to backends in background, with conflict detection and a `storage conflicts` command
- **Remote Change Notification**: inotify watchers on SSH members invalidate the cache when files change behind the mount
- **Mount Profiles**: Named TOML profiles for mounts
- **Extended Attribute Configuration**: Control replication, encryption, and recomputation via xattrs
- **Cache Metadata Xattrs**: Cached files carry `user.storage.metadata` so the local SQLite index can be rebuilt and verified
- **File Recomputation**: Mark files as recomputable for automatic space reclamation
- **FUSE-based**: Runs in userspace, no kernel modules required
- **SQLite State Tracking**: Efficient metadata tracking with optional extended attributes

See [Status](#status) before trusting it with anything.

## Installation

The flake's default package is the `storage` CLI, with its Python
dependencies and libfuse 2. Mounting also needs `/dev/fuse` and a setuid
`fusermount` from the host on `PATH`, and the rsync backend and remote
watcher run the host's `ssh`, `rsync` and (on the member) `inotifywait`.

```bash
nix profile install github:tedks/storage   # or a Home Manager flake input
# Named profiles come from ~/.config/storage/storage.toml; copy
# storage.example.toml there and edit it first.
storage mount example-media
```

For development:

```bash
# Set up local virtualenv (recommended)
./setup-venv.sh

# Or install manually
pip install -r requirements.txt
pip install -e .
```

The `setup-venv.sh` helper script creates `.venv/`, installs/upgrades `pip`, and applies `requirements.txt`. Activate it via `source .venv/bin/activate` before hacking or running pytest.

## Development Environment (Nix + Bazel + direnv)

- **Nix flake dev shell**: `nix develop` (provides Bazel 7, JDK 11, Python 3.11, git, direnv).
- **direnv auto-load**: Install `direnv` and `nix-direnv`, add `eval "$(direnv hook bash)"` (or zsh/fish) to your shell, then run `direnv allow` once in the repo. This auto-enters the flake environment on `cd`.
- **Devcontainer**: Open in VS Code Remote Containers; the image installs Nix with flakes and runs `nix develop` on startup. The `/nix` store is cached in a named Docker volume for speed.
- **Bazel**: Build/test from the dev shell: `bazel build //:storage_cli` and `bazel test //:storage_tests`.
- **Helper script**: `./scripts/ensure-nix-shell.sh` will fetch nix-portable if Nix is absent, then run `nix develop`.

## Testing

- **Pytest (venv)**: Activate the virtualenv (`source .venv/bin/activate`) and run `pytest`.
- **Bazel**: From the Nix dev shell, run `bazel test //:storage_tests` (or `bazel test //...` for every target not tagged `manual`). This uses the hermetic Bazel toolchain and the repo’s pinned Python deps.
- **Remote gate**: `bazel test //:storage_tests_remote_gate` runs the SSH, rsync and watcher tests against sshd in Docker containers. It needs Docker, and it fails when Docker is unavailable. It is tagged `manual`, so `//...` does not run it. See [AGENTS.md](AGENTS.md).

## Quick Start

### Basic Usage

Mount a Storage filesystem with local cache and multiple backing filesystems:

```bash
# Mount with 3 backing filesystems and 50GB cache
storage mount /mnt/storage /srv/raid /srv/chunk0 /srv/chunk1 \
  --cache /var/cache/storage \
  --cache-size 50G
```

This creates a unified filesystem at `/mnt/storage` backed by three directories with a local cache.

### Configure Replication

```bash
# Set backups directory to replicate across all backends
storage configure /mnt/storage/backups --replicas -1

# Set downloads to replicate to 2 backends
storage configure /mnt/storage/downloads --replicas 2

# View configuration
storage info /mnt/storage/backups
```

## CLI Commands

### mount

Mount a Storage filesystem:

```bash
storage mount MOUNTPOINT BACKING_PATHS... [OPTIONS]

Required:
  -c, --cache PATH         Local cache directory

Options:
  -s, --cache-size SIZE    Cache size (e.g., 10G, 500M) [default: 10G]
  -d, --database PATH      State database path [default: cache/.storage.db]
  -f, --foreground         Run in foreground
```

Example:
```bash
storage mount /mnt/storage /srv/raid /srv/chunk0 /srv/chunk1 \
  -c /var/cache/storage -s 100G -f
```

TOML mount profiles are also supported:

```toml
[mounts.example-media]
mountpoint = "/home/user/mnt/media"
cache = "/home/user/.cache/storage/media"
cache_size = "50G"
key_file = "/home/user/.ssh/id_ed25519"
foreground = true
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
  "ssh://user@storage-host.example:/srv/chunk0/media/",
  "ssh://user@storage-host.example:/srv/chunk1/media/",
]
```

Save that as `~/.config/storage/storage.toml` to auto-load by name:

```bash
storage mount example-media
```

Or mount from an explicit TOML path (`storage.example.toml` in this repo
is a fuller version of the same profile):

```bash
storage mount --config ./storage.example.toml example-media
```

A mount with at least one SSH or rsync-over-SSH member appears in the mount
table as type `fuse.sshfs` (a mount of only local members stays plain
`fuse`), so desktop tools that follow GLib's notion of remote (Nautilus,
LocalSearch) can treat it as a remote filesystem rather than a local disk and avoid thumbnails, item counts
and crawling through it.

Streaming reads (`streaming_reads`, on by default) serve an uncached file
by range reads while a background worker prefetches the rest into the
cache. When the last reader closes before the file is complete, the
prefetch continues only if the readers consumed the file: the longest
contiguous span they read reached `streaming_keep_after_close`
(`--streaming-keep-after-close`). Otherwise the partial download is
discarded, so a file manager sniffing content types or making thumbnails
does not pull whole files. The value is a comma-separated list of sizes
(`4M`, `512K`, plain bytes) and percentages of the file (`50%`, `12.5%`,
rounded down to bytes), whichever is reached first. Spans count the
read requests the kernel sends, which include its own readahead (up to
128K by default), so a size at or below that can act like `0`. Unset, it is 4
chunks or 50% (`4M,50%` at the default 1M `streaming_chunk_size`). `0`
keeps prefetching after any read, the behaviour before this option.
`100%` effectively never keeps prefetching after a close. Empty terms,
negative sizes, unknown suffixes and percentages above 100 are rejected
when the mount configuration loads.

#### SSH host keys

There is no host-key setting yet: host-key verification is not configurable,
and the three SSH code paths behave as follows (the decision for now is to
keep this and document it). Which ones a mount uses depends on its members:

- **`ssh://` members** use paramiko (`SSHBackend`) for everything: reads,
  metadata and writes. It uses `AutoAddPolicy` without loading any
  known-hosts file and persists nothing, so every connection accepts
  whatever key the far end presents. An impersonator is not detected, on a
  LAN or a VPN as much as anywhere. Key-only authentication stops it from
  relaying to the real host, but it can receive every file replicated to the
  member and serve forged listings and file contents to the mount. Weigh
  that before pointing a profile at a host over a path you do not control.
- **`rsync://` members** use paramiko the same way for reads, metadata and
  directory operations, and the system `ssh` (through `rsync -e`, with
  `StrictHostKeyChecking=accept-new`) for uploads.
- **Remote watchers** (`enable_watchers`, off by default) run the system
  `ssh` with `StrictHostKeyChecking=accept-new` for every remote member.

`accept-new` adds an unseen host to `~/.ssh/known_hosts` of the user running
the mount without asking and without showing anything (the output is only
logged when the command fails), and refuses a host whose recorded key
differs. Those `ssh` runs read your ssh config, so a `UserKnownHostsFile` or
`HostKeyAlias` there changes where the entry lives. Members are always
reached on port 22.

**A changed host key splits the paths.** After a member is re-provisioned,
or its address is reused by another host, the paramiko paths keep working
while the system-`ssh` ones fail with
`REMOTE HOST IDENTIFICATION HAS CHANGED` (logged by rsync as
`rsync failed`, and by the watcher as `inotifywait exited with code 255`,
which is ssh failing). For an `rsync://` member that means reads and listings
work but uploads do not replicate; with watchers on, a failed watcher stops
invalidating the cache, so listings can go stale until it reconnects (it
retries with backoff of up to 5 minutes, so a fix takes effect without a
remount). An `ssh://` member with no watchers never notices.

If the new key is expected, first check its fingerprint through a channel
you trust (for example `ssh-keygen -lf` on the member's
`/etc/ssh/ssh_host_*_key.pub`): a re-provisioned host does not make whatever
key answers genuine. Then remove the old entry; the next connection records
the key it is then offered, so make sure that is the verified one:

```bash
ssh-keygen -R storage-host.example
```

Use the name or address the profile uses. If the change is not expected,
treat it as a possible impersonation and do not remove the entry. Storage's
rsync and watcher call sites do not expose a known-hosts file or a strict
policy; making host-key verification configurable across all three paths is
tracked in [#107](https://github.com/tedks/storage/issues/107) and
[#30](https://github.com/tedks/storage/issues/30).

### configure

Configure storage settings for files/directories:

```bash
storage configure PATH [OPTIONS]

Options:
  -r, --replicas N             Number of replicas (-1 for all)
  -b, --backends LIST          Preferred backends (comma-separated)
  --recomputable              Mark as recomputable
  --recompute-cmd CMD         Command to recompute file
```

Examples:
```bash
# Replicate across all backends
storage configure /mnt/storage/critical --replicas -1

# Mark downloads as recomputable
storage configure /mnt/storage/downloads --recomputable \
  --recompute-cmd "wget https://example.com/file"
```

### info

Show storage configuration for a path:

```bash
storage info PATH
```

### debug

Inspect or repair local cache/index state:

```bash
storage debug rebuild-index --cache /var/cache/storage --database /var/cache/storage/.storage.db
storage debug reprobe-locations --cache /var/cache/storage --database /var/cache/storage/.storage.db --member local0=/srv/raid --member remote0=ssh://user@example.com:/srv/archive
storage debug verify-index --cache /var/cache/storage --database /var/cache/storage/.storage.db
```

## Configuration System

Storage uses extended attributes to store YAML configuration for files and directories:

- **replica_count**: Number of replicas (1-N, or -1 for all backends)
- **preferred_backends**: Preferred backing filesystems (list)
- **encrypted**: Transparent encryption enabled
- **encryption_backends**: Which backends to encrypt for
- **recomputable**: File can be recomputed if deleted
- **recompute_cmd**: Command to recreate the file

Configuration is hierarchical - child directories inherit from parents, with the maximum replica count being selected along any path.

## Architecture

### Components

1. **StateManager** (`storage/state.py`): Rebuildable SQLite operational index
2. **CacheManager** (`storage/cache.py`): LRU cache management
3. **Cache Metadata** (`storage/cache_metadata.py`): Durable cache-file metadata xattrs and cache/index audit helpers
4. **ReplicationManager** (`storage/replication.py`): N-way replication across storage members
5. **Storage Member Adapters** (`storage/backends/`): Local and remote filesystem support behind one capability surface
6. **StorageFS** (`storage/fs.py`): FUSE filesystem implementation
7. **CLI** (`storage/cli.py`): Command-line tools

### How It Works

**Read Path:**
1. Check local cache for file
2. If not cached, download from the best available storage member
3. Future: Recompute if configured and available

**Write Path:**
1. Write to local cache first (ensure at least one copy)
2. Mark file as dirty in state database
3. Background sync to configured replicas (directory operations run best-effort synchronously and queue member-specific retries when any replica is offline)

**Cache Management:**
1. Track file access times for LRU
2. Evict clean files when space needed
3. Prefer evicting recomputable files
4. Never evict dirty (unsynced) files

**Metadata and Recovery:**
1. File/directory policy remains xattr-defined via `user.storage.config`
2. Cached files persist cache-local metadata in `user.storage.metadata`
3. SQLite stores rebuildable acceleration state such as cache locations and LRU ordering
4. `storage debug rebuild-index` reconstructs cache-backed rows quickly from cache files plus xattrs
5. `storage debug reprobe-locations` explicitly repopulates non-cache member rows for the rebuilt file set using the cache root to keep `user.storage.metadata` authoritative
6. `storage debug verify-index` audits the cache/index boundary without mutating state

**Replication:**
1. Use extended attributes for per-directory configuration
2. Select storage members using consistent hashing
3. Respect preferred member choices and speed priorities
4. Propagate via background sync workers with retry/backoff

## Status

Alpha. Do not point it at data you do not have elsewhere.

One person has used it on one setup: a laptop mounting three replicas on a
single SSH host over a home LAN, for media playback and desktop file
browsing, mostly in February and March 2026. The bugs found that way are
fixed, but the code has been refactored substantially since, and those
paths have not been re-exercised against a real remote. The automated
suite (about 290 tests, see [AGENTS.md](AGENTS.md) for the Bazel gates)
mounts real FUSE filesystems and runs real `rsync`, but every remote
member in it is a local directory flagged as remote. A separate gate that
needs Docker, `//:storage_tests_remote_gate`, runs the SSH backend, rsync
over SSH and the inotify watcher against sshd in containers, and mounts
the filesystem with two of those containers as SSH members. Nothing yet
has been run against a real host since the field use above.

Implemented:

- Local, SSH/SFTP and rsync storage members (SFTP for reads, rsync for writes)
- Local cache with LRU eviction; streaming cold reads with background prefetch that stops when readers close early (`streaming_keep_after_close`)
- Background sync workers with retry; N-way replication configured per directory through xattrs
- Conflict detection on sync, and `storage conflicts` to list and resolve conflicts
- Remote inotify watchers that invalidate the cache when a member changes
- TOML mount profiles
- Cache metadata xattrs, with `storage debug rebuild-index`, `reprobe-locations` and `verify-index`
- File recomputation
- Transparent encryption (implemented; one test covers `storage/crypto.py`, so treat it as unverified)

Not implemented:

- Space management beyond cache eviction. Over-replication cleanup,
  defragmentation and hot/cold placement are stubs in
  `storage/spacemgmt.py`; the escalation chain in [DESIGN.md](DESIGN.md)
  describes the intent, not the code.
- Event-driven sync scheduling ([#17](https://github.com/tedks/storage/issues/17)); the sync monitor polls.

## Requirements

- Linux with FUSE (libfuse 2) and extended attribute support
- Python 3.11 (the Nix flake pins it; `setup.py` allows 3.7+ but that is not tested)
- SQLite3
- `rsync` and an SSH server on remote members; `inotifywait` there for change notification

## License

GNU Affero General Public License v3.0 or later. See [LICENSE](LICENSE).
