# Storage

Storage is a meta-filesystem that multiplexes an abstract filesystem over multiple underlying filesystems. Unlike other network storage systems like Dropbox or Google Drive, which function like big USB sticks, or local storage systems like RAIDs, which create big USB sticks, Storage is a single filesystem that is split over multiple different substrates.

## Features

- **Local Cache with LRU**: Local cache mirrors remote files with automatic LRU eviction
- **N-way Replication**: Configure per-directory replication (1 to N copies, or -1 for all backends)
- **Multiple Backend Types**: Local filesystems, SSH/SFTP remotes (coming soon)
- **Background Sync**: Writes go to cache first, sync to backends in background
- **Extended Attribute Configuration**: Control replication, encryption, and recomputation via xattrs
- **File Recomputation**: Mark files as recomputable for automatic space reclamation
- **FUSE-based**: Runs in userspace, no kernel modules required
- **SQLite State Tracking**: Efficient metadata tracking with optional extended attributes

## Installation

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

1. **StateManager** (`storage/state.py`): SQLite database + xattr metadata tracking
2. **CacheManager** (`storage/cache.py`): LRU cache management
3. **ReplicationManager** (`storage/replication.py`): N-way replication across backends
4. **Backend Abstraction** (`storage/backends/`): Local and remote filesystem support
5. **StorageFS** (`storage/fs.py`): FUSE filesystem implementation
6. **CLI** (`storage/cli.py`): Command-line tools

### How It Works

**Read Path:**
1. Check local cache for file
2. If not cached, download from best available backend
3. Future: Recompute if configured and available

**Write Path:**
1. Write to local cache first (ensure at least one copy)
2. Mark file as dirty in state database
3. Background sync to configured replicas (directory operations run best-effort synchronously and queue backend-specific retries when any replica is offline)

**Cache Management:**
1. Track file access times for LRU
2. Evict clean files when space needed
3. Prefer evicting recomputable files
4. Never evict dirty (unsynced) files

**Replication:**
1. Use extended attributes for per-directory configuration
2. Select backends using consistent hashing
3. Respect preferred backends and speed priorities
4. Propagate via background sync workers with retry/backoff

## Implementation Status

✅ **Implemented:**
- State tracking (SQLite + extended attributes)
- Local cache with LRU eviction
- Local filesystem backends
- N-way replication manager
- Basic FUSE operations (read, write, create, delete)
- Configuration via extended attributes
- CLI tools (mount, configure, info)

⏳ **Not Yet Implemented:**
- Background sync workers (writes only go to cache currently)
- Remote backends (SSH/SFTP planned)
- Recomputation engine
- Transparent encryption
- Conflict resolution
- Advanced space management (defragmentation, over-replication cleanup)

## Requirements

- Python 3.7+
- FUSE support (libfuse)
- Extended attribute support in filesystems
- SQLite3

## Limitations (Current Version)

- Files are written to cache but not automatically synced to backends (background sync TODO)
- Only local backends supported (remote backends TODO)
- No encryption yet
- No conflict detection/resolution
- Basic space management only

## License

See LICENSE file for details.
