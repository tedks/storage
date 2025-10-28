# Storage

Storage is a meta-filesystem that multiplexes an abstract filesystem over multiple underlying filesystems. Unlike other network storage systems like Dropbox or Google Drive, which function like big USB sticks, or local storage systems like RAIDs, which create big USB sticks, Storage is a single filesystem that is split over multiple different substrates.

## Features

- **Multiple Backing Filesystems**: Store your data across multiple physical filesystems transparently
- **Extended Attribute Metadata**: Control storage distribution using YAML documents in extended attributes
- **Flexible Configuration**: Use `/etc/storagetab` for system-wide mounts (similar to `/etc/fstab`)
- **FUSE-based**: Runs in userspace, no kernel modules required
- **Simple CLI**: Easy-to-use command-line interface for mounting and managing storage

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Quick Start

### Basic Usage

Mount a Storage filesystem with multiple backing directories:

```bash
storage mount /mnt/storage /mnt/disk1 /mnt/disk2 /mnt/disk3
```

This creates a unified filesystem at `/mnt/storage` backed by three different directories.

### Using /etc/storagetab

Create a configuration file at `/etc/storagetab`:

```
# Storage mount configuration
# Format: <mountpoint> <backing_paths> <options>
#
/mnt/storage /mnt/disk1,/mnt/disk2,/mnt/disk3 defaults
/home/user/docs /mnt/backup1,/mnt/backup2 replication=2
```

Then mount all configured filesystems:

```bash
storage mount-all
```

Or mount a specific entry:

```bash
storage mount-all /mnt/storage
```

## CLI Commands

### mount

Mount a Storage filesystem manually:

```bash
storage mount [OPTIONS] MOUNTPOINT BACKING_PATHS...

Options:
  -f, --foreground    Run in foreground
  -o, --options TEXT  Mount options (comma-separated)
```

Example:
```bash
storage mount -f /mnt/storage /mnt/disk1 /mnt/disk2
```

### mount-all

Mount filesystems from `/etc/storagetab`:

```bash
storage mount-all [OPTIONS] [MOUNTPOINT]

Options:
  -c, --config TEXT   Path to storagetab config file
  -f, --foreground    Run in foreground
```

### show-config

Display the current storagetab configuration:

```bash
storage show-config [CONFIG_PATH]
```

### info

Show storage metadata for a specific path:

```bash
storage info PATH
```

## Storage Metadata

Storage uses extended attributes to store YAML metadata about how files and directories should be stored. This metadata controls:

- **backing_filesystems**: Which backing filesystems to use
- **replication_count**: How many copies to maintain
- **preferred_filesystem**: Primary backing filesystem for this path
- **custom_config**: Additional custom configuration

Metadata is inherited from parent directories if not explicitly set.

## Architecture

### Components

1. **BackingManager** (`storage/backing.py`): Manages multiple backing filesystems
2. **XAttrManager** (`storage/xattr.py`): Handles extended attribute YAML metadata
3. **StorageFS** (`storage/fs.py`): FUSE filesystem implementation
4. **StorageTabConfig** (`storage/config.py`): Configuration file parser
5. **CLI** (`storage/cli.py`): Command-line interface

### How It Works

1. The FUSE filesystem layer receives filesystem operations
2. Paths are resolved to backing filesystems using metadata
3. Extended attributes store configuration in YAML format
4. Operations are forwarded to the appropriate backing filesystem
5. Multiple backing filesystems appear as a single unified view

## Requirements

- Python 3.7+
- FUSE support (libfuse)
- Extended attribute support in backing filesystems

## License

See LICENSE file for details.
