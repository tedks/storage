#!/bin/bash
# Manual smoke-test mount against two real SSH hosts. Set the hosts and
# remote paths for your own machines; the defaults are placeholders.
set -e

REMOTE0_HOST=${REMOTE0_HOST:-"user@storage-host0.example"}
REMOTE0_PATH=${REMOTE0_PATH:-"/srv/storage-test"}
REMOTE1_HOST=${REMOTE1_HOST:-"user@storage-host1.example"}
REMOTE1_PATH=${REMOTE1_PATH:-"/srv/storage-test"}
KEY_FILE=${KEY_FILE:-"$HOME/.ssh/id_ed25519"}
MOUNTPOINT=${MOUNTPOINT:-"$HOME/Downloads/storage-test"}
CACHE=${CACHE:-"$HOME/Downloads/storage-cache"}

echo "=== Storage Test Mount Setup ==="
echo

# Create local directories
echo "Creating local directories..."
mkdir -p "$CACHE"
mkdir -p "$MOUNTPOINT"

# Create remote directories
echo "Creating remote directories..."
ssh "$REMOTE0_HOST" "mkdir -p '$REMOTE0_PATH'" || echo "Warning: Could not create directory on $REMOTE0_HOST"
ssh "$REMOTE1_HOST" "mkdir -p '$REMOTE1_PATH'" || echo "Warning: Could not create directory on $REMOTE1_HOST"

echo
echo "=== Mounting Storage Filesystem ==="
echo "Mountpoint: $MOUNTPOINT"
echo "Cache: $CACHE (5GB)"
echo "Backends:"
echo "  - remote0: ssh://$REMOTE0_HOST:$REMOTE0_PATH"
echo "  - remote1: ssh://$REMOTE1_HOST:$REMOTE1_PATH"
echo
echo "Use scripts/test_unmount.sh to unmount and stop the daemon"
echo

# Mount the filesystem using virtualenv python
.venv/bin/python3 -m storage.cli mount "$MOUNTPOINT" \
  --cache "$CACHE" \
  --cache-size 5G \
  --remote "ssh://$REMOTE0_HOST:$REMOTE0_PATH" \
  --remote "ssh://$REMOTE1_HOST:$REMOTE1_PATH" \
  --key-file "$KEY_FILE"
