#!/bin/bash
set -e

echo "=== Storage Test Mount Setup ==="
echo

# Create local directories
echo "Creating local directories..."
mkdir -p ~/Downloads/storage-cache
mkdir -p ~/Downloads/storage-test

# Create remote directories
echo "Creating remote directories..."
ssh drynwyn "mkdir -p /srv/raid/warez/Downloads/storage-test" || echo "Warning: Could not create directory on drynwyn"
ssh framework0 "mkdir -p /home/tedks/Downloads/storage-test" || echo "Warning: Could not create directory on framework0"

echo
echo "=== Mounting Storage Filesystem ==="
echo "Mountpoint: ~/Downloads/storage-test"
echo "Cache: ~/Downloads/storage-cache (5GB)"
echo "Backends:"
echo "  - remote0: ssh://tedks@drynwyn:/srv/raid/warez/Downloads/storage-test"
echo "  - remote1: ssh://tedks@framework0:/home/tedks/Downloads/storage-test"
echo
echo "Use scripts/test_unmount.sh to unmount and stop the daemon"
echo

# Mount the filesystem using virtualenv python
.venv/bin/python3 -m storage.cli mount ~/Downloads/storage-test \
  --cache ~/Downloads/storage-cache \
  --cache-size 5G \
  --remote ssh://tedks@drynwyn:/srv/raid/warez/Downloads/storage-test \
  --remote ssh://tedks@framework0:/home/tedks/Downloads/storage-test \
  --key-file ~/.ssh/id_rsa
