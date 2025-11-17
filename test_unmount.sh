#!/bin/bash
set -euo pipefail

MOUNTPOINT=${1:-"$HOME/Downloads/storage-test"}

echo "=== Unmounting Storage Filesystem ==="
echo "Mountpoint: ${MOUNTPOINT}"

echo "Unmounting with fusermount..."
if ! fusermount -u "${MOUNTPOINT}" 2>/dev/null; then
  echo "Warning: fusermount reported an error (maybe already unmounted)" >&2
fi

echo "Stopping storage daemon..."
# Kill any storage mount process for this mountpoint
if ! pkill -f "storage\.cli mount ${MOUNTPOINT}" 2>/dev/null; then
  echo "Warning: no storage process matched for ${MOUNTPOINT}" >&2
fi

echo "Done."
