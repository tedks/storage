#!/usr/bin/env bash
set -euo pipefail
cd "${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}/desktop/acceptance"
/usr/bin/python3 visible_rows.py --fixture "$PWD/held_mount" "$@"
