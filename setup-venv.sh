#!/usr/bin/env bash
set -euo pipefail
VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r requirements.txt
cat <<EOM
Virtual environment ready.
Activate with: source $VENV_DIR/bin/activate
Run tests with: $VENV_DIR/bin/python -m pytest
EOM
