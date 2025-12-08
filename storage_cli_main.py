import os
from pathlib import Path

from storage.cli import main


def _restore_invocation_cwd():
    """
    Bazel runs binaries from a runfiles directory, which breaks relative paths
    (mountpoints, cache dirs) passed on the command line. Use Bazel-provided
    env vars to restore the user's original working directory when available.
    """
    orig_cwd = os.environ.get("BUILD_WORKING_DIRECTORY") or os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if orig_cwd:
        orig = Path(orig_cwd)
        try:
            if orig != Path.cwd():
                os.chdir(orig)
        except Exception:
            # If we can't chdir, continue with current cwd rather than failing early.
            pass


if __name__ == "__main__":
    _restore_invocation_cwd()
    raise SystemExit(main())
