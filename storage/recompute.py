"""
File recomputation engine for Storage

Allows files to be marked as recomputable with a command.
These files can be deleted to free space and recreated on access.
"""

import subprocess
import threading
import time
import logging
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass

from .state import StateManager, FileConfig

logger = logging.getLogger(__name__)


@dataclass
class RecomputeResult:
    """Result of a recomputation operation"""
    success: bool
    output: str
    error: str
    duration: float
    timed_out: bool


class RecomputeEngine:
    """
    Engine for recomputing files from commands

    Executes commands to recreate files that have been deleted
    to free space.
    """

    def __init__(self, state_manager: StateManager, default_timeout: float = 300.0):
        """
        Initialize recompute engine

        Args:
            state_manager: State manager instance
            default_timeout: Default timeout for recompute commands (seconds)
        """
        self.state_manager = state_manager
        self.default_timeout = default_timeout
        self._active_recomputes = {}  # path -> thread
        self._lock = threading.Lock()

    def can_recompute(self, virtual_path: str, cache_path: Path) -> bool:
        """
        Check if a file can be recomputed

        Args:
            virtual_path: Virtual path of file
            cache_path: Local cache path

        Returns:
            True if file is marked as recomputable and has a command
        """
        config = self.state_manager.get_inherited_config(cache_path)
        return config.recomputable and bool(config.recompute_cmd)

    def recompute_file(self, virtual_path: str, cache_path: Path,
                       timeout: Optional[float] = None,
                       callback: Optional[Callable] = None) -> RecomputeResult:
        """
        Recompute a file by executing its recompute command

        Args:
            virtual_path: Virtual path of file
            cache_path: Local cache path where file should be created
            timeout: Timeout in seconds (uses default if None)
            callback: Optional callback(progress_message) for status updates

        Returns:
            RecomputeResult with execution details
        """
        config = self.state_manager.get_inherited_config(cache_path)

        if not config.recomputable or not config.recompute_cmd:
            return RecomputeResult(
                success=False,
                output="",
                error="File is not marked as recomputable",
                duration=0.0,
                timed_out=False
            )

        # Ensure parent directory exists
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Prepare command environment
        env = {
            'STORAGE_PATH': virtual_path,
            'STORAGE_CACHE_PATH': str(cache_path),
            'STORAGE_OUTPUT': str(cache_path),
        }

        # Use system environment plus our variables
        import os
        full_env = os.environ.copy()
        full_env.update(env)

        timeout_val = timeout or self.default_timeout
        start_time = time.time()

        if callback:
            callback(f"Starting recompute: {config.recompute_cmd}")

        try:
            # Execute command
            result = subprocess.run(
                config.recompute_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_val,
                env=full_env,
                cwd=str(cache_path.parent)
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                # Check that file was created
                if cache_path.exists():
                    if callback:
                        callback(f"Recompute successful ({duration:.1f}s)")

                    logger.info(f"Successfully recomputed {virtual_path} in {duration:.1f}s")

                    return RecomputeResult(
                        success=True,
                        output=result.stdout,
                        error=result.stderr,
                        duration=duration,
                        timed_out=False
                    )
                else:
                    if callback:
                        callback("Recompute command succeeded but file not created")

                    logger.error(f"Recompute command succeeded but {cache_path} not created")

                    return RecomputeResult(
                        success=False,
                        output=result.stdout,
                        error="File not created by recompute command",
                        duration=duration,
                        timed_out=False
                    )
            else:
                if callback:
                    callback(f"Recompute failed with exit code {result.returncode}")

                logger.error(f"Recompute failed for {virtual_path}: {result.stderr}")

                return RecomputeResult(
                    success=False,
                    output=result.stdout,
                    error=result.stderr,
                    duration=duration,
                    timed_out=False
                )

        except subprocess.TimeoutExpired as e:
            duration = time.time() - start_time

            if callback:
                callback(f"Recompute timed out after {timeout_val}s")

            logger.warning(f"Recompute timed out for {virtual_path} after {timeout_val}s")

            return RecomputeResult(
                success=False,
                output=e.stdout.decode() if e.stdout else "",
                error=f"Timeout after {timeout_val}s",
                duration=duration,
                timed_out=True
            )

        except Exception as e:
            duration = time.time() - start_time

            if callback:
                callback(f"Recompute error: {e}")

            logger.error(f"Error recomputing {virtual_path}: {e}")

            return RecomputeResult(
                success=False,
                output="",
                error=str(e),
                duration=duration,
                timed_out=False
            )

    def recompute_file_async(self, virtual_path: str, cache_path: Path,
                            timeout: Optional[float] = None,
                            callback: Optional[Callable] = None):
        """
        Recompute a file asynchronously in a background thread

        Args:
            virtual_path: Virtual path of file
            cache_path: Local cache path
            timeout: Timeout in seconds
            callback: Optional callback for status updates

        Returns:
            Thread object for the recomputation task
        """
        with self._lock:
            # Check if already recomputing
            if virtual_path in self._active_recomputes:
                thread = self._active_recomputes[virtual_path]
                if thread.is_alive():
                    logger.debug(f"Recompute already in progress for {virtual_path}")
                    return thread

            # Start new recompute thread
            def _recompute_worker():
                try:
                    result = self.recompute_file(virtual_path, cache_path, timeout, callback)
                    if result.success:
                        logger.info(f"Async recompute successful for {virtual_path}")
                    else:
                        logger.error(f"Async recompute failed for {virtual_path}: {result.error}")
                finally:
                    with self._lock:
                        if virtual_path in self._active_recomputes:
                            del self._active_recomputes[virtual_path]

            thread = threading.Thread(target=_recompute_worker, daemon=True)
            thread.start()

            self._active_recomputes[virtual_path] = thread

            return thread

    def is_recomputing(self, virtual_path: str) -> bool:
        """Check if a file is currently being recomputed"""
        with self._lock:
            if virtual_path in self._active_recomputes:
                thread = self._active_recomputes[virtual_path]
                return thread.is_alive()
            return False

    def wait_for_recompute(self, virtual_path: str, timeout: Optional[float] = None) -> bool:
        """
        Wait for a recomputation to complete

        Args:
            virtual_path: Virtual path of file
            timeout: Maximum time to wait (None = wait forever)

        Returns:
            True if recompute completed, False if timeout or not recomputing
        """
        with self._lock:
            if virtual_path not in self._active_recomputes:
                return False
            thread = self._active_recomputes[virtual_path]

        thread.join(timeout=timeout)
        return not thread.is_alive()

    def cancel_recompute(self, virtual_path: str):
        """
        Attempt to cancel a recomputation

        Note: This is best-effort. The subprocess may continue running.
        """
        with self._lock:
            if virtual_path in self._active_recomputes:
                # Thread will clean itself up when subprocess completes/times out
                logger.info(f"Requested cancellation of recompute for {virtual_path}")

    def get_active_recomputes(self) -> list:
        """Get list of files currently being recomputed"""
        with self._lock:
            return [
                path for path, thread in self._active_recomputes.items()
                if thread.is_alive()
            ]


def mark_recomputable(cache_path: Path, recompute_cmd: str, state_manager: StateManager):
    """
    Mark a file or directory as recomputable

    Args:
        cache_path: Local cache path
        recompute_cmd: Command to execute to recreate the file
        state_manager: State manager instance
    """
    config = state_manager.get_file_config(cache_path) or FileConfig()
    config.recomputable = True
    config.recompute_cmd = recompute_cmd
    state_manager.set_file_config(cache_path, config)


def unmark_recomputable(cache_path: Path, state_manager: StateManager):
    """
    Remove recomputable marking from a file or directory

    Args:
        cache_path: Local cache path
        state_manager: State manager instance
    """
    config = state_manager.get_file_config(cache_path) or FileConfig()
    config.recomputable = False
    config.recompute_cmd = None
    state_manager.set_file_config(cache_path, config)
