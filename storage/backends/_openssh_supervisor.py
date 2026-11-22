"""Own a foreground OpenSSH process group for the mounting process's lifetime."""

import array
import ctypes
import os
import select
import shutil
import signal
import socket
import struct
import subprocess
import sys


def _linux_call(name, *args):
    function = getattr(ctypes.CDLL(None, use_errno=True), name)
    result = function(*args)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def open_pidfd(pid: int) -> int:
    opener = getattr(os, "pidfd_open", None)
    if opener is not None:
        return opener(pid)
    # Some Python distributions build against older kernel headers.
    return _linux_call("pidfd_open", pid, 0)


def signal_pidfd(fd: int, sig: int) -> None:
    sender = getattr(signal, "pidfd_send_signal", None)
    if sender is not None:
        sender(fd, sig)
    else:
        _linux_call("pidfd_send_signal", fd, sig, None, 0)


def _guard(supervisor_fd: int, directory: str) -> None:
    """Stay in the group to make cleanup safe if its leader is killed."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    poll = select.poll()
    poll.register(supervisor_fd, select.POLLIN)
    poll.poll()
    try:
        shutil.rmtree(directory, ignore_errors=True)
    finally:
        os.killpg(os.getpgrp(), signal.SIGKILL)


def main(owner_fd: int, handoff_fd: int, directory: str, command: list[str]) -> None:
    stop = False

    def request_stop(_number, _frame):
        nonlocal stop
        stop = True

    group = os.getpid()
    if os.getpgrp() != group:
        raise RuntimeError("OpenSSH supervisor needs a private process group")
    signal.signal(signal.SIGTERM, request_stop)
    poll = select.poll()
    poll.register(owner_fd, select.POLLIN)
    process = None
    try:
        if stop or poll.poll(0):
            return
        with socket.socket(fileno=handoff_fd) as handoff:
            supervisor_fd = open_pidfd(group)
            try:
                guard = os.fork()
                if guard == 0:
                    # The guard keeps the handoff endpoint open until group
                    # cleanup. SSH's child closes its copy before exec.
                    try:
                        _guard(supervisor_fd, directory)
                    finally:
                        os._exit(1)
                guard_fd = open_pidfd(guard)
            finally:
                os.close(supervisor_fd)

            def authorize_ssh():
                # This pre-exec runs in a single-threaded supervisor. The
                # mount must retain the exact SSH pidfd before any dial.
                _linux_call("prctl", 1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG
                if os.getppid() != group:
                    os._exit(1)
                ssh_fd = open_pidfd(os.getpid())
                try:
                    descriptors = array.array("i", [ssh_fd])
                    handoff.sendmsg([struct.pack("!I", os.getpid())],
                                    [(socket.SOL_SOCKET, socket.SCM_RIGHTS, descriptors)])
                    if handoff.recv(1) != b"1":
                        os._exit(1)
                finally:
                    os.close(ssh_fd)
                    handoff.close()

            try:
                process = subprocess.Popen(command, preexec_fn=authorize_ssh)
                poll.register(guard_fd, select.POLLIN)
                while not stop and process.poll() is None and not poll.poll(100):
                    pass
            finally:
                os.close(guard_fd)
    finally:
        if process is not None:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(group, signal.SIGTERM)
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        try:
            shutil.rmtree(directory, ignore_errors=True)
        finally:
            if process is not None:
                os.killpg(group, signal.SIGKILL)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4:])
