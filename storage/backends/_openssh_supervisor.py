"""Own a foreground OpenSSH process group for the mounting process's lifetime."""

import os
import select
import shutil
import signal
import subprocess
import sys


def main(owner_fd: int, directory: str, command: list[str]) -> None:
    stop = False

    def request_stop(_number, _frame):
        nonlocal stop
        stop = True

    # The caller starts us as the leader of a private process group.
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
        process = subprocess.Popen(command)
        while not stop and process.poll() is None and not poll.poll(100):
            pass
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
            os.close(owner_fd)
            if process is not None:
                # The group can still contain an SSH ProxyCommand child
                # after ssh itself exits. Signal only this owned group.
                os.killpg(group, signal.SIGKILL)


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2], sys.argv[3:])
