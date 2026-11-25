"""Exit when stdin closes, so a fixture stops when the test process dies."""

import os
import sys
import threading


def exit_on_stdin_close() -> None:
    """Start a thread that exits the process when stdin closes."""

    def watch() -> None:
        sys.stdin.read()
        os._exit(0)

    threading.Thread(target=watch, daemon=True).start()
