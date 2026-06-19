"""Single-instance run lock for the Trestle ETL pipeline.

Two concurrent pipeline invocations against the same State_Store race on
the atomic ``sync_state.json`` replace: each writes ``sync_state.json.tmp``
and renames it over the target, so one process can rename the tmp away
before the other's rename runs, surfacing as a ``FileNotFoundError`` and,
worse, interleaving progress writes. This module provides an advisory file
lock that lets exactly one real run proceed at a time.

Design choices:

* **``fcntl.flock`` advisory lock**, not a bare PID file. The kernel
  releases an ``flock`` automatically when the holding process exits —
  including on crash or ``SIGKILL`` — so there is no stale-lock problem to
  reap. A plain PID file would survive a crash and require fragile
  "is that PID still alive?" heuristics.
* **Non-blocking acquisition** (``LOCK_NB``): a second invocation fails
  fast with :class:`~trestle_etl.errors.PipelineLockError` rather than
  silently queueing behind the running one.
* The holder's **PID is written into the lock file** purely for
  diagnostics, so the error message can name the process currently
  holding the lock.

POSIX only (macOS/Linux), which matches the pipeline's deployment target.
"""

from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path
from typing import Optional

from .errors import PipelineLockError

logger = logging.getLogger(__name__)


def default_lock_path(state_file_path: Path) -> Path:
    """Return the lock path that sits alongside the State_Store file.

    Co-locating the lock with ``sync_state.json`` ties the lock's scope to
    exactly the resource it protects: two invocations pointed at different
    state files (e.g. separate environments) get separate locks and do not
    block each other, while two pointed at the same state file contend.
    """
    return state_file_path.with_name(state_file_path.name + ".lock")


class PipelineLock:
    """Advisory single-instance lock backed by ``fcntl.flock``.

    Usable as a context manager::

        with PipelineLock(path):
            run_pipeline()

    or via explicit :meth:`acquire` / :meth:`release` when the surrounding
    code already manages its own ``try/finally`` (as the CLI does).
    """

    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        """Take the lock or raise :class:`PipelineLockError` if held.

        Opens (creating if needed) the lock file and requests a
        non-blocking exclusive ``flock``. On contention, reads the PID the
        current holder recorded so the error message can identify it, then
        raises without holding any descriptor.
        """
        # Ensure the parent directory exists; the state file may point into
        # a not-yet-created directory on a first run.
        parent = self._lock_path.parent if str(self._lock_path.parent) else Path(".")
        parent.mkdir(parents=True, exist_ok=True)

        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            # Another process holds the lock. Read whatever PID it wrote so
            # the operator can find it, then release our descriptor (NOT
            # the lock, which we never acquired) and fail.
            holder = self._read_holder(fd)
            os.close(fd)
            raise PipelineLockError(
                f"another pipeline run is already in progress "
                f"(lock: {self._lock_path}, held by PID {holder}). "
                f"Wait for it to finish or terminate it before retrying."
            ) from exc

        # Acquired. Record our PID for diagnostics, replacing any stale
        # content the previous (now-exited) holder left behind.
        self._fd = fd
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(fd)
        except OSError:
            # PID bookkeeping is best-effort; failing to write it does not
            # invalidate the lock we already hold.
            logger.warning("could not record PID in lock file %s", self._lock_path)
        logger.info("acquired pipeline lock %s (pid=%d)", self._lock_path, os.getpid())

    def release(self) -> None:
        """Release the lock and close the descriptor. Idempotent."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        logger.info("released pipeline lock %s", self._lock_path)

    @staticmethod
    def _read_holder(fd: int) -> str:
        """Best-effort read of the PID recorded by the current lock holder."""
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            data = os.read(fd, 64).decode("ascii", errors="replace").strip()
        except OSError:
            return "<unknown>"
        return data or "<unknown>"

    def __enter__(self) -> "PipelineLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


__all__ = ["PipelineLock", "default_lock_path"]
