"""Unit tests for the single-instance run lock (:mod:`trestle_etl.lock`).

The lock exists to stop two concurrent real runs from racing on the
atomic ``sync_state.json`` replace — the failure mode that previously
corrupted progress tracking when two ``--full-sync`` invocations
overlapped. These tests pin the behaviors the CLI depends on:

* a fresh lock can be acquired and records the holder's PID;
* a second acquisition of the same lock path fails fast with
  :class:`~trestle_etl.errors.PipelineLockError` (not a block/queue);
* the conflict message names the lock path and the holder's PID so an
  operator can find the running process;
* releasing the lock lets a later acquisition succeed;
* :meth:`PipelineLock.release` is idempotent;
* the context-manager form acquires on enter and releases on exit;
* :func:`default_lock_path` derives ``<state>.lock`` next to the state
  file so runs against different state files do not contend.

The two-handles-one-path tests rely on ``fcntl.flock`` treating two
separate ``os.open`` descriptions as distinct lock owners even within a
single process, which holds on macOS and Linux (the pipeline's POSIX
deployment target).
"""

from __future__ import annotations

import os

import pytest

from trestle_etl.errors import PipelineLockError
from trestle_etl.lock import PipelineLock, default_lock_path


def test_default_lock_path_sits_beside_state_file(tmp_path):
    """``default_lock_path`` appends ``.lock`` to the state file name."""
    state = tmp_path / "sync_state.json"
    assert default_lock_path(state) == tmp_path / "sync_state.json.lock"


def test_acquire_creates_file_and_records_pid(tmp_path):
    """A fresh acquire creates the lock file and writes the holder PID."""
    lock_path = tmp_path / "state.json.lock"
    lock = PipelineLock(lock_path)

    lock.acquire()
    try:
        assert lock_path.exists()
        assert lock_path.read_text().strip() == str(os.getpid())
    finally:
        lock.release()


def test_second_acquire_same_path_raises(tmp_path):
    """A second lock on the same path fails fast rather than blocking."""
    lock_path = tmp_path / "state.json.lock"
    first = PipelineLock(lock_path)
    second = PipelineLock(lock_path)

    first.acquire()
    try:
        with pytest.raises(PipelineLockError):
            second.acquire()
    finally:
        first.release()


def test_conflict_message_names_path_and_holder_pid(tmp_path):
    """The conflict error identifies the lock path and the holder PID."""
    lock_path = tmp_path / "state.json.lock"
    first = PipelineLock(lock_path)
    second = PipelineLock(lock_path)

    first.acquire()
    try:
        with pytest.raises(PipelineLockError) as excinfo:
            second.acquire()
    finally:
        first.release()

    message = str(excinfo.value)
    assert "state.json.lock" in message
    assert str(os.getpid()) in message


def test_release_allows_reacquire(tmp_path):
    """After the holder releases, another instance can acquire the lock."""
    lock_path = tmp_path / "state.json.lock"
    first = PipelineLock(lock_path)
    second = PipelineLock(lock_path)

    first.acquire()
    first.release()

    # Should not raise now that the first holder has let go.
    second.acquire()
    try:
        assert lock_path.read_text().strip() == str(os.getpid())
    finally:
        second.release()


def test_release_is_idempotent(tmp_path):
    """Calling release twice (or without acquire) is a harmless no-op."""
    lock_path = tmp_path / "state.json.lock"
    lock = PipelineLock(lock_path)

    # Release before any acquire: no error.
    lock.release()

    lock.acquire()
    lock.release()
    # Second release after a real acquire/release: still no error.
    lock.release()


def test_context_manager_acquires_and_releases(tmp_path):
    """The context-manager form holds the lock in the body and frees it after."""
    lock_path = tmp_path / "state.json.lock"

    with PipelineLock(lock_path):
        # While held, a second acquisition must fail.
        contender = PipelineLock(lock_path)
        with pytest.raises(PipelineLockError):
            contender.acquire()

    # After the block exits, the lock is free and can be taken again.
    after = PipelineLock(lock_path)
    after.acquire()
    after.release()


def test_different_paths_do_not_contend(tmp_path):
    """Locks on different paths are independent and do not block each other."""
    lock_a = PipelineLock(tmp_path / "a.json.lock")
    lock_b = PipelineLock(tmp_path / "b.json.lock")

    lock_a.acquire()
    try:
        # A different path acquires cleanly while the first is held.
        lock_b.acquire()
        lock_b.release()
    finally:
        lock_a.release()
