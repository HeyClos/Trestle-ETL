"""Durable pipeline state persistence.

The State_Store is a JSON document on local disk that records just enough
information to let a crashed run either resume a replication stream or
safely pivot to incremental sync. It is the only component that writes to
``sync_state.json`` and it is written only *after* each batch commit
succeeds; that sequencing is what makes the crash-recovery invariant
(Requirement 15.1, 15.2, 15.3) hold.

This module implements Requirements 9.1, 9.2, 9.6, 9.7, and 9.8:

* The state is a JSON document at a configurable path (default
  ``sync_state.json``) with the fields enumerated by Requirement 9.2 plus
  the ``replication_next_link_persisted_at`` field that the 4-minute
  freshness check (Requirement 3.8) needs.
* ``load()`` treats a missing file as an un-initialized pipeline and
  returns a default ``SyncState`` (Requirement 9.7).
* ``load()`` raises ``CorruptStateError`` when the file is present but
  malformed and **does not modify the file** in that case
  (Requirement 9.8), leaving it available for the operator to inspect or
  repair.
* ``save()`` writes atomically by creating a sibling ``<path>.tmp`` file,
  ``fsync``-ing its contents to disk, and then ``os.rename``-ing it over
  the target. On POSIX this rename is atomic, so a crash at any point
  leaves either the prior state or the new state on disk, never a
  partially-written document (Requirement 9.6).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import CorruptStateError

# Keys in the on-disk JSON document. Centralizing them here keeps the
# serializer and deserializer aligned with a single source of truth and
# makes grep-driven refactors safe.
_KEY_LAST_MOD_TS = "last_modification_timestamp"
_KEY_REPLICATION_IN_PROGRESS = "replication_in_progress"
_KEY_REPLICATION_NEXT_LINK = "replication_next_link"
_KEY_REPLICATION_NEXT_LINK_PERSISTED_AT = "replication_next_link_persisted_at"

_ALL_KEYS = frozenset(
    {
        _KEY_LAST_MOD_TS,
        _KEY_REPLICATION_IN_PROGRESS,
        _KEY_REPLICATION_NEXT_LINK,
        _KEY_REPLICATION_NEXT_LINK_PERSISTED_AT,
    }
)


@dataclass
class SyncState:
    """In-memory representation of the pipeline's persisted progress.

    A default-constructed ``SyncState`` represents an un-initialized
    pipeline: no commits have happened yet and there is no replication in
    progress. ``StateStore.load`` returns this shape when the state file is
    absent (Requirement 9.7).

    Attributes:
        last_modification_timestamp:
            The highest ``ModificationTimestamp`` of any batch that has
            committed successfully. ``None`` means no batch has ever
            committed for this pipeline instance.
        replication_in_progress:
            ``True`` while a full-sync replication stream is mid-flight.
            Cleared to ``False`` when the stream terminates cleanly.
        replication_next_link:
            The most recent ``@odata.nextLink`` URL observed from the
            replication endpoint, or ``None`` when no stream is active.
        replication_next_link_persisted_at:
            Wall-clock UTC timestamp recorded at the moment
            ``replication_next_link`` was last written. Used by the
            orchestrator's 4-minute freshness check (Requirement 3.8) to
            decide between resume and pivot-to-incremental. ``None`` when
            ``replication_next_link`` is ``None``.
    """

    last_modification_timestamp: datetime | None = None
    replication_in_progress: bool = False
    replication_next_link: str | None = None
    replication_next_link_persisted_at: datetime | None = None


class StateStore:
    """Atomic reader/writer for the pipeline's JSON state document.

    Construction is cheap and performs no I/O; the file is touched only on
    ``load()`` and ``save()``. A single ``StateStore`` instance is intended
    to live for the duration of a single pipeline run.
    """

    def __init__(self, path: Path) -> None:
        # Stored as a Path so callers can pass either str or Path; the rest
        # of the module assumes Path semantics (``.parent``, ``.with_name``).
        self._path = Path(path)

    @property
    def path(self) -> Path:
        """Return the configured state-file path (for logging/diagnostics)."""
        return self._path

    # ------------------------------------------------------------------ load

    def load(self) -> SyncState:
        """Read the state file and return a ``SyncState``.

        Returns a default-constructed ``SyncState`` when the file does not
        exist, treating that as an un-initialized pipeline
        (Requirement 9.7).

        Raises:
            CorruptStateError: The file exists but cannot be parsed or does
                not match the expected schema. The file is **not** modified
                in this case (Requirement 9.8).
        """
        try:
            # Read as bytes first so we never silently paper over an
            # encoding issue; the JSON parser will surface a clear error.
            raw_bytes = self._path.read_bytes()
        except FileNotFoundError:
            # Missing file == un-initialized pipeline. This is the normal
            # first-run case and must not be an error (Requirement 9.7).
            return SyncState()

        try:
            document = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            # Requirement 9.8: do not modify the file. Since we only read
            # above, the file is untouched; we just surface a typed error.
            raise CorruptStateError(
                f"State file {self._path} is not valid JSON: {exc}"
            ) from exc

        return _deserialize(document, self._path)

    # ------------------------------------------------------------------ save

    def save(self, state: SyncState) -> None:
        """Persist ``state`` to disk atomically.

        The write sequence is:

        1. Serialize ``state`` to JSON bytes.
        2. Write the bytes to a sibling ``<path>.tmp`` file in the same
           directory as the target (same-directory is important because
           ``os.rename`` is only guaranteed atomic within a single
           filesystem, and putting the tmp file in the same directory as
           the target guarantees that).
        3. ``fsync`` the tmp file so its contents are durably on disk
           before we publish the new state via rename.
        4. ``os.rename`` the tmp file over the target. On POSIX this is an
           atomic filesystem operation: a concurrent reader sees either
           the old or the new file, never a partial document
           (Requirement 9.6).

        On any failure during the write, the tmp file is removed so we
        don't leave turds on disk that could confuse the next run.
        """
        document = _serialize(state)
        payload = json.dumps(document, indent=2, sort_keys=True).encode("utf-8")

        # Ensure the target directory exists; running the pipeline for the
        # first time with a configured STATE_FILE_PATH that points into a
        # not-yet-created directory is a reasonable scenario.
        parent = self._path.parent if str(self._path.parent) else Path(".")
        parent.mkdir(parents=True, exist_ok=True)

        tmp_path = self._path.with_name(self._path.name + ".tmp")

        # Open with os.open so we can fsync the specific file descriptor
        # before close; a naive write_bytes + fsync dance would have to
        # re-open the file just to fsync it.
        fd = os.open(
            tmp_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o644,
        )
        try:
            try:
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            # os.replace is POSIX-atomic like os.rename but also works on
            # Windows where rename over an existing file fails. We want
            # rename-over-existing semantics.
            os.replace(tmp_path, self._path)
        except Exception:
            # Best-effort cleanup: if the rename didn't happen, the tmp
            # file is garbage. Swallow cleanup errors so we don't mask
            # the original exception.
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise


# ---------------------------------------------------------------------------
# Serialization helpers (module-private).
#
# Kept as free functions rather than SyncState methods so that the dataclass
# remains a simple data container and so that unit tests can exercise the
# (de)serialization logic without constructing a StateStore.
# ---------------------------------------------------------------------------


def _serialize(state: SyncState) -> dict[str, Any]:
    """Turn a ``SyncState`` into a JSON-serializable dict.

    Timestamps are serialized as ISO 8601 strings with an explicit
    ``+00:00`` UTC offset (per the design's state-file schema). Naive
    datetimes are rejected: they would round-trip ambiguously since
    ``datetime.fromisoformat`` returns a naive datetime for an offset-less
    string and we cannot guarantee the original intent was UTC.
    """
    return {
        _KEY_LAST_MOD_TS: _encode_datetime(state.last_modification_timestamp),
        _KEY_REPLICATION_IN_PROGRESS: bool(state.replication_in_progress),
        _KEY_REPLICATION_NEXT_LINK: state.replication_next_link,
        _KEY_REPLICATION_NEXT_LINK_PERSISTED_AT: _encode_datetime(
            state.replication_next_link_persisted_at
        ),
    }


def _deserialize(document: Any, path: Path) -> SyncState:
    """Turn a parsed JSON document into a ``SyncState``.

    Every schema violation is reported as ``CorruptStateError`` with a
    message that includes the path (so operators can act on the log entry
    alone) and the specific field at fault.
    """
    if not isinstance(document, dict):
        raise CorruptStateError(
            f"State file {path} must contain a JSON object, got "
            f"{type(document).__name__}"
        )

    # Unknown keys are tolerated (forward-compat), but any REQUIRED key
    # whose value is of the wrong type is a hard failure. We don't require
    # every key to be present: a state file written by an older version of
    # the pipeline might be missing replication_next_link_persisted_at,
    # and treating absent as None is the natural forward-compat story.
    unexpected = set(document.keys()) - _ALL_KEYS
    if unexpected:
        # Unexpected keys indicate schema drift; surface rather than
        # silently drop them. The file is NOT modified (Requirement 9.8).
        raise CorruptStateError(
            f"State file {path} contains unexpected keys: "
            f"{sorted(unexpected)}"
        )

    last_mod = _decode_datetime(
        document.get(_KEY_LAST_MOD_TS), _KEY_LAST_MOD_TS, path
    )

    in_progress_raw = document.get(_KEY_REPLICATION_IN_PROGRESS, False)
    if not isinstance(in_progress_raw, bool):
        raise CorruptStateError(
            f"State file {path} field {_KEY_REPLICATION_IN_PROGRESS!r} must "
            f"be a boolean, got {type(in_progress_raw).__name__}"
        )

    next_link_raw = document.get(_KEY_REPLICATION_NEXT_LINK)
    if next_link_raw is not None and not isinstance(next_link_raw, str):
        raise CorruptStateError(
            f"State file {path} field {_KEY_REPLICATION_NEXT_LINK!r} must "
            f"be a string or null, got {type(next_link_raw).__name__}"
        )

    persisted_at = _decode_datetime(
        document.get(_KEY_REPLICATION_NEXT_LINK_PERSISTED_AT),
        _KEY_REPLICATION_NEXT_LINK_PERSISTED_AT,
        path,
    )

    return SyncState(
        last_modification_timestamp=last_mod,
        replication_in_progress=in_progress_raw,
        replication_next_link=next_link_raw,
        replication_next_link_persisted_at=persisted_at,
    )


def _encode_datetime(value: datetime | None) -> str | None:
    """Encode a tz-aware UTC datetime as an ISO 8601 string with ``+00:00``.

    Accepts any tz-aware datetime: non-UTC values are normalized to UTC so
    the on-disk representation is always in UTC with an explicit offset.
    Naive datetimes are rejected because their timezone intent is unknown;
    silently assuming UTC would be a correctness footgun.
    """
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(
            "SyncState datetimes must be timezone-aware (UTC); got a naive "
            "datetime, which cannot be round-tripped safely."
        )
    # Normalize to UTC so the on-disk document is always canonical,
    # regardless of the caller's tz. isoformat() produces "+00:00" for a
    # UTC datetime, matching the design's state-file schema.
    return value.astimezone(timezone.utc).isoformat()


def _decode_datetime(
    value: Any,
    field_name: str,
    path: Path,
) -> datetime | None:
    """Decode an ISO 8601 string back into a tz-aware UTC datetime.

    ``None`` passes through as ``None``. Strings that lack timezone
    information are rejected: the on-disk contract (per ``_encode_datetime``)
    always includes an offset, so an offset-less string is a corruption
    signal, not a legitimate value.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise CorruptStateError(
            f"State file {path} field {field_name!r} must be an ISO 8601 "
            f"string or null, got {type(value).__name__}"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CorruptStateError(
            f"State file {path} field {field_name!r} is not a valid ISO "
            f"8601 timestamp: {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise CorruptStateError(
            f"State file {path} field {field_name!r} is missing timezone "
            f"information; timestamps must include an explicit UTC offset"
        )
    # Normalize to UTC so callers never have to think about offsets.
    return parsed.astimezone(timezone.utc)


__all__ = ["SyncState", "StateStore"]
