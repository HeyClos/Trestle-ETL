"""Unit tests for corrupt state-file handling in ``trestle_etl.state``.

Validates Requirement 9.8: when the state file is present but malformed,
``StateStore.load()`` SHALL raise ``CorruptStateError`` and SHALL NOT
modify the file. The operator needs the file preserved byte-for-byte so
they can inspect or repair it.

Also exercises Requirement 9.7 (missing file returns a default, empty
``SyncState``) since that is the other half of the load() error-handling
contract and is cheap to cover in the same file.

Each corruption variant below represents a distinct failure mode that
``_deserialize`` in ``state.py`` rejects:

* raw bytes that are not valid UTF-8 or not valid JSON
* valid JSON whose top-level value is not an object
* unknown top-level keys (schema drift signal)
* a required field with the wrong Python type after JSON decoding
* a timestamp field whose string is not a valid ISO 8601 value
* a timestamp field whose ISO 8601 value lacks a timezone offset

For every variant the file is written, ``StateStore(path).load()`` is
called, and we assert both that ``CorruptStateError`` is raised AND that
the on-disk bytes are unchanged after the call. The byte-level comparison
is what proves Requirement 9.8's "does not modify" clause.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from trestle_etl.errors import CorruptStateError
from trestle_etl.state import StateStore, SyncState


def _assert_load_raises_and_file_unchanged(
    path: Path, original_bytes: bytes
) -> None:
    """Helper: load() raises CorruptStateError AND the file is untouched.

    Reads the file bytes AFTER the load() call and compares against what
    was written originally. Using a byte-level comparison (rather than
    ``json.loads`` equality) is deliberate: Requirement 9.8 speaks to the
    physical file, not a semantic equivalence, so a loader that rewrote
    the file with, say, re-sorted keys would be a bug even if the parsed
    content were unchanged.
    """
    store = StateStore(path)

    with pytest.raises(CorruptStateError):
        store.load()

    # Read back as bytes (never text) to catch any encoding-level edits.
    assert path.read_bytes() == original_bytes


# ---------------------------------------------------------------------------
# Malformed JSON (the canonical corruption case called out by Req 9.8)
# ---------------------------------------------------------------------------


def test_malformed_json_raises_and_leaves_file_unmodified(tmp_path: Path) -> None:
    """Truly broken JSON bytes: load() raises, file byte-identical after."""
    path = tmp_path / "sync_state.json"
    # Unclosed brace + stray garbage: json.loads will reject this outright.
    malformed = b'{"last_modification_timestamp": "2024-03-14T00:00:00+00:00",'
    path.write_bytes(malformed)

    _assert_load_raises_and_file_unchanged(path, malformed)


def test_non_utf8_bytes_raise_corrupt_state_error(tmp_path: Path) -> None:
    """Bytes that aren't valid UTF-8 surface as CorruptStateError, not UnicodeDecodeError.

    ``state.load()`` decodes bytes explicitly so it can wrap the decoding
    failure in a typed error; that wrapping is what the caller contract
    depends on.
    """
    path = tmp_path / "sync_state.json"
    # 0xFF is not a legal UTF-8 start byte; decoding will fail.
    bad_bytes = b"\xff\xfe\xfd not json at all"
    path.write_bytes(bad_bytes)

    _assert_load_raises_and_file_unchanged(path, bad_bytes)


# ---------------------------------------------------------------------------
# Valid JSON, invalid state-file schema
# ---------------------------------------------------------------------------


def test_top_level_array_raises_corrupt_state_error(tmp_path: Path) -> None:
    """Valid JSON whose root is a list, not an object."""
    path = tmp_path / "sync_state.json"
    # A JSON array parses cleanly but violates the state-file schema,
    # which mandates a top-level object.
    original = b'["not", "a", "state", "object"]'
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_top_level_string_raises_corrupt_state_error(tmp_path: Path) -> None:
    """Valid JSON whose root is a bare string."""
    path = tmp_path / "sync_state.json"
    original = b'"just a string"'
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_unknown_top_level_key_raises_corrupt_state_error(tmp_path: Path) -> None:
    """An otherwise-valid document with an unexpected key signals schema drift."""
    path = tmp_path / "sync_state.json"
    document = {
        "last_modification_timestamp": "2024-03-14T17:32:00+00:00",
        "replication_in_progress": False,
        "replication_next_link": None,
        "replication_next_link_persisted_at": None,
        # This key is not in the known schema; surfacing it as an error
        # is safer than silently dropping it because it likely indicates
        # a state file written by a newer version of the pipeline.
        "unexpected_future_field": "whoops",
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_wrong_type_for_replication_in_progress_raises(tmp_path: Path) -> None:
    """``replication_in_progress`` must be a JSON boolean, not a string."""
    path = tmp_path / "sync_state.json"
    document = {
        "last_modification_timestamp": "2024-03-14T17:32:00+00:00",
        # A reasonable-looking misconfiguration: someone typing "yes"
        # instead of true. The deserializer must refuse it rather than
        # coerce, because truthiness in Python does not match the file
        # contract.
        "replication_in_progress": "yes",
        "replication_next_link": None,
        "replication_next_link_persisted_at": None,
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_wrong_type_for_replication_next_link_raises(tmp_path: Path) -> None:
    """``replication_next_link`` must be a string or null, not an integer."""
    path = tmp_path / "sync_state.json"
    document = {
        "last_modification_timestamp": None,
        "replication_in_progress": True,
        "replication_next_link": 12345,
        "replication_next_link_persisted_at": None,
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_bad_iso8601_timestamp_raises(tmp_path: Path) -> None:
    """Timestamp field that is a string but not parseable as ISO 8601."""
    path = tmp_path / "sync_state.json"
    document = {
        # "not-a-timestamp" decodes as a JSON string but datetime.fromisoformat
        # will reject it; the deserializer wraps the ValueError.
        "last_modification_timestamp": "not-a-timestamp",
        "replication_in_progress": False,
        "replication_next_link": None,
        "replication_next_link_persisted_at": None,
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_naive_timestamp_without_offset_raises(tmp_path: Path) -> None:
    """Timestamp missing the explicit UTC offset is corruption, not a default.

    The on-disk contract (see ``_encode_datetime``) always writes a
    ``+00:00`` suffix. A string without one would round-trip as a naive
    datetime, and the design deliberately refuses to silently assume UTC.
    """
    path = tmp_path / "sync_state.json"
    document = {
        # No trailing "+00:00" — this parses as a naive datetime, which
        # the deserializer rejects.
        "last_modification_timestamp": "2024-03-14T17:32:00",
        "replication_in_progress": False,
        "replication_next_link": None,
        "replication_next_link_persisted_at": None,
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


def test_timestamp_as_non_string_raises(tmp_path: Path) -> None:
    """Timestamp encoded as an integer (epoch seconds) is not our contract."""
    path = tmp_path / "sync_state.json"
    document = {
        # Unix epoch integers are a common alternative encoding but are
        # NOT what this pipeline writes; reject rather than guess.
        "last_modification_timestamp": 1710438720,
        "replication_in_progress": False,
        "replication_next_link": None,
        "replication_next_link_persisted_at": None,
    }
    original = json.dumps(document).encode("utf-8")
    path.write_bytes(original)

    _assert_load_raises_and_file_unchanged(path, original)


# ---------------------------------------------------------------------------
# Req 9.7: missing file returns a default SyncState
# ---------------------------------------------------------------------------


def test_missing_file_returns_default_syncstate(tmp_path: Path) -> None:
    """Absent state file is an un-initialized pipeline, not an error.

    Validates Requirement 9.7. The returned state must be the same as a
    default-constructed ``SyncState`` so the orchestrator can reliably
    check ``last_modification_timestamp is None`` as the "first run"
    signal.
    """
    # Point at a path that definitely does not exist.
    path = tmp_path / "does-not-exist.json"
    assert not path.exists()

    state = StateStore(path).load()

    assert state == SyncState()
    assert state.last_modification_timestamp is None
    assert state.replication_in_progress is False
    assert state.replication_next_link is None
    assert state.replication_next_link_persisted_at is None
    # load() must not have created the file as a side effect.
    assert not path.exists()
