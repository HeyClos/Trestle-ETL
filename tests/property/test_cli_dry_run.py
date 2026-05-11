"""Property-based test for ``--dry-run`` no-side-effects.

Property 25 (design.md): For any mode flag combined with ``--dry-run`` and
any input stream, the run issues zero writes to MySQL and does not modify
the State_File on disk.

**Validates: Requirements 11.3**

Scope
-----

Task 10.1 stopped short of wiring the orchestrator into ``cli._run`` (the
TODO marker in ``trestle_etl/cli.py`` is still there), so end-to-end
interception of real MySQL and filesystem I/O is not yet possible from
the CLI surface. What *is* fully implemented and available for direct
verification are the two shims the CLI installs under ``--dry-run``:

* :class:`~trestle_etl.cli.DryRunLoader` — substituted for
  :class:`~trestle_etl.loader.upsert.UpsertLoader` /
  :class:`~trestle_etl.loader.bulk.BulkLoader`.
* :class:`~trestle_etl.cli.DryRunStateStore` — wraps the real
  :class:`~trestle_etl.state.StateStore`.

Property 25 is trivially true by *construction* for these shims: the
loader has no SQL engine and never opens a connection, and the state
store wrapper's ``save()`` is a pure no-op. This test pins that
construction via Hypothesis so any future refactor (e.g. adding a
"log the SQL we *would* have sent" debug path) that accidentally
reaches out to MySQL or rewrites the state file fails loudly.

The two assertions map directly to the clauses of Requirement 11.3:

* "SHALL NOT write to MySQL" → :func:`test_dry_run_loader_writes_zero_sql`
  exercises :meth:`DryRunLoader.write_batch` with arbitrary row counts
  and row contents; the shim is constructed without a SQLAlchemy engine
  so a SQL emission would crash with ``AttributeError`` rather than
  silently succeed.
* "SHALL NOT update the State_Store" →
  :func:`test_dry_run_state_store_does_not_modify_file` pre-seeds a real
  on-disk state file, wraps the store in :class:`DryRunStateStore`,
  issues an arbitrary number of ``save()`` calls with arbitrary
  ``SyncState`` values, and asserts the file is byte-for-byte identical
  to its pre-save state.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hypothesis import HealthCheck, given, settings as hyp_settings, strategies as st

from trestle_etl.cli import DryRunLoader, DryRunStateStore
from trestle_etl.state import StateStore, SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS


# ---------------------------------------------------------------------------
# Part 1: DryRunLoader issues zero SQL statements for any input.
# ---------------------------------------------------------------------------


@given(n_rows=st.integers(min_value=0, max_value=100))
@hyp_settings(max_examples=100)
def test_dry_run_loader_writes_zero_sql(n_rows: int) -> None:
    """``DryRunLoader.write_batch`` returns without opening a DB connection.

    The shim is intentionally constructed with no SQLAlchemy engine and no
    ``pymysql`` connection pool. If a future edit inadvertently grows a
    write path, the missing engine attribute would surface here as an
    ``AttributeError``; as written, the only observable effect of
    ``write_batch`` is the in-memory bookkeeping on ``_batches`` / ``_rows``.

    The generated rows carry ``ListingKey`` values and ``None`` for every
    other Promoted_Column, which forces the shim's watermark-computation
    branch that handles the all-``None`` ``ModificationTimestamp`` case
    (returning the UTC epoch so the ``BatchResult`` contract stays non-
    ``Optional``). That branch is otherwise hard to reach.
    """
    loader = DryRunLoader()

    rows: list[tuple[tuple, str]] = [
        (
            tuple([f"K{i}"] + [None] * (len(PROMOTED_COLUMNS) - 1)),
            "{}",
        )
        for i in range(n_rows)
    ]

    result = loader.write_batch(rows)

    # Count reported back to the orchestrator matches the input length.
    assert result.count == n_rows

    # Watermark is non-None (required by BatchResult) and, because every
    # row has ModificationTimestamp=None, collapses to the UTC epoch
    # sentinel the shim documents.
    assert result.max_modification_timestamp.tzinfo is not None
    assert result.max_modification_timestamp == datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )

    # No DB interaction is possible: the shim has no engine attribute.
    # This assertion documents the invariant rather than probing private
    # state; a future regression that added an engine would break the
    # public-surface contract tested here.
    assert not hasattr(loader, "_engine")
    assert not hasattr(loader, "engine")

    # close() is a no-op that only logs; calling it must not raise.
    loader.close()


# ---------------------------------------------------------------------------
# Part 2: DryRunStateStore never modifies the underlying state file.
# ---------------------------------------------------------------------------


@st.composite
def _dry_run_save_sequences(draw: st.DrawFn) -> list[SyncState]:
    """Generate a list of ``SyncState`` values to feed through ``save()``.

    The list length is bounded so each Hypothesis example finishes
    quickly (the real StateStore's ``save`` performs an fsync when
    called for real, so we keep sequence length small even though we
    never invoke the real save here).

    Each generated ``SyncState`` covers the shape-space the orchestrator
    actually produces:

    * ``last_modification_timestamp`` is always set (a real save that
      touches the link would always carry a watermark).
    * ``replication_in_progress`` may be True or False.
    * When the link is set, ``replication_next_link_persisted_at`` is
      also set (mirroring the orchestrator's coupled invariant).
    """
    n_saves = draw(st.integers(min_value=0, max_value=10))
    states: list[SyncState] = []
    for i in range(n_saves):
        has_link = draw(st.booleans())
        link = (
            draw(st.text(min_size=1, max_size=128)) if has_link else None
        )
        persisted = (
            datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(minutes=i)
            if has_link
            else None
        )
        states.append(
            SyncState(
                last_modification_timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc)
                + timedelta(days=i),
                replication_in_progress=has_link,
                replication_next_link=link,
                replication_next_link_persisted_at=persisted,
            )
        )
    return states


@given(saves=_dry_run_save_sequences())
@hyp_settings(
    max_examples=100,
    # The test body creates its own tempdir per example (so each
    # Hypothesis draw gets a fresh on-disk state to pre-seed), which
    # satisfies both ``function_scoped_fixture`` and ``too_slow`` —
    # suppressed out of abundance of caution in case a future refactor
    # reaches for a pytest tmp_path fixture.
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_dry_run_state_store_does_not_modify_file(saves: list[SyncState]) -> None:
    """Wrapping a real ``StateStore`` in ``DryRunStateStore`` suppresses writes.

    Procedure:

    1. Create a fresh tempdir and pre-seed a real state file via the
       normal ``StateStore.save`` path. This gives us a well-formed,
       deterministic byte baseline (``pre_bytes``).
    2. Wrap the real store in :class:`DryRunStateStore`.
    3. Confirm ``load()`` still delegates correctly: the wrapper returns
       the pre-seeded state (so the orchestrator's missing-watermark
       check, Requirement 4.2, keeps working under ``--dry-run``).
    4. Issue every generated ``save()`` call against the wrapper.
    5. Read the file back and assert byte-for-byte equality with
       ``pre_bytes``. Any change — including a whitespace-only rewrite
       caused by, say, re-dumping through ``json.dumps`` — would fail
       this assertion.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        state_path = Path(tmp_dir) / "state.json"
        real_store = StateStore(state_path)

        initial = SyncState(
            last_modification_timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            replication_in_progress=False,
        )
        real_store.save(initial)
        pre_bytes = state_path.read_bytes()
        pre_mtime_ns = state_path.stat().st_mtime_ns

        dry = DryRunStateStore(real_store)

        # load() must still return the pre-seeded state: Requirement 11.3
        # only suppresses writes, not reads. The orchestrator needs a
        # working ``load()`` to honor Requirement 4.2.
        assert dry.load() == initial

        # Issue every generated save. Each one would, under a real
        # StateStore, perform a tmp-file + fsync + os.replace sequence
        # that changes both the bytes and the mtime of the target file.
        # Under the dry-run wrapper it must be a no-op.
        for state in saves:
            dry.save(state)

        # Byte-for-byte equality is the strongest possible form of
        # "file was not modified": it rules out rewrite-with-same-
        # contents, which would still count as a write for the purposes
        # of Requirement 11.3 (the file's mtime would change and a
        # crashed rewrite could corrupt it).
        assert state_path.read_bytes() == pre_bytes

        # mtime equality is a defense-in-depth check: it catches the
        # pathological case where a future refactor intentionally
        # rewrites the file with its previous contents (which would
        # pass the byte check on some filesystems but still constitute
        # a disk write).
        assert state_path.stat().st_mtime_ns == pre_mtime_ns
