"""Property test for CLI flag combination validation (Property 26).

Property 26 (design.md): For any subset of CLI flags drawn from
``{--full-sync, --incremental, --reconcile, --dry-run, --since=<ts>}``,
the CLI exits non-zero with a usage error iff the combination violates
the rules (more than one of the three mode flags, or ``--dry-run`` with
``--reconcile``); all other combinations are accepted.

**Validates: Requirements 11.2**

Implementation notes
--------------------

The CLI splits flag-combination enforcement into two layers:

* :func:`trestle_etl.cli._build_parser` constructs the argparse parser.
  It does not enforce mutual exclusion across the three mode flags
  because argparse's ``add_mutually_exclusive_group`` cannot express the
  "``--dry-run`` modifier is allowed with any single mode flag" rule
  without losing the specificity of the error messages.
* :func:`trestle_etl.cli._validate_args` enforces the semantic rules
  encoded in Requirement 11.2:

  - More than one of ``{--full-sync, --incremental, --reconcile}`` is a
    usage error.
  - ``--dry-run`` combined with ``--reconcile`` is a usage error.
  - No mode flag AND no ``--since`` is a usage error.

This test exercises the boundary where those rules are enforced. It
generates every 2^4 * 2 = 32 combination of the four boolean flags plus
"``--since`` supplied or not", pushes each through ``_build_parser`` to
match the real CLI flow, and asserts that ``_validate_args`` raises
:class:`~trestle_etl.errors.UsageError` iff the combination violates
the rules.

Because the combination space is small, the test also restates the
expected-invalid predicate independently so that regressions in the
validator logic surface as test failures, not as a tautology where both
sides of the assertion drift together.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from trestle_etl.cli import _build_parser, _validate_args
from trestle_etl.errors import UsageError

# A single fixed ISO 8601 timestamp is sufficient here. Property 26
# constrains the COMBINATION of flags that are present vs absent; the
# value carried by ``--since`` is irrelevant to the combination rules
# and is exercised separately by Property 27 (Task 10.4).
_FIXED_SINCE = "2024-06-01T00:00:00Z"


@given(
    full_sync=st.booleans(),
    incremental=st.booleans(),
    reconcile=st.booleans(),
    dry_run=st.booleans(),
    # Model ``--since`` as "present with a valid value" vs "absent".
    # The validation rules in Req 11.2 depend only on presence, so the
    # two-element strategy covers the full space without inflating
    # ``max_examples`` against repeated identical draws.
    since=st.one_of(st.none(), st.just(_FIXED_SINCE)),
)
@settings(max_examples=100)
def test_flag_combinations_accept_iff_rules_satisfied(
    full_sync: bool,
    incremental: bool,
    reconcile: bool,
    dry_run: bool,
    since: str | None,
) -> None:
    """Property 26 (Requirements 11.2).

    Build the ``argv`` list from the drawn flag presences, parse it
    through the real parser, and assert that ``_validate_args`` either
    accepts the combination or raises :class:`UsageError` in agreement
    with the independent predicate defined here.
    """
    # Build argv exactly as a shell would: each boolean flag contributes
    # a single token, and ``--since`` contributes a pair ``[--since, value]``
    # only when present.
    argv: list[str] = []
    if full_sync:
        argv.append("--full-sync")
    if incremental:
        argv.append("--incremental")
    if reconcile:
        argv.append("--reconcile")
    if dry_run:
        argv.append("--dry-run")
    if since is not None:
        argv.extend(["--since", since])

    # Parse through the real parser so we're validating the same
    # ``argparse.Namespace`` shape the CLI consumes. Using a hand-built
    # namespace would drift from the parser's attribute names (dashes
    # vs underscores) and from any defaults argparse supplies.
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Independent predicate derived directly from Requirement 11.2. The
    # test oracle is this predicate, NOT ``_validate_args`` itself, so a
    # regression that flips a condition in the validator will surface as
    # a test failure rather than silently match an equivalent bug in
    # both sides.
    n_modes = int(full_sync) + int(incremental) + int(reconcile)
    too_many_modes = n_modes > 1
    dry_run_with_reconcile = dry_run and reconcile
    no_mode_and_no_since = n_modes == 0 and since is None
    expect_invalid = too_many_modes or dry_run_with_reconcile or no_mode_and_no_since

    if expect_invalid:
        with pytest.raises(UsageError):
            _validate_args(args)
    else:
        # Must not raise. Any exception — UsageError or otherwise — is
        # a regression: UsageError would mean the validator rejected a
        # combination Req 11.2 says to accept, and any other exception
        # would mean the validator crashed on valid input.
        _validate_args(args)
