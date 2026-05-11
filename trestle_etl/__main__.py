"""Module entry point for ``python -m trestle_etl``.

Delegates to :func:`trestle_etl.cli.main`. Keeping this file tiny means
the real CLI logic lives in a single place (``cli.py``) that both the
``python -m`` invocation and the ``trestle-etl`` console script (wired
via ``pyproject.toml``) share.

:func:`sys.exit` is called with ``main``'s return value so the shell
sees the exit code contract documented in :mod:`trestle_etl.cli`, not
a plain ``0`` from interpreter shutdown.
"""

from __future__ import annotations

import sys

from trestle_etl.cli import main

if __name__ == "__main__":
    # Forwarding ``sys.argv[1:]`` explicitly rather than relying on
    # argparse's implicit default makes this file exercise the same
    # code path as the ``console_scripts`` entry point.
    sys.exit(main(sys.argv[1:]))
