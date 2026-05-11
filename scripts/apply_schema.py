"""Apply trestle_etl/sql/schema.sql to the configured MySQL database.

Reads the same .env as the pipeline. Splits the schema on ``;``, strips
SQL line comments and blank lines, and executes each non-empty statement
with pymysql. Idempotent on "already exists" errors so a partial prior
run can be re-applied safely. Exits non-zero if any other error occurs.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
import pymysql


SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent / "trestle_etl" / "sql" / "schema.sql"
)

# MySQL error codes we treat as "already applied" rather than failures.
#   1050 = ER_TABLE_EXISTS_ERROR
#   1061 = ER_DUP_KEYNAME (index already exists)
_ALREADY_EXISTS_ERRNOS = {1050, 1061}


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"FAIL: {name} is not set in the environment / .env")
        sys.exit(2)
    return value


def _split_statements(sql_text: str) -> list[str]:
    """Strip ``--`` line comments and split on ``;`` into non-empty statements."""
    stripped_lines = []
    for line in sql_text.splitlines():
        # Remove everything from `--` onward on each line, preserving the
        # leading content.
        no_comment = re.sub(r"--.*$", "", line)
        stripped_lines.append(no_comment)
    cleaned = "\n".join(stripped_lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def main() -> int:
    load_dotenv()

    host = _require("MYSQL_HOST")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    user = _require("MYSQL_USER")
    password = _require("MYSQL_PASSWORD")
    database = _require("MYSQL_DATABASE")

    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = _split_statements(sql_text)
    print(f"applying {len(statements)} statements from {SCHEMA_PATH}")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        connect_timeout=10,
    )

    failures = 0
    with conn:
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements, 1):
                label = stmt.splitlines()[0][:80]
                try:
                    cur.execute(stmt)
                    print(f"  [{i}/{len(statements)}] OK   {label}")
                except pymysql.err.MySQLError as exc:
                    errno = exc.args[0] if exc.args else None
                    msg = exc.args[1] if len(exc.args) > 1 else str(exc)
                    if errno in _ALREADY_EXISTS_ERRNOS:
                        print(
                            f"  [{i}/{len(statements)}] SKIP {label} "
                            f"(already exists: {errno})"
                        )
                    else:
                        print(
                            f"  [{i}/{len(statements)}] FAIL {label} "
                            f"errno={errno} msg={msg}"
                        )
                        failures += 1

    if failures:
        print(f"done with {failures} failure(s)")
        return 1
    print("done. schema applied cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
