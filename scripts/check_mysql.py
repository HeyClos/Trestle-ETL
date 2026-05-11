"""Verify MySQL connectivity, local_infile, schema, and grants for the ETL.

Reads the same .env the pipeline reads, connects with pymysql (with
local_infile=True, matching the loader), and reports:

  - TCP reach + auth
  - server version
  - GLOBAL local_infile (must be ON for --full-sync)
  - SESSION local_infile (should follow the client flag)
  - database reachable and selected
  - `property` table present (schema applied)
  - user grants (sanity check for SELECT/INSERT/UPDATE/ALTER)

Exits 0 if everything needed for --full-sync is good. Exits non-zero with
a pointed message otherwise. Does not write any data.
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
import pymysql


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"FAIL: {name} is not set in the environment / .env")
        sys.exit(2)
    return value


def main() -> int:
    load_dotenv()

    host = _require("MYSQL_HOST")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    user = _require("MYSQL_USER")
    password = _require("MYSQL_PASSWORD")
    database = _require("MYSQL_DATABASE")

    print(f"connecting to mysql at {host}:{port} as {user} (db={database})")

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            local_infile=True,
            connect_timeout=10,
        )
    except pymysql.err.OperationalError as exc:
        print(f"FAIL: could not connect: {exc.args}")
        return 1

    exit_code = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            print(f"  connected. server version = {version}")

            cur.execute("SHOW GLOBAL VARIABLES LIKE 'local_infile'")
            row = cur.fetchone()
            global_local_infile = row[1] if row else "<missing>"
            cur.execute("SHOW SESSION VARIABLES LIKE 'local_infile'")
            row = cur.fetchone()
            session_local_infile = row[1] if row else "<missing>"
            print(
                f"  local_infile: global={global_local_infile} "
                f"session={session_local_infile}"
            )
            if global_local_infile != "ON":
                print(
                    "  WARN: GLOBAL local_infile is not ON. --full-sync will "
                    "fail with BulkLoadConfigError until you set "
                    "local_infile=1 under [mysqld] and restart mysqld. "
                    "--incremental does not need this."
                )
                exit_code = max(exit_code, 1)

            cur.execute("SELECT DATABASE()")
            current_db = cur.fetchone()[0]
            print(f"  current database = {current_db}")

            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='property'",
                (database,),
            )
            has_property = cur.fetchone()[0] == 1
            print(f"  property table present: {has_property}")
            if not has_property:
                print(
                    "  WARN: the `property` table does not exist. Apply "
                    "trestle_etl/sql/schema.sql before running the pipeline."
                )
                exit_code = max(exit_code, 1)

            try:
                cur.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants = [r[0] for r in cur.fetchall()]
                print("  grants:")
                for g in grants:
                    print(f"    {g}")
            except pymysql.err.MySQLError as exc:
                print(f"  note: could not read grants ({exc.args})")

    if exit_code == 0:
        print("OK: MySQL is ready for both --full-sync and --incremental.")
    else:
        print("One or more checks above need attention before --full-sync.")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
