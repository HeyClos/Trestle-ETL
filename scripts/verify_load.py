"""Sanity-check the loaded data against the final state file."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import pymysql


def main() -> int:
    load_dotenv()
    conn = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        connect_timeout=10,
    )
    state_path = Path("sync_state.json")
    state = json.loads(state_path.read_text(encoding="utf-8"))

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM property")
            row_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM property_raw")
            raw_row_count = cur.fetchone()[0]

            cur.execute(
                "SELECT MIN(ModificationTimestamp), MAX(ModificationTimestamp) "
                "FROM property"
            )
            min_ts, max_ts = cur.fetchone()

            cur.execute(
                "SELECT COUNT(DISTINCT MlsStatus) FROM property"
            )
            distinct_status = cur.fetchone()[0]

            # Every property row must have a matching raw payload row.
            cur.execute(
                "SELECT COUNT(*) FROM property p "
                "LEFT JOIN property_raw r ON p.ListingKey = r.ListingKey "
                "WHERE r.ListingKey IS NULL"
            )
            orphaned = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name='property' "
                "AND index_name LIKE 'idx_property_%'"
            )
            secondary_indexes = cur.fetchone()[0]

    print(f"property row count        : {row_count:,}")
    print(f"property_raw row count    : {raw_row_count:,}")
    print(f"earliest ModTs            : {min_ts}")
    print(f"latest ModTs              : {max_ts}")
    print(f"state.last_mod_ts         : {state['last_modification_timestamp']}")
    print(f"distinct MlsStatus        : {distinct_status}")
    print(f"property rows w/o raw row : {orphaned}")
    print(f"secondary indexes present : {secondary_indexes} (expect 61)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
