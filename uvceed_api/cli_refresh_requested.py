import argparse
import datetime as dt
from typing import List

from .db import db_conn, ensure_phase3_schema
from .refresh import refresh_zip

UTC = dt.timezone.utc

def main():
    ap = argparse.ArgumentParser(description="Refresh all requested ZIP codes (for cron).")
    ap.add_argument("--days", type=int, default=30, help="Only refresh zips requested within last N days")
    ap.add_argument("--force", action="store_true", help="Force refresh regardless of TTL")
    args = ap.parse_args()

    cutoff = dt.datetime.now(UTC) - dt.timedelta(days=args.days)

    with db_conn() as conn:
        ensure_phase3_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zip_code
                FROM zip_requests
                WHERE last_requested_at >= %s
                ORDER BY last_requested_at DESC;
                """,
                (cutoff,),
            )
            zips = [r["zip_code"] for r in cur.fetchall()]

        refreshed_total = 0
        for z in zips:
            refreshed, errors = refresh_zip(conn, z, force=args.force)
            if refreshed:
                refreshed_total += 1
            if errors:
                print(f"WARN {z}: {errors}")

    print(f"OK: processed={len(zips)} refreshed_any={refreshed_total}")

if __name__ == "__main__":
    main()
