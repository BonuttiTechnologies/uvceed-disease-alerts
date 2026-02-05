import os
import datetime as dt
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, Json

UTC = dt.timezone.utc

def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url

@contextmanager
def db_conn():
    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def ensure_phase3_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_snapshots (
          id bigserial PRIMARY KEY,
          zip_code text NOT NULL,
          signal_type text NOT NULL,
          generated_at timestamptz NOT NULL,
          payload jsonb NOT NULL,
          pathogen text,
          geo_level text,
          geo_id text,
          state text,
          county_fips text,
          risk_level text,
          trend text,
          confidence text,
          composite_score double precision
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_snapshots_zip_type_time
          ON signal_snapshots(zip_code, signal_type, generated_at DESC);
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS zip_requests (
          zip_code text PRIMARY KEY,
          first_requested_at timestamptz NOT NULL DEFAULT now(),
          last_requested_at timestamptz NOT NULL DEFAULT now(),
          last_refreshed_at timestamptz
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_zip_requests_last_requested
          ON zip_requests(last_requested_at DESC);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_zip_requests_last_refreshed
          ON zip_requests(last_refreshed_at DESC);
        """)

def upsert_zip_request(conn, zip_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO zip_requests(zip_code)
            VALUES (%s)
            ON CONFLICT (zip_code)
            DO UPDATE SET last_requested_at = now();
            """,
            (zip_code,),
        )

def mark_zip_refreshed(conn, zip_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE zip_requests
            SET last_refreshed_at = now()
            WHERE zip_code = %s;
            """,
            (zip_code,),
        )

def latest_snapshots(conn, zip_code: str, signal_types: Iterable[str]) -> Dict[str, Optional[Dict[str, Any]]]:
    out: Dict[str, Optional[Dict[str, Any]]] = {}
    with conn.cursor() as cur:
        for st in signal_types:
            cur.execute(
                """
                SELECT *
                FROM signal_snapshots
                WHERE zip_code = %s AND signal_type = %s
                ORDER BY generated_at DESC
                LIMIT 1;
                """,
                (zip_code, st),
            )
            out[st] = cur.fetchone()
    return out

def insert_signal_snapshot(
    conn,
    *,
    zip_code: str,
    signal_type: str,
    generated_at: dt.datetime,
    payload: Dict[str, Any],
    risk_level: Optional[str] = None,
    trend: Optional[str] = None,
    confidence: Optional[str] = None,
    composite_score: Optional[float] = None,
    pathogen: Optional[str] = None,
    state: Optional[str] = None,
    county_fips: Optional[str] = None,
    geo_level: Optional[str] = None,
    geo_id: Optional[str] = None,
) -> int:
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO signal_snapshots(
                zip_code, signal_type, generated_at, payload,
                pathogen, geo_level, geo_id, state, county_fips,
                risk_level, trend, confidence, composite_score
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
            """,
            (
                zip_code,
                signal_type,
                generated_at,
                Json(payload),
                pathogen,
                geo_level,
                geo_id,
                state,
                county_fips,
                risk_level,
                trend,
                confidence,
                composite_score,
            ),
        )
        row = cur.fetchone()
        return int(row["id"])

def advisory_key(zip_code: str, signal_type: str) -> str:
    return f"uvceed_refresh:{zip_code}:{signal_type}"

def try_advisory_lock(conn, key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s)::bigint) AS locked;", (key,))
        row = cur.fetchone()
        return bool(row["locked"])

def advisory_unlock(conn, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint);", (key,))
