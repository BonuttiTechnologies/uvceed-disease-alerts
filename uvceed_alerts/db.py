# uvceed_alerts/db.py
"""Shared Postgres helpers for UVCeed disease alerts.

Phase 2 goal: a single canonical snapshot table.

Canonical table:
  signal_snapshots(zip_code, signal_type, generated_at, payload)

Required index:
  (zip_code, signal_type, generated_at DESC)

This module uses psycopg2 for broad compatibility on small VPS/DigitalOcean.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor


SIGNAL_SNAPSHOTS_DDL = r"""
CREATE TABLE IF NOT EXISTS signal_snapshots (
  id bigserial PRIMARY KEY,
  zip_code text NOT NULL,
  signal_type text NOT NULL,
  generated_at timestamptz NOT NULL,
  payload jsonb NOT NULL,

  -- optional standardized fields (nullable)
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

CREATE INDEX IF NOT EXISTS idx_signal_snapshots_zip_type_time
  ON signal_snapshots(zip_code, signal_type, generated_at DESC);
"""


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set.")
    return url


def connect():
    return psycopg2.connect(get_db_url(), cursor_factory=RealDictCursor)


def ensure_signal_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SIGNAL_SNAPSHOTS_DDL)
    conn.commit()


def insert_signal_snapshot(
    conn,
    *,
    zip_code: str,
    signal_type: str,
    generated_at: str,
    payload: Dict[str, Any],
    pathogen: Optional[str] = None,
    geo_level: Optional[str] = None,
    geo_id: Optional[str] = None,
    state: Optional[str] = None,
    county_fips: Optional[str] = None,
    risk_level: Optional[str] = None,
    trend: Optional[str] = None,
    confidence: Optional[str] = None,
    composite_score: Optional[float] = None,
) -> int:
    """Insert one row into signal_snapshots; returns new id."""
    ensure_signal_schema(conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO signal_snapshots
              (zip_code, signal_type, generated_at, payload,
               pathogen, geo_level, geo_id, state, county_fips,
               risk_level, trend, confidence, composite_score)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                zip_code,
                signal_type,
                generated_at,
                json.dumps(payload),
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
        new_id = cur.fetchone()["id"]
    conn.commit()
    return int(new_id)


def get_latest_snapshot(conn, *, zip_code: str, signal_type: str) -> Optional[Dict[str, Any]]:
    ensure_signal_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM signal_snapshots
            WHERE zip_code=%s AND signal_type=%s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (zip_code, signal_type),
        )
        return cur.fetchone()
