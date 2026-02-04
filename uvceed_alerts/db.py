# uvceed_alerts/db.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set.")
    return url


def connect():
    # DO managed PG typically requires sslmode=require in DATABASE_URL querystring
    return psycopg.connect(get_db_url(), row_factory=dict_row)


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists wastewater_snapshots (
              id bigserial primary key,
              zip_code text not null,
              county_fips text not null,
              county_name text,
              state_abbr text,
              place text,
              generated_at timestamptz not null,
              days_requested int not null,
              overall_level text,
              overall_trend text,
              overall_confidence text,
              overall_score double precision,
              suggestion text,
              snapshot jsonb not null,
              unique (zip_code, generated_at)
            );
            """
        )
        cur.execute(
            """
            create table if not exists wastewater_pathogen_results (
              id bigserial primary key,
              snapshot_id bigint not null references wastewater_snapshots(id) on delete cascade,
              pathogen text not null,
              dataset_id text,
              pcr_target text,
              window_days int,
              daily_points int,
              metric text,
              last7_median double precision,
              prev7_median double precision,
              risk text,
              trend text,
              confidence text,
              note text,
              risk_score double precision,
              trend_score double precision,
              confidence_score double precision,
              composite_score double precision
            );
            """
        )
        cur.execute(
            """
            create index if not exists idx_ww_snapshots_zip_time
              on wastewater_snapshots(zip_code, generated_at desc);
            """
        )
        cur.execute(
            """
            create index if not exists idx_ww_pathogen_snapshot
              on wastewater_pathogen_results(snapshot_id);
            """
        )
    conn.commit()


def insert_wastewater_snapshot(conn, snapshot_dict: Dict[str, Any]) -> int:
    """
    Inserts one snapshot + its per-pathogen results.
    Returns snapshot_id.
    """
    # snapshot_dict matches asdict(SnapshotResult) in cdc_wastewater.py
    rollup = snapshot_dict.get("rollup") or {}
    results = snapshot_dict.get("results") or []

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into wastewater_snapshots (
              zip_code, county_fips, county_name, state_abbr, place,
              generated_at, days_requested,
              overall_level, overall_trend, overall_confidence, overall_score, suggestion,
              snapshot
            )
            values (
              %(zip_code)s, %(county_fips)s, %(county_name)s, %(state_abbr)s, %(place)s,
              %(generated_at)s::timestamptz, %(days_requested)s,
              %(overall_level)s, %(overall_trend)s, %(overall_confidence)s, %(overall_score)s, %(suggestion)s,
              %(snapshot)s::jsonb
            )
            on conflict (zip_code, generated_at)
            do update set snapshot = excluded.snapshot
            returning id;
            """,
            {
                "zip_code": snapshot_dict.get("zip_code"),
                "county_fips": snapshot_dict.get("county_fips"),
                "county_name": snapshot_dict.get("county_name"),
                "state_abbr": snapshot_dict.get("state_abbr"),
                "place": snapshot_dict.get("place"),
                "generated_at": snapshot_dict.get("generated_at"),
                "days_requested": snapshot_dict.get("days_requested"),
                "overall_level": rollup.get("overall_level"),
                "overall_trend": rollup.get("overall_trend"),
                "overall_confidence": rollup.get("overall_confidence"),
                "overall_score": rollup.get("overall_score"),
                "suggestion": rollup.get("suggestion"),
                "snapshot": json.dumps(snapshot_dict, default=str),
            },
        )
        snapshot_id = cur.fetchone()["id"]

        # Replace pathogen rows for this snapshot_id to keep it clean
        cur.execute("delete from wastewater_pathogen_results where snapshot_id = %s;", (snapshot_id,))

        for r in results:
            cur.execute(
                """
                insert into wastewater_pathogen_results (
                  snapshot_id, pathogen, dataset_id, pcr_target, window_days, daily_points, metric,
                  last7_median, prev7_median, risk, trend, confidence, note,
                  risk_score, trend_score, confidence_score, composite_score
                )
                values (
                  %(snapshot_id)s, %(pathogen)s, %(dataset_id)s, %(pcr_target)s, %(window_days)s, %(daily_points)s, %(metric)s,
                  %(last7_median)s, %(prev7_median)s, %(risk)s, %(trend)s, %(confidence)s, %(note)s,
                  %(risk_score)s, %(trend_score)s, %(confidence_score)s, %(composite_score)s
                );
                """,
                {
                    "snapshot_id": snapshot_id,
                    "pathogen": r.get("pathogen"),
                    "dataset_id": r.get("dataset_id"),
                    "pcr_target": r.get("pcr_target"),
                    "window_days": r.get("window_days"),
                    "daily_points": r.get("daily_points"),
                    "metric": r.get("metric"),
                    "last7_median": r.get("last7_median"),
                    "prev7_median": r.get("prev7_median"),
                    "risk": r.get("risk"),
                    "trend": r.get("trend"),
                    "confidence": r.get("confidence"),
                    "note": r.get("note"),
                    "risk_score": r.get("risk_score"),
                    "trend_score": r.get("trend_score"),
                    "confidence_score": r.get("confidence_score"),
                    "composite_score": r.get("composite_score"),
                },
            )

    conn.commit()
    return snapshot_id

