#!/usr/bin/env python3
"""
CDC Wastewater Surveillance (Socrata dataset j9g8-acpt)

Supports:
- ZIP-based lookup (via county / state rollup)
- JSON output
- DB persistence into shared `signal_snapshots` table
"""

import argparse
import datetime as dt
import json
import os
from statistics import median
from typing import Dict, List, Optional

import psycopg2
import requests

from uvceed_alerts.geo import lookup_zip
from uvceed_alerts.config import (
    CDC_APP_TOKEN,
)

DATASET_ID = "j9g8-acpt"
BASE_URL = f"https://data.cdc.gov/resource/{DATASET_ID}.json"

DEFAULT_WINDOW_DAYS = 60
FALLBACK_WINDOW_DAYS = 180

PATHOGENS = {
    "covid": "sars-cov-2",
    "flu_a": "influenza-a",
    "rsv": "rsv",
}


# -----------------------------
# Utilities
# -----------------------------

def socrata_get(params: Dict) -> List[Dict]:
    headers = {}
    if CDC_APP_TOKEN:
        headers["X-App-Token"] = CDC_APP_TOKEN

    r = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def risk_from_value(val: Optional[float]) -> str:
    if val is None:
        return "unknown"
    if val >= 3e5:
        return "high"
    if val >= 1e5:
        return "moderate"
    return "low"


def trend_from_values(prev: Optional[float], curr: Optional[float]) -> str:
    if prev is None or curr is None:
        return "unknown"
    if curr > prev * 1.1:
        return "rising"
    if curr < prev * 0.9:
        return "falling"
    return "flat"


def confidence_from_points(n: int) -> str:
    if n >= 20:
        return "high"
    if n >= 10:
        return "moderate"
    return "low"


# -----------------------------
# Core logic
# -----------------------------

def fetch_wastewater(
    county_fips: str,
    pcr_target: str,
    days: int,
) -> List[Dict]:
    since = (dt.date.today() - dt.timedelta(days=days)).isoformat()

    params = {
        "$where": (
            f"county_fips = '{county_fips}' "
            f"AND pcr_target = '{pcr_target}' "
            f"AND sample_collect_date >= '{since}'"
        ),
        "$order": "sample_collect_date ASC",
        "$limit": 5000,
    }

    return socrata_get(params)


def analyze_series(rows: List[Dict]) -> Dict:
    if not rows:
        return {
            "daily_points": 0,
            "last7_median": None,
            "prev7_median": None,
            "risk": "unknown",
            "trend": "unknown",
            "confidence": "low",
        }

    values = []
    for r in rows:
        try:
            values.append(float(r["pcr_target_avg_conc_lin"]))
        except Exception:
            continue

    if not values:
        return {
            "daily_points": 0,
            "last7_median": None,
            "prev7_median": None,
            "risk": "unknown",
            "trend": "unknown",
            "confidence": "low",
        }

    last7 = values[-7:]
    prev7 = values[-14:-7] if len(values) >= 14 else []

    last7_m = median(last7) if last7 else None
    prev7_m = median(prev7) if prev7 else None

    risk = risk_from_value(last7_m)
    trend = trend_from_values(prev7_m, last7_m)
    confidence = confidence_from_points(len(values))

    return {
        "daily_points": len(values),
        "last7_median": last7_m,
        "prev7_median": prev7_m,
        "risk": risk,
        "trend": trend,
        "confidence": confidence,
    }


SIGNAL_DDL = r"""
CREATE TABLE IF NOT EXISTS signal_snapshots (
  id bigserial PRIMARY KEY,
  signal_type text NOT NULL,
  pathogen text,
  geo_level text,
  geo_id text,
  zip_code text,
  state text,
  county_fips text,
  generated_at timestamptz NOT NULL,
  risk_level text,
  trend text,
  confidence text,
  composite_score double precision,
  payload jsonb NOT NULL
);
"""

def _db_connect():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set.")
    try:
        import psycopg2  # type: ignore
    except Exception as e:
        raise RuntimeError("psycopg2 is not installed in this environment.") from e
    return psycopg2.connect(url)

def save_to_db(snapshot: Dict) -> int:
    conn = _db_connect()
    cur = conn.cursor()
    # ensure table exists
    cur.execute(SIGNAL_DDL)

    cur.execute(
        """
        INSERT INTO signal_snapshots (
          signal_type,
          pathogen,
          geo_level,
          geo_id,
          zip_code,
          state,
          county_fips,
          generated_at,
          risk_level,
          trend,
          confidence,
          composite_score,
          payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        (
            "wastewater",
            snapshot["results"][0]["pathogen"],
            "zip",
            snapshot["zip_code"],
            snapshot["zip_code"],
            snapshot["state_abbr"],
            snapshot["county_fips"],
            snapshot["generated_at"],
            snapshot["rollup"]["overall_level"],
            snapshot["rollup"]["overall_trend"],
            snapshot["rollup"]["overall_confidence"],
            snapshot["rollup"]["overall_score"],
            json.dumps(snapshot),
        ),
    )

    row_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    return row_id


# -----------------------------
# CLI
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("zip", help="ZIP code")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--db", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    geo = lookup_zip(args.zip)

    results = []
    scores = {}

    for pathogen, pcr in PATHOGENS.items():
        rows = fetch_wastewater(
            geo["county_fips"],
            pcr,
            DEFAULT_WINDOW_DAYS,
        )

        if not rows:
            rows = fetch_wastewater(
                geo["county_fips"],
                pcr,
                FALLBACK_WINDOW_DAYS,
            )

        analysis = analyze_series(rows)

        risk_score = 0.6 if analysis["risk"] == "moderate" else 1.0 if analysis["risk"] == "high" else 0.0
        trend_score = -0.25 if analysis["trend"] == "falling" else 0.25 if analysis["trend"] == "rising" else 0.0
        conf_score = 1.0 if analysis["confidence"] == "high" else 0.5 if analysis["confidence"] == "moderate" else 0.25

        composite = round((risk_score + conf_score + trend_score) / 2, 4)

        scores[pathogen] = composite

        results.append({
            "pathogen": pathogen,
            "dataset_id": DATASET_ID,
            "pcr_target": pcr,
            "window_days": DEFAULT_WINDOW_DAYS,
            "daily_points": analysis["daily_points"],
            "metric": "pcr_target_avg_conc_lin",
            "last7_median": analysis["last7_median"],
            "prev7_median": analysis["prev7_median"],
            "risk": analysis["risk"],
            "trend": analysis["trend"],
            "confidence": analysis["confidence"],
            "note": None if rows else "no wastewater data returned",
            "risk_score": risk_score,
            "trend_score": trend_score,
            "confidence_score": conf_score,
            "composite_score": composite,
        })

        if not args.all:
            break

    overall_score = round(max(scores.values()), 4)
    overall_level = "high" if overall_score >= 0.75 else "moderate" if overall_score >= 0.4 else "low"

    snapshot = {
        "zip_code": args.zip,
        "place": geo["place"],
        "state_name": geo["state_name"],
        "state_abbr": geo["state_abbr"],
        "county_name": geo["county_name"],
        "county_fips": geo["county_fips"],
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds"),
        "days_requested": DEFAULT_WINDOW_DAYS,
        "results": results,
        "rollup": {
            "overall_level": overall_level,
            "overall_trend": results[0]["trend"],
            "overall_confidence": results[0]["confidence"],
            "overall_score": overall_score,
            "suggestion": (
                "High respiratory activity detected. Increase disinfection frequency."
                if overall_level == "high"
                else "Moderate respiratory activity detected. Consider extra disinfection."
                if overall_level == "moderate"
                else "Low respiratory activity detected."
            ),
            "per_pathogen_scores": scores,
        },
    }

    if args.db:
        db_id = save_to_db(snapshot)
        snapshot["rollup"]["db"] = {"signal_snapshots_id": db_id}
        print(f"Saved snapshot to DB (signal_snapshots.id={db_id})")

    if args.json:
        print(json.dumps(snapshot, indent=2))
    else:
        print(json.dumps(snapshot, indent=2))


if __name__ == "__main__":
    main()

