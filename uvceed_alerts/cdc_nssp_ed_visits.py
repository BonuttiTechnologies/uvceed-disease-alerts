#!/usr/bin/env python3
"""
CDC NSSP ED Visit Trends (weekly) — direction categories (Increasing/Decreasing/Stable).

Dataset: rdmq-nq56 (CDC Socrata)

This script:
- resolves ZIP -> (place/state/county/fips)
- queries NSSP ED trend categories for a given state (via geography='Illinois', etc.)
- summarizes last-3 vs prev-3 (mode)
- outputs human-readable summary
- optional JSON output (--json / --json-only)
- optional DB persistence to Postgres (--db) via DATABASE_URL

Examples:
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen combined --weeks 16
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen combined --weeks 16 --json
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen combined --weeks 16 --json-only
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen combined --weeks 16 --db --json
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass, asdict, replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from uvceed_alerts.geo import zip_to_county


SODA_BASE = "https://data.cdc.gov/resource"
DATASET_ID = "rdmq-nq56"
DEFAULT_TIMEOUT = 30


# ---------------------------
# Models
# ---------------------------

@dataclass(frozen=True)
class TrendPoint:
    week_end: str
    value: str  # Increasing/Decreasing/Stable/Unknown


@dataclass(frozen=True)
class Summary:
    zip_code: str
    place: str
    state_name: str
    state_abbr: str
    county_name: str
    county_fips: str
    generated_at: str
    dataset_id: str
    pathogen: str
    metric_used: str
    lookback_weeks: int
    points: List[TrendPoint]
    last3_mode: Optional[str]
    prev3_mode: Optional[str]
    risk: str
    trend: str
    confidence: str
    note: Optional[str]
    scores: Dict[str, float]
    db: Optional[Dict[str, Any]] = None


# ---------------------------
# Helpers
# ---------------------------

def _endpoint(dataset_id: str) -> str:
    return f"{SODA_BASE}/{dataset_id}.json"


def _escape_soql_literal(s: str) -> str:
    # Socrata uses SQL-ish quoting. Single quotes are escaped by doubling them.
    return s.replace("'", "''")


def _get_json(dataset_id: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    r = requests.get(
        _endpoint(dataset_id),
        params=params,
        timeout=DEFAULT_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected CDC response type (expected JSON list).")
    return [x for x in data if isinstance(x, dict)]


def _mode(values: List[str]) -> Optional[str]:
    vals = [v for v in values if v]
    if not vals:
        return None
    c = Counter(vals)
    return c.most_common(1)[0][0]


def _normalize_trend_value(v: Any) -> str:
    if v is None:
        return "Unknown"
    s = str(v).strip()
    if not s:
        return "Unknown"
    low = s.lower()
    if "decreas" in low:
        return "Decreasing"
    if "increas" in low:
        return "Increasing"
    if "stable" in low or "flat" in low:
        return "Stable"
    return s


def _risk_from_last3(last3_mode: Optional[str]) -> str:
    if not last3_mode:
        return "unknown"
    if last3_mode == "Increasing":
        return "moderate"
    if last3_mode in ("Decreasing", "Stable"):
        return "low"
    return "unknown"


def _trend_label(last3_mode: Optional[str], prev3_mode: Optional[str]) -> str:
    if not last3_mode or not prev3_mode:
        return "unknown"
    if last3_mode == prev3_mode:
        if last3_mode == "Increasing":
            return "rising"
        if last3_mode == "Decreasing":
            return "falling"
        if last3_mode == "Stable":
            return "stable"
        return "unknown"
    if last3_mode == "Increasing":
        return "rising"
    if last3_mode == "Decreasing":
        return "falling"
    if last3_mode == "Stable":
        return "stable"
    return "unknown"


def _confidence_from_n(n_points: int, weeks_requested: int) -> str:
    if n_points >= min(weeks_requested, 12):
        return "high"
    if n_points >= 6:
        return "moderate"
    if n_points >= 3:
        return "low"
    return "low"


def _scores(risk: str, trend: str, confidence: str) -> Dict[str, float]:
    risk_score = {"unknown": 0.0, "low": 0.25, "moderate": 0.6, "high": 1.0}.get(risk, 0.0)
    trend_score = {"falling": -0.25, "stable": 0.0, "rising": 0.25, "unknown": 0.0}.get(trend, 0.0)
    conf_score = {"low": 0.4, "moderate": 0.7, "high": 1.0}.get(confidence, 0.4)
    raw = max(0.0, min(1.0, risk_score + trend_score))
    composite = round(raw * conf_score, 6)
    return {
        "risk_score": float(risk_score),
        "trend_score": float(trend_score),
        "confidence_score": float(conf_score),
        "composite_score": float(composite),
    }


def _append_note(existing: Optional[str], extra: str) -> str:
    extra = extra.strip()
    if not extra:
        return existing or ""
    if not existing:
        return extra
    return f"{existing} {extra}".strip()


# ---------------------------
# CDC query (direction categories)
# ---------------------------

def _fetch_trend_rows_for_state(state_name: str, weeks: int) -> List[Dict[str, Any]]:
    state_name_escaped = _escape_soql_literal(state_name)
    params = {
        "$select": ",".join(
            [
                "week_end",
                "geography",
                "ed_trends_covid",
                "ed_trends_influenza",
                "ed_trends_rsv",
            ]
        ),
        "$where": f"geography = '{state_name_escaped}'",
        "$order": "week_end DESC",
        "$limit": max(weeks, 4) * 4,  # cushion (dataset sometimes returns multiple rows per week)
        "$offset": 0,
    }
    return _get_json(DATASET_ID, params)


def _pick_metric(pathogen: str) -> Tuple[str, str]:
    p = pathogen.lower().strip()
    if p in ("covid", "sarscov2", "sars-cov-2"):
        return ("ed_trends_covid", "covid")
    if p in ("flu", "influenza", "flu_a", "flu-a"):
        return ("ed_trends_influenza", "influenza")
    if p in ("rsv",):
        return ("ed_trends_rsv", "rsv")
    if p in ("combined", "all", "respiratory"):
        return ("__combined__", "combined")
    raise ValueError("Invalid pathogen. Use: covid, flu, rsv, combined")


def _combined_from_three(covid: str, flu: str, rsv: str) -> str:
    vals = [covid, flu, rsv]
    inc = sum(1 for v in vals if v == "Increasing")
    dec = sum(1 for v in vals if v == "Decreasing")
    if inc >= 2:
        return "Increasing"
    if dec >= 2:
        return "Decreasing"
    if any(v == "Stable" for v in vals):
        return "Stable"
    return "Unknown"


def build_summary_for_zip(zip_code: str, *, pathogen: str, weeks: int) -> Summary:
    geo = zip_to_county(zip_code)

    rows = _fetch_trend_rows_for_state(geo.state_name, weeks=weeks)
    metric_field, pathogen_norm = _pick_metric(pathogen)

    # De-dupe by week_end: keep the first row we see for a given week_end after ordering DESC.
    seen_week_ends: set[str] = set()
    points: List[TrendPoint] = []

    for r in rows:
        week_end = str(r.get("week_end") or "").strip()
        if not week_end:
            continue
        if week_end in seen_week_ends:
            continue
        seen_week_ends.add(week_end)

        if metric_field == "__combined__":
            covid = _normalize_trend_value(r.get("ed_trends_covid"))
            flu = _normalize_trend_value(r.get("ed_trends_influenza"))
            rsv = _normalize_trend_value(r.get("ed_trends_rsv"))
            v = _combined_from_three(covid, flu, rsv)
        else:
            v = _normalize_trend_value(r.get(metric_field))

        points.append(TrendPoint(week_end=week_end, value=v))
        if len(points) >= weeks:
            break

    last3 = [p.value for p in points[:3]]
    prev3 = [p.value for p in points[3:6]]

    last3_mode = _mode(last3)
    prev3_mode = _mode(prev3)

    risk = _risk_from_last3(last3_mode)
    trend = _trend_label(last3_mode, prev3_mode)
    confidence = _confidence_from_n(len(points), weeks)

    note = None
    if len(points) < 6:
        note = "Insufficient data points (need >= 6 weeks) to compute stable comparison windows."

    return Summary(
        zip_code=str(zip_code),
        place=geo.place,
        state_name=geo.state_name,
        state_abbr=geo.state_abbr,
        county_name=geo.county_name,
        county_fips=str(geo.county_fips),
        generated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        dataset_id=DATASET_ID,
        pathogen=pathogen_norm,
        metric_used=pathogen_norm,
        lookback_weeks=weeks,
        points=points,
        last3_mode=last3_mode,
        prev3_mode=prev3_mode,
        risk=risk,
        trend=trend,
        confidence=confidence,
        note=note,
        scores=_scores(risk, trend, confidence),
        db=None,
    )


# ---------------------------
# DB persistence (Postgres)
# ---------------------------

DDL = """
CREATE TABLE IF NOT EXISTS nssp_ed_visits_snapshots (
  id BIGSERIAL PRIMARY KEY,
  zip_code TEXT NOT NULL,
  state_abbr TEXT NOT NULL,
  pathogen TEXT NOT NULL,
  lookback_weeks INT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL
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
    conn = psycopg2.connect(url, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout TO 90000;")  # 90s
    except Exception:
        pass
    return conn


def db_save(summary: Summary) -> int:
    payload = asdict(summary)
    payload["db"] = None  # don't store db field inside itself

    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(
                """
                INSERT INTO nssp_ed_visits_snapshots
                  (zip_code, state_abbr, pathogen, lookback_weeks, generated_at, payload)
                VALUES
                  (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    summary.zip_code,
                    summary.state_abbr,
                    summary.pathogen,
                    summary.lookback_weeks,
                    summary.generated_at,  # ISO string is OK; Postgres will cast for TIMESTAMPTZ
                    json.dumps(payload),
                ),
            )
            new_id = int(cur.fetchone()[0])
            conn.commit()
            return new_id


# ---------------------------
# Output
# ---------------------------

def print_human(summary: Summary) -> None:
    print(f"ZIP: {summary.zip_code} -> {summary.place}, {summary.state_name} ({summary.state_abbr})")
    print(f"County: {summary.county_name} | FIPS: {summary.county_fips}\n")

    print("CDC NSSP ED Visit Trends (weekly) — direction categories")
    print(f"Dataset: {summary.dataset_id}")
    print(f"Metric used: {summary.metric_used}")
    print(f"Lookback used: {summary.lookback_weeks} weeks")
    print(f"Recent points shown: {len(summary.points)}")
    print(f"Last-3 mode: {summary.last3_mode}")
    print(f"Prev-3 mode: {summary.prev3_mode}")
    print(f"Risk: {summary.risk} | Trend: {summary.trend} | Confidence: {summary.confidence}")
    if summary.note:
        print(f"Note: {summary.note}")

    if summary.db and summary.db.get("snapshot_id"):
        print(f"\nSaved snapshot to DB (nssp_ed_visits_snapshots.id={summary.db['snapshot_id']})")

    print("\nMost recent weeks:")
    for p in summary.points[:12]:
        print(f"- {p.week_end[:10]} | {p.value} | {summary.metric_used}")


def main() -> int:
    ap = argparse.ArgumentParser(description="CDC NSSP ED visit trend categories (weekly).")
    ap.add_argument("zip_code", help="US ZIP code (e.g., 60614)")
    ap.add_argument("--pathogen", default="combined", help="covid | flu | rsv | combined")
    ap.add_argument("--weeks", type=int, default=16, help="lookback window in weeks (default 16)")
    ap.add_argument("--json", action="store_true", help="also print JSON output")
    ap.add_argument("--json-only", action="store_true", help="print JSON only (no human text)")
    ap.add_argument("--db", action="store_true", help="save JSON snapshot to Postgres via DATABASE_URL")
    args = ap.parse_args()

    summary = build_summary_for_zip(args.zip_code, pathogen=args.pathogen, weeks=args.weeks)

    if args.db:
        # IMPORTANT FIX: use dataclasses.replace() so TrendPoint objects stay TrendPoint objects
        try:
            new_id = db_save(summary)
            summary = replace(summary, db={"snapshot_id": new_id})
        except Exception as e:
            summary = replace(summary, note=_append_note(summary.note, f"DB save failed: {e}"))

    if args.json_only:
        print(json.dumps(asdict(summary), indent=2, default=str))
        return 0

    print_human(summary)

    if args.json:
        print("\n" + json.dumps(asdict(summary), indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

