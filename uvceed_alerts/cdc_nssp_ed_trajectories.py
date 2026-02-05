#!/usr/bin/env python3
"""
cdc_nssp_ed_trajectories.py

CDC NSSP ED Visits "Trajectories" / numeric early-warning enhancer.

Reality check (based on your validation):
- The numeric percent fields live in dataset rdmq-nq56 (same dataset as "ED trends").
- rdmq-nq56 does NOT provide a clean single "state row" in a dedicated 'state' column.
- Instead, state rollups appear as geography="<StateName>" AND county="All".
- If those aren't present for a state/window, we can aggregate across rows per week_end.

This module:
- Resolves ZIP -> state_name/state_abbr via uvceed_alerts.geo.zip_to_county()
- Queries Socrata for state-level weekly % ED visits
- Computes last3 / prev3 medians + simple risk/trend/confidence
- Prints human-readable output (default) or JSON (--json)
- Provides --describe to help validate columns/coverage

Usage:
  python -m uvceed_alerts.cdc_nssp_ed_trajectories 60614 --pathogen flu --weeks 16
  python -m uvceed_alerts.cdc_nssp_ed_trajectories 60614 --pathogen combined --weeks 16 --json
  python -m uvceed_alerts.cdc_nssp_ed_trajectories --describe --state IL
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import asdict
from datetime import date
import datetime as dt, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from uvceed_alerts.config import SOCRATA_APP_TOKEN
from uvceed_alerts.geo import zip_to_county  # returns GeoResult dataclass


# ------------------------------------------------------------
# Constants / mappings
# ------------------------------------------------------------

# IMPORTANT: numeric % visits appear here (based on your tests)
DATASET_ID = "rdmq-nq56"
SOCRATA_BASE = "https://data.cdc.gov/resource"

PATHOGEN_TO_FIELD = {
    "covid": "percent_visits_covid",
    "flu": "percent_visits_influenza",
    "rsv": "percent_visits_rsv",
    # combined handled separately
}

# 2-letter -> state name mapping for geography filter.
# (Socrata geography uses full state name e.g. "Illinois")
US_STATE_ABBR_TO_NAME = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _parse_week_end(s: str) -> date:
    """
    Socrata returns ISO strings like:
      '2026-01-24T00:00:00.000'
    """
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except Exception:
        # last resort: first 10 chars
        return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x))
    except Exception:
        return None


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return float(statistics.median(vals))


def _confidence_from_points(n_points: int) -> str:
    if n_points >= 12:
        return "high"
    if n_points >= 6:
        return "moderate"
    return "low"


def _trend_from_medians(last3: Optional[float], prev3: Optional[float]) -> str:
    if last3 is None or prev3 is None or prev3 == 0:
        return "unknown"
    # Simple hysteresis so it doesn't flap
    if last3 > prev3 * 1.15:
        return "rising"
    if last3 < prev3 * 0.85:
        return "falling"
    return "flat"


def _risk_from_latest(latest: Optional[float], pathogen: str) -> str:
    if latest is None:
        return "unknown"

    # Very simple thresholds; we can tune later.
    # These are % of ED visits, so values like 0.5–6 are realistic.
    if pathogen == "covid":
        if latest >= 1.5:
            return "high"
        if latest >= 0.8:
            return "moderate"
        return "low"

    if pathogen == "flu":
        if latest >= 3.0:
            return "high"
        if latest >= 1.5:
            return "moderate"
        return "low"

    if pathogen == "rsv":
        if latest >= 2.0:
            return "high"
        if latest >= 1.0:
            return "moderate"
        return "low"

    if pathogen == "combined":
        if latest >= 6.0:
            return "high"
        if latest >= 3.0:
            return "moderate"
        return "low"

    return "unknown"


def _socrata_get_with_retries(
    dataset_id: str,
    params: Dict[str, str],
    max_attempts: int = 6,
    sleep_base: float = 0.7,
) -> List[Dict[str, Any]]:
    """
    Socrata can throw occasional 500s / coordinator hiccups.
    We'll retry with small backoff.
    """
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    headers = {"Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * attempt)

    raise RuntimeError(f"Socrata request failed after {max_attempts} attempts: {last_err}")


def _state_name_from_abbr(state_abbr: str) -> str:
    ab = (state_abbr or "").strip().upper()
    if ab in US_STATE_ABBR_TO_NAME:
        return US_STATE_ABBR_TO_NAME[ab]
    # fallback: return input; sometimes user passes full name already
    return state_abbr


def _compute_series_from_rows(
    rows: List[Dict[str, Any]],
    pathogen: str,
) -> List[Tuple[date, float]]:
    """
    Convert Socrata rows -> weekly series (week_end, value).

    For state rollup rows (county='All') you typically get one row per week_end.
    For non-rollup, there can be many rows per week_end; we median them.
    """
    # group values by week_end
    buckets: Dict[date, List[float]] = {}

    for row in rows:
        if "week_end" not in row:
            continue
        wk = _parse_week_end(row["week_end"])

        if pathogen == "combined":
            c = _to_float(row.get("percent_visits_covid"))
            f = _to_float(row.get("percent_visits_influenza"))
            r = _to_float(row.get("percent_visits_rsv"))
            if c is None and f is None and r is None:
                continue
            val = (c or 0.0) + (f or 0.0) + (r or 0.0)
        else:
            field = PATHOGEN_TO_FIELD.get(pathogen)
            if not field:
                continue
            val = _to_float(row.get(field))
            if val is None:
                continue

        buckets.setdefault(wk, []).append(val)

    series: List[Tuple[date, float]] = []
    for wk, vals in buckets.items():
        med = _median(vals)
        if med is not None:
            series.append((wk, med))

    # newest first
    series.sort(key=lambda t: t[0], reverse=True)
    return series


# ------------------------------------------------------------
# Core logic
# ------------------------------------------------------------

def fetch_state_weekly_percent_visits(
    state_abbr: str,
    state_name: str,
    weeks: int,
    pathogen: str,
) -> Tuple[str, List[Tuple[date, float]], str]:
    """
    Returns: (geography_used, series, note)

    Strategy:
    1) Try strict state rollup via: geography=<StateName> AND county='All'
       This is the cleanest weekly series when available.
    2) If empty, fallback to geography=<StateName> and median-aggregate across rows per week_end.
    """
    geography = state_name.strip()

    # Build a time window. We request a bit more than needed in case of gaps.
    # Socrata doesn't use epiweek ints here; it's week_end dates.
    start_date = (date.today() - timedelta(days=(weeks + 8) * 7)).isoformat()

    select_cols = (
        "week_end,geography,county,percent_visits_covid,"
        "percent_visits_influenza,percent_visits_rsv"
    )

    # 1) strict rollup
    where_strict = (
        f"geography = '{geography}' "
        f"AND county = 'All' "
        f"AND week_end >= '{start_date}T00:00:00.000'"
    )
    params_strict = {
        "$select": select_cols,
        "$where": where_strict,
        "$order": "week_end DESC",
        "$limit": str(max(weeks * 2, 40)),
    }
    rows = _socrata_get_with_retries(DATASET_ID, params_strict)
    series = _compute_series_from_rows(rows, pathogen)

    if len(series) >= 2:
        return geography, series[:weeks], None

    # 2) fallback: all rows in geography; aggregate by week_end
    where_fallback = (
        f"geography = '{geography}' "
        f"AND week_end >= '{start_date}T00:00:00.000'"
    )
    params_fb = {
        "$select": select_cols,
        "$where": where_fallback,
        "$order": "week_end DESC",
        "$limit": str(max(weeks * 250, 2000)),  # plenty; many rows per week
    }
    rows_fb = _socrata_get_with_retries(DATASET_ID, params_fb)
    series_fb = _compute_series_from_rows(rows_fb, pathogen)

    if series_fb:
        note = (
            "State rollup rows (county='All') were missing or sparse; "
            "used median rollup across geography rows per week_end."
        )
        return geography, series_fb[:weeks], note

    return geography, [], "no ED-visit % data returned for this state / window (possible dataset coverage gap)"


def summarize_series(
    series: List[Tuple[date, float]],
    pathogen: str,
) -> Dict[str, Any]:
    """
    Compute last3/prev3 medians and classify risk/trend/confidence.
    series is newest-first.
    """
    values = [v for _, v in series if v is not None]
    n_points = len(values)
    conf = _confidence_from_points(n_points)

    last3 = _median(values[:3]) if n_points >= 3 else None
    prev3 = _median(values[3:6]) if n_points >= 6 else None
    trend = _trend_from_medians(last3, prev3)

    latest = values[0] if values else None
    risk = _risk_from_latest(latest, pathogen)

    # If we have too few points, avoid overconfident trend/risk
    note = None
    if n_points < 6:
        note = "Insufficient data points (need >= 6 weeks) for stable medians/trend."

    return {
        "metric": "percent_visits" if pathogen != "combined" else "percent_visits_sum(covid+flu+rsv)",
        "recent_points": n_points,
        "last3_median": last3,
        "prev3_median": prev3,
        "risk": risk if n_points >= 1 else "unknown",
        "trend": trend if n_points >= 6 else "unknown",
        "confidence": conf,
        "note": note,
    }


def build_nssp_ed_trajectories_for_zip(
    zip_code: str,
    pathogen: str,
    weeks: int,
) -> Tuple[str, Dict[str, Any]]:
    """
    Returns:
      header_text, result_dict
    """
    geo = zip_to_county(zip_code)
    state_abbr = getattr(geo, "state_abbr", "").upper()
    state_name = getattr(geo, "state_name", _state_name_from_abbr(state_abbr))

    geography_used, series, fetch_note = fetch_state_weekly_percent_visits(
        state_abbr=state_abbr,
        state_name=state_name,
        weeks=weeks,
        pathogen=pathogen,
    )

    summary = summarize_series(series, pathogen)
    if fetch_note:
        # combine notes
        if summary.get("note"):
            summary["note"] = f"{fetch_note} {summary['note']}"
        else:
            summary["note"] = fetch_note

    recent = [{"week_end": d.isoformat(), "value": v} for d, v in series]

    result = {
        "region": f"{state_abbr} (geography='{geography_used}')",
        "pathogen": pathogen,
        "metric": summary["metric"],
        "lookback_weeks": weeks,
        "recent_points": summary["recent_points"],
        "last3_median": summary["last3_median"],
        "prev3_median": summary["prev3_median"],
        "risk": summary["risk"],
        "trend": summary["trend"],
        "confidence": summary["confidence"],
        "note": summary.get("note"),
        "recent": recent,
    }

    header = (
        f"ZIP: {zip_code} -> {geo.place}, {geo.state_name} ({geo.state_abbr})\n"
        f"County: {geo.county_name} | FIPS: {geo.county_fips}\n\n"
        f"CDC NSSP ED Visit Trajectories (weekly percent ED visits) — numeric signal\n"
        f"Dataset: {DATASET_ID}\n"
        f"Region: {result['region']}\n"
        f"Pathogen: {pathogen}\n"
        f"Metric used: {result['metric']}\n"
        f"Lookback used: {weeks} weeks\n"
        f"Recent points shown: {min(len(recent), 12)}\n"
        f"Last-3 median: {result['last3_median']}\n"
        f"Prev-3 median: {result['prev3_median']}\n"
        f"Risk: {result['risk']} | Trend: {result['trend']} | Confidence: {result['confidence']}\n"
    )
    if result.get("note"):
        header += f"Note: {result['note']}\n"

    return header, result


def describe_dataset(state_abbr: str) -> int:
    """
    Diagnostics:
    - Show columns present
    - Show some example rows for geography=<StateName>
    - Show whether strict state rollup rows exist (county='All')
    """
    state_abbr = (state_abbr or "").strip().upper()
    state_name = _state_name_from_abbr(state_abbr)

    # Pull a handful of rows to inspect columns
    params = {
        "$select": "*",
        "$where": f"geography = '{state_name}'",
        "$order": "week_end DESC",
        "$limit": "50",
    }
    rows = _socrata_get_with_retries(DATASET_ID, params=params)

    print(f"NSSP ED Visits (Trajectories / numeric) — CDC Socrata dataset {DATASET_ID}")
    print(f"State input: {state_abbr} -> geography='{state_name}'\n")

    if not rows:
        print("No rows returned for this geography.\n")
        return 0

    # Columns
    cols = sorted({k for r in rows for k in r.keys()})
    print(f"Columns ({len(cols)}): {', '.join(cols)}\n")

    # Show example rows (up to 5)
    print("Example rows (up to 5):")
    for r in rows[:5]:
        wk = r.get("week_end")
        county = r.get("county")
        hsa = r.get("hsa")
        covid = r.get("percent_visits_covid")
        flu = r.get("percent_visits_influenza")
        rsv = r.get("percent_visits_rsv")
        print(f"- week_end={wk} county={county} hsa={hsa} covid={covid} flu={flu} rsv={rsv}")

    # Check strict rollup rows
    params_strict = {
        "$select": "week_end,county,geography,percent_visits_covid,percent_visits_influenza,percent_visits_rsv",
        "$where": f"geography = '{state_name}' AND county = 'All'",
        "$order": "week_end DESC",
        "$limit": "30",
    }
    strict_rows = _socrata_get_with_retries(DATASET_ID, params=params_strict)
    print("\nStrict state-level row check (geography + county='All'):")
    print(f"- strict rows returned: {len(strict_rows)}")
    if strict_rows:
        print("- most recent strict row:")
        r0 = strict_rows[0]
        print(
            f"  week_end={r0.get('week_end')} covid={r0.get('percent_visits_covid')} "
            f"flu={r0.get('percent_visits_influenza')} rsv={r0.get('percent_visits_rsv')}"
        )

    return 0


# ---------------------------
# DB persistence
# ---------------------------

DDL_TRAJ = r"""
CREATE TABLE IF NOT EXISTS nssp_ed_trajectories_snapshots (
  id bigserial PRIMARY KEY,
  zip_code text NOT NULL,
  state_abbr text NOT NULL,
  pathogen text NOT NULL,
  weeks_requested int NOT NULL,
  generated_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  UNIQUE(zip_code, state_abbr, pathogen, weeks_requested, generated_at)
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


def db_save_trajectories(payload: Dict[str, Any]) -> int:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_TRAJ)
            cur.execute(
                """
                INSERT INTO nssp_ed_trajectories_snapshots
                  (zip_code, state_abbr, pathogen, weeks_requested, generated_at, payload)
                VALUES
                  (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    payload.get("zip_code"),
                    payload.get("state_abbr"),
                    payload.get("pathogen"),
                    int(payload.get("weeks_requested") or 0),
                    payload.get("generated_at"),
                    json.dumps(payload),
                ),
            )
            new_id = int(cur.fetchone()[0])
            conn.commit()
            return new_id

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="uvceed_alerts.cdc_nssp_ed_trajectories",
        description="CDC NSSP ED Visit Trajectories (numeric % ED visits) via CDC Socrata",
    )
    parser.add_argument("zip_code", nargs="?", help="5-digit ZIP code (e.g., 60614)")

    parser.add_argument(
        "--pathogen",
        default="combined",
        choices=["covid", "flu", "rsv", "combined"],
        help="Which pathogen series to summarize",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=16,
        help="Lookback window (weeks)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="also print JSON payload",
    )

    parser.add_argument(
        "--json-only",
        action="store_true",
        help="print JSON only (no human text)",
    )

    parser.add_argument(
        "--db",
        action="store_true",
        help="save JSON snapshot to Postgres via DATABASE_URL",
    )

    parser.add_argument(
        "--describe",
        action="store_true",
        help="Describe dataset columns and sample rows for a state",
    )
    parser.add_argument(
        "--state",
        default=None,
        help="State abbreviation for --describe (e.g. IL)",
    )

    args = parser.parse_args(argv)

    if args.describe:
        if not args.state:
            print("ERROR: --describe requires --state (e.g. --describe --state IL)", file=sys.stderr)
            return 2
        return describe_dataset(args.state)

    if not args.zip_code:
        print("ERROR: zip_code is required unless using --describe", file=sys.stderr)
        return 2

    header, result = build_nssp_ed_trajectories_for_zip(
        zip_code=args.zip_code,
        pathogen=args.pathogen,
        weeks=args.weeks,
    )

    geo = zip_to_county(args.zip_code)
    generated_at = dt.datetime.utcnow().isoformat()

    payload = {
        "zip_code": args.zip_code,
        "place": geo.place,
        "state_name": geo.state_name,
        "state_abbr": geo.state_abbr,
        "county_name": geo.county_name,
        "county_fips": geo.county_fips,
        "generated_at": generated_at,
        "generated_date": date.today().isoformat(),
        "source": "cdc_socrata_nssp_ed_trajectories_numeric",
        "dataset_id": DATASET_ID,
        "weeks_requested": args.weeks,
        "pathogen": args.pathogen,
        "results": result,
        "db": None,
    }

    if args.db:
        try:
            snapshot_id = db_save_trajectories(payload)
            payload["db"] = {"snapshot_id": snapshot_id}
        except Exception as e:
            payload["results"]["note"] = (payload["results"].get("note") or "") + f" | DB save failed: {e}"

    if args.json_only:
        print(json.dumps(payload, indent=2, default=str))
        return 0

    print(header)
    if payload.get("db") and payload["db"].get("snapshot_id"):
        print(f"\nSaved snapshot to DB (nssp_ed_trajectories_snapshots.id={payload['db']['snapshot_id']})")

    print("\nMost recent weeks:")
    for item in result["recent"][:12]:
        print(f"- {item['week_end']} | {item['value']:.2f}")

    if args.json:
        print("\n" + json.dumps(payload, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

