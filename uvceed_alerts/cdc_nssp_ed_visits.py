"""
cdc_nssp_ed_visits.py

CDC NSSP Emergency Department (ED) Visit Trends by county FIPS (ZIP -> county FIPS).

IMPORTANT:
- This CDC Socrata dataset reports *trend direction* categories, NOT severity levels.
  Values are typically: "Increasing", "Decreasing", "No Change".
- Therefore:
  - risk is derived from direction (Increasing->high, No Change->moderate, Decreasing->low)
  - trend compares recent direction vs prior direction

Data source (Socrata / CDC):
- https://data.cdc.gov/resource/rdmq-nq56.json

Common fields:
- week_end (ISO date)
- geography (state name)
- county
- fips (county FIPS, e.g., 17031)
- ed_trends_covid
- ed_trends_influenza
- ed_trends_rsv
- hsa / hsa_counties / hsa_nci_id
- trend_source
- buildnumber

Usage:
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen combined --weeks 16
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen flu --weeks 16
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --pathogen rsv --weeks 16 --json
  python -m uvceed_alerts.cdc_nssp_ed_visits 60614 --describe
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from uvceed_alerts.geo import zip_to_county  # GeoResult dataclass


SOC_DATASET_ID = "rdmq-nq56"
SOC_BASE = f"https://data.cdc.gov/resource/{SOC_DATASET_ID}.json"

PATHOGEN_TO_FIELD = {
    "covid": "ed_trends_covid",
    "flu": "ed_trends_influenza",
    "rsv": "ed_trends_rsv",
    "combined": None,  # roll up from the three fields (direction rollup)
}

# Direction scoring (NOT severity).
# We keep both rank (for comparing) and score (for rollups).
DIRECTION_RANK = {
    "decreasing": -1,
    "no change": 0,
    "increasing": 1,
    "unknown": None,
    "data unavailable": None,
    "insufficient data": None,
}

DIRECTION_SCORE = {
    "decreasing": -1,
    "no change": 0,
    "increasing": 1,
}


@dataclass
class NsspPoint:
    week_end: str  # YYYY-MM-DD
    value: str     # Increasing / Decreasing / No Change / unknown
    metric: str    # field name or "combined"
    geography: Optional[str] = None
    county: Optional[str] = None
    trend_source: Optional[str] = None


@dataclass
class NsspSummary:
    region: str
    metric: str
    lookback_weeks: int
    recent_points: int
    last3_mode: Optional[str]
    prev3_mode: Optional[str]
    risk: str
    trend: str
    confidence: str
    note: Optional[str]
    recent: List[Dict[str, Any]]


def _today() -> dt.date:
    return dt.date.today()


def _start_date_for_weeks(weeks: int) -> dt.date:
    return _today() - dt.timedelta(days=int(weeks * 7) + 3)


def _norm(s: Optional[str]) -> str:
    if not s:
        return "unknown"
    return str(s).strip().lower()


def _pretty_direction(s: str) -> str:
    k = _norm(s)
    if k in ("no change", "increasing", "decreasing"):
        return "No Change" if k == "no change" else k.title()
    if k in ("data unavailable", "insufficient data"):
        return k
    return "unknown"


def _direction_rank(direction: Optional[str]) -> Optional[int]:
    if direction is None:
        return None
    return DIRECTION_RANK.get(_norm(direction), None)


def _mode(values: List[str]) -> Optional[str]:
    if not values:
        return None
    counts: Dict[str, int] = {}
    for v in values:
        k = _norm(v)
        counts[k] = counts.get(k, 0) + 1
    top = max(counts.items(), key=lambda kv: kv[1])[0]
    return _pretty_direction(top)


def _rollup_direction(labels: List[str]) -> str:
    # Score rollup: Increasing=+1, No Change=0, Decreasing=-1
    scores = []
    for lab in labels:
        k = _norm(lab)
        if k in DIRECTION_SCORE:
            scores.append(DIRECTION_SCORE[k])
    if not scores:
        return "unknown"
    s = sum(scores)
    if s > 0:
        return "Increasing"
    if s < 0:
        return "Decreasing"
    return "No Change"


def _risk_from_direction(direction: Optional[str]) -> str:
    if not direction:
        return "unknown"
    k = _norm(direction)
    if k == "increasing":
        return "high"
    if k == "no change":
        return "moderate"
    if k == "decreasing":
        return "low"
    return "unknown"


def _trend_from_modes(prev: Optional[str], cur: Optional[str]) -> str:
    if not prev or not cur:
        return "unknown"
    rp = _direction_rank(prev)
    rc = _direction_rank(cur)
    if rp is None or rc is None:
        return "unknown"
    if rc > rp:
        return "rising"
    if rc < rp:
        return "falling"
    return "stable"


def _confidence(recent_points: int) -> str:
    if recent_points >= 10:
        return "high"
    if recent_points >= 5:
        return "moderate"
    return "low"


def socrata_get(params: Dict[str, Any], timeout: int = 30, tries: int = 5) -> List[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            r = requests.get(SOC_BASE, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            wait = 1.5 * (2 ** i)
            print(f"[warn] Socrata fetch failed (attempt {i+1}/{tries}): {e}", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Socrata query failed after {tries} attempts: {last_err}")


def fetch_nssp_for_county_fips(county_fips: str, weeks: int) -> List[Dict[str, Any]]:
    start = _start_date_for_weeks(weeks)
    where = f"fips = '{county_fips}' AND week_end >= '{start.isoformat()}'"
    params = {
        "$where": where,
        "$order": "week_end DESC",
        "$limit": 5000,
    }
    return socrata_get(params)


def build_nssp_ed_summary_for_zip(
    zip_code: str,
    pathogen: str,
    weeks: int,
) -> Tuple[Dict[str, Any], NsspSummary]:
    geo = zip_to_county(zip_code)  # GeoResult dataclass
    county_fips = str(getattr(geo, "county_fips"))
    state_abbr = str(getattr(geo, "state_abbr"))
    state_name = str(getattr(geo, "state_name"))
    place = str(getattr(geo, "place"))
    county_name = str(getattr(geo, "county_name"))

    rows = fetch_nssp_for_county_fips(county_fips, weeks)

    seen_dates = set()
    points: List[NsspPoint] = []

    field = PATHOGEN_TO_FIELD.get(pathogen)
    if pathogen not in PATHOGEN_TO_FIELD:
        raise ValueError(f"Unsupported pathogen '{pathogen}'. Use: {', '.join(PATHOGEN_TO_FIELD.keys())}")

    for row in rows:
        week_end = str(row.get("week_end", ""))[:10]
        if not week_end or week_end in seen_dates:
            continue
        seen_dates.add(week_end)

        geo_state = row.get("geography")
        geo_county = row.get("county")
        trend_source = row.get("trend_source")

        if pathogen == "combined":
            labels = [
                str(row.get("ed_trends_covid", "")),
                str(row.get("ed_trends_influenza", "")),
                str(row.get("ed_trends_rsv", "")),
            ]
            value = _rollup_direction(labels)
            metric = "combined"
        else:
            raw = str(row.get(field, "")) if field else ""
            value = _pretty_direction(raw)
            metric = field or pathogen

        points.append(
            NsspPoint(
                week_end=week_end,
                value=value if value else "unknown",
                metric=metric,
                geography=geo_state,
                county=geo_county,
                trend_source=trend_source,
            )
        )

    last3 = [p.value for p in points[:3]]
    prev3 = [p.value for p in points[3:6]]

    last3_mode = _mode(last3)
    prev3_mode = _mode(prev3)

    risk = _risk_from_direction(last3_mode)
    trend = _trend_from_modes(prev3_mode, last3_mode)
    conf = _confidence(min(12, len(points)))

    note = None
    if len(points) == 0:
        note = "no NSSP ED trend records found for this county in the requested window"
    elif len(points) < 6:
        note = "sparse weekly records; trend may be unreliable"

    header = {
        "zip_code": zip_code,
        "place": place,
        "state_name": state_name,
        "state_abbr": state_abbr,
        "county_name": county_name,
        "county_fips": county_fips,
    }

    summary = NsspSummary(
        region=f"{state_abbr} / {county_name}",
        metric=pathogen,
        lookback_weeks=weeks,
        recent_points=min(12, len(points)),
        last3_mode=last3_mode,
        prev3_mode=prev3_mode,
        risk=risk,
        trend=trend,
        confidence=conf,
        note=note,
        recent=[{"week_end": p.week_end, "value": p.value, "metric": p.metric} for p in points[:12]],
    )

    return header, summary


def print_human(header: Dict[str, Any], summary: NsspSummary) -> None:
    print(f"ZIP: {header['zip_code']} -> {header['place']}, {header['state_name']} ({header['state_abbr']})")
    print(f"County: {header['county_name']} | FIPS: {header['county_fips']}")
    print()
    print("CDC NSSP ED Visit Trends (weekly) — direction categories")
    print(f"Dataset: {SOC_DATASET_ID}")
    print(f"Metric used: {summary.metric}")
    print(f"Lookback used: {summary.lookback_weeks} weeks")
    print(f"Recent points shown: {summary.recent_points}")
    print(f"Last-3 mode: {summary.last3_mode}")
    print(f"Prev-3 mode: {summary.prev3_mode}")
    print(f"Risk: {summary.risk} | Trend: {summary.trend} | Confidence: {summary.confidence}")
    if summary.note:
        print(f"Note: {summary.note}")
    print()
    print("Most recent weeks:")
    for p in summary.recent:
        print(f"- {p['week_end']} | {p['value']} | {p['metric']}")


def describe() -> None:
    print("NSSP ED Visit Trends (CDC Socrata)")
    print(f"Dataset: {SOC_DATASET_ID}")
    print("Endpoint:", SOC_BASE)
    print()
    print("This dataset provides weekly ED visit *trend direction* categories by county FIPS.")
    print("Typical values: Increasing / No Change / Decreasing")
    print()
    print("Key fields commonly present:")
    print("- week_end")
    print("- geography (state name)")
    print("- county")
    print("- fips (county FIPS)")
    print("- ed_trends_covid")
    print("- ed_trends_influenza")
    print("- ed_trends_rsv")
    print("- hsa / hsa_counties / hsa_nci_id")
    print("- trend_source")
    print("- buildnumber")
    print()
    print("Pathogen -> field mapping:")
    print("- covid    -> ed_trends_covid")
    print("- flu      -> ed_trends_influenza")
    print("- rsv      -> ed_trends_rsv")
    print("- combined -> score rollup across covid/flu/rsv")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="CDC NSSP ED Visit Trends by ZIP (county FIPS).")
    p.add_argument("zip_code", nargs="?", help="US ZIP code (e.g., 60614)")
    p.add_argument("--pathogen", default="combined", choices=list(PATHOGEN_TO_FIELD.keys()))
    p.add_argument("--weeks", type=int, default=16, help="Lookback window in weeks (default: 16)")
    p.add_argument("--json", action="store_true", help="Print JSON output")
    p.add_argument("--describe", action="store_true", help="Describe source + mappings and exit")

    args = p.parse_args(argv)

    if args.describe:
        describe()
        return 0

    if not args.zip_code:
        p.print_help()
        return 2

    header, summary = build_nssp_ed_summary_for_zip(
        zip_code=args.zip_code,
        pathogen=args.pathogen,
        weeks=args.weeks,
    )

    if args.json:
        out = {
            **header,
            "generated_date": dt.date.today().isoformat(),
            "source": "cdc_socrata_nssp_ed_trends",
            "dataset_id": SOC_DATASET_ID,
            "weeks_requested": args.weeks,
            "results": asdict(summary),
        }
        print_human(header, summary)
        print()
        print(json.dumps(out, indent=2))
    else:
        print_human(header, summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

