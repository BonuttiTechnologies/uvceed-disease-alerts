#!/usr/bin/env python3
"""
CDC FluView "Severity" (state-level)

Signals:
1) Lab positivity (clinical labs): Delphi Epidata fluview_clinical -> percent_positive
2) Hospitalizations: FluSurv-NET is limited-coverage; for non-covered states we report "not available"

Usage:
  python -m uvceed_alerts.cdc_fluview_severity 60614
  python -m uvceed_alerts.cdc_fluview_severity 62401 --weeks 16
  python -m uvceed_alerts.cdc_fluview_severity 60614 --json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests

# IMPORTANT: your project currently uses zip_to_county (per your earlier smoke script).
# We keep this import compatible with your repo.
from uvceed_alerts.geo import zip_to_county  # returns GeoResult dataclass

# Delphi Epidata FluView Clinical endpoint docs:
# https://api.delphi.cmu.edu/epidata/fluview_clinical/
DELPHI_FLUVIEW_CLINICAL_URL = "https://api.delphi.cmu.edu/epidata/fluview_clinical/"

# FluSurv-NET is limited coverage; keep a conservative allowlist.
# (You can revise later if you wire an alternate hospitalization source.)
FLUSURV_NET_STATES = {
    "CA", "CO", "CT", "GA", "IA", "ID", "MD", "MI", "MN", "NM", "NY", "OR", "TN", "UT"
}

DEFAULT_WEEKS_LOOKBACK = 104
DEFAULT_RECENT_WEEKS_SHOWN = 12


@dataclass(frozen=True)
class LabPositivitySummary:
    region: str
    metric: str  # "percent_positive"
    lookback_weeks: int
    recent_points: int
    last3_median: Optional[float]
    prev3_median: Optional[float]
    risk: str
    trend: str
    confidence: str
    note: Optional[str]
    recent: List[Dict[str, Any]]  # [{"epiweek": 202601, "percent_positive": 12.3}, ...]


@dataclass(frozen=True)
class HospSummary:
    region: str
    metric: str  # "hospitalizations"
    lookback_weeks: int
    recent_points: int
    risk: str
    trend: str
    confidence: str
    note: Optional[str]
    recent: List[Dict[str, Any]]


def _is_finite(x: Any) -> bool:
    try:
        return x is not None and math.isfinite(float(x))
    except Exception:
        return False


def _median(vals: List[float]) -> Optional[float]:
    vals = [float(v) for v in vals if _is_finite(v)]
    if not vals:
        return None
    return float(statistics.median(vals))


def _assess_simple_risk(last3: Optional[float], prev3: Optional[float]) -> Tuple[str, str, str]:
    """
    Heuristic for percent_positive:
      - Risk based on last3 absolute level
      - Trend based on last3 vs prev3
      - Confidence requires both medians present
    Tune thresholds later.
    """
    if last3 is None:
        return ("unknown", "unknown", "low")

    # Level heuristic (tune as needed)
    if last3 >= 15:
        risk = "high"
    elif last3 >= 5:
        risk = "moderate"
    else:
        risk = "low"

    if prev3 is None:
        return (risk, "unknown", "low")

    if last3 > prev3 * 1.15:
        trend = "rising"
    elif last3 < prev3 * 0.85:
        trend = "falling"
    else:
        trend = "stable"

    return (risk, trend, "high")


def _epiweeks_back_from_today(n_weeks: int) -> List[int]:
    """
    Delphi accepts epiweeks (YYYYWW). We'll approximate epiweek using ISO calendar weeks.
    """
    today = date.today()
    iso_year, iso_week, _ = today.isocalendar()

    weeks: List[int] = []
    y, w = iso_year, iso_week
    for _ in range(max(1, n_weeks)):
        weeks.append(y * 100 + w)
        w -= 1
        if w <= 0:
            y -= 1
            w = date(y, 12, 28).isocalendar()[1]  # last ISO week of the year
    weeks = sorted(set(weeks))
    return weeks


def fetch_fluview_clinical_percent_positive(state_abbr: str, weeks_lookback: int) -> List[Dict[str, Any]]:
    """
    Returns list of epidata entries for the state (region like 'il') including percent_positive.
    """
    region = state_abbr.lower()
    epiweeks = _epiweeks_back_from_today(weeks_lookback)
    epiweek_param = ",".join(str(w) for w in epiweeks)

    params = {"regions": region, "epiweeks": epiweek_param}

    r = requests.get(DELPHI_FLUVIEW_CLINICAL_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if data.get("result") != 1 or not data.get("epidata"):
        return []

    out: List[Dict[str, Any]] = []
    for row in data["epidata"]:
        if row.get("region") != region:
            continue
        out.append(
            {
                "epiweek": int(row["epiweek"]),
                "percent_positive": row.get("percent_positive"),
                "total_specimens": row.get("total_specimens"),
                "total_a": row.get("total_a"),
                "total_b": row.get("total_b"),
            }
        )
    out.sort(key=lambda x: x["epiweek"])
    return out


def build_lab_positivity_summary(state_abbr: str, weeks_lookback: int, recent_weeks: int) -> LabPositivitySummary:
    region = state_abbr.lower()
    rows = fetch_fluview_clinical_percent_positive(state_abbr, weeks_lookback)

    series = [(r["epiweek"], r.get("percent_positive")) for r in rows if _is_finite(r.get("percent_positive"))]
    if not series:
        return LabPositivitySummary(
            region=region,
            metric="percent_positive",
            lookback_weeks=weeks_lookback,
            recent_points=0,
            last3_median=None,
            prev3_median=None,
            risk="unknown",
            trend="unknown",
            confidence="low",
            note="no fluview_clinical percent_positive data returned for this region/window",
            recent=[],
        )

    values = [float(v) for _, v in series]
    last3 = _median(values[-3:])
    prev3 = _median(values[-6:-3]) if len(values) >= 6 else None
    risk, trend, conf = _assess_simple_risk(last3, prev3)

    recent_slice = series[-max(1, recent_weeks):]
    recent_out = [{"epiweek": ew, "percent_positive": float(v)} for ew, v in recent_slice]

    return LabPositivitySummary(
        region=region,
        metric="percent_positive",
        lookback_weeks=weeks_lookback,
        recent_points=len(recent_out),
        last3_median=last3,
        prev3_median=prev3,
        risk=risk,
        trend=trend,
        confidence=conf,
        note=None,
        recent=recent_out,
    )


def build_hospitalization_summary(state_abbr: str, weeks_lookback: int) -> HospSummary:
    region = state_abbr.lower()

    if state_abbr.upper() not in FLUSURV_NET_STATES:
        return HospSummary(
            region=region,
            metric="hospitalizations",
            lookback_weeks=weeks_lookback,
            recent_points=0,
            risk="unknown",
            trend="unknown",
            confidence="low",
            note="FluSurv-NET hospitalization rates are not available for this state (limited network coverage).",
            recent=[],
        )

    # Placeholder for future: wire a real ingestion for FluSurv-NET covered states.
    return HospSummary(
        region=region,
        metric="hospitalizations",
        lookback_weeks=weeks_lookback,
        recent_points=0,
        risk="unknown",
        trend="unknown",
        confidence="low",
        note="hospitalization ingestion not implemented yet (state is FluSurv-NET covered, but no data source wired).",
        recent=[],
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("zip_code", help="5-digit ZIP code")
    ap.add_argument("--weeks", type=int, default=DEFAULT_WEEKS_LOOKBACK, help="lookback window in weeks (default: 104)")
    ap.add_argument("--recent", type=int, default=DEFAULT_RECENT_WEEKS_SHOWN, help="recent points to show (default: 12)")
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON at end")
    args = ap.parse_args()

    geo = zip_to_county(args.zip_code)

    print(f"ZIP: {geo.zip_code} -> {geo.place}, {geo.state_name} ({geo.state_abbr})")
    print(f"County: {geo.county_name} | FIPS: {geo.county_fips}")
    print("")
    print("CDC FluView “Severity” (state-level)")
    print("")

    lab = build_lab_positivity_summary(geo.state_abbr, args.weeks, args.recent)
    hosp = build_hospitalization_summary(geo.state_abbr, args.weeks)

    print("Lab positivity (clinical labs)")
    print(f"Region: {geo.state_abbr} (state-level)")
    print("Metric used: percent_positive")
    print(f"Lookback used: {lab.lookback_weeks} weeks")
    print(f"Recent points shown: {lab.recent_points}")
    print(f"Last-3 median: {lab.last3_median}")
    print(f"Prev-3 median: {lab.prev3_median}")
    print(f"Risk: {lab.risk} | Trend: {lab.trend} | Confidence: {lab.confidence}")
    if lab.note:
        print(f"Note: {lab.note}")
    if lab.recent:
        print("\nMost recent weeks:")
        for r in lab.recent[::-1]:
            print(f"- {r['epiweek']} | {r['percent_positive']:.3f} | percent_positive")
    print("")

    print("Hospitalizations (FluSurv-NET)")
    print(f"Region: {geo.state_abbr} (state-level)")
    print("Metric used: hospitalization rates (network coverage varies)")
    print(f"Lookback used: {hosp.lookback_weeks} weeks")
    print(f"Recent points shown: {hosp.recent_points}")
    print(f"Risk: {hosp.risk} | Trend: {hosp.trend} | Confidence: {hosp.confidence}")
    if hosp.note:
        print(f"Note: {hosp.note}")
    print("")

    if args.json:
        payload = {
            "zip_code": geo.zip_code,
            "place": geo.place,
            "state_name": geo.state_name,
            "state_abbr": geo.state_abbr,
            "county_name": geo.county_name,
            "county_fips": geo.county_fips,
            "generated_date": date.today().isoformat(),
            "source": "delphi_epidata_fluview_clinical",
            "weeks_requested": args.weeks,
            "results": {
                "lab_positivity": asdict(lab),
                "hospitalizations": asdict(hosp),
            },
        }
        print(json.dumps(payload, indent=2, sort_keys=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

