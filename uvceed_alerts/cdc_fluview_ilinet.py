#!/usr/bin/env python3
"""
CDC FluView / ILINet ingestion via Delphi Epidata FluView endpoint.

Why Delphi?
- Programmatic, stable API
- FluView ILINet outpatient ILI (state/region/national granularity)
Docs: https://cmu-delphi.github.io/delphi-epidata/api/fluview.html

Usage:
  python -m uvceed_alerts.cdc_fluview_ilinet 60614
  python -m uvceed_alerts.cdc_fluview_ilinet 60614 --weeks 12 --lookback-weeks 104
  python -m uvceed_alerts.cdc_fluview_ilinet 60614 --json
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from epiweeks import Week

from uvceed_alerts.geo import zip_to_county


DELPHI_FLUVIEW_URL = "https://delphi.cmu.edu/epidata/fluview/"


# ---------- Helpers for GeoResult (dict / dataclass / pydantic) ----------

def geo_get(geo: Any, key: str, default: Any = None) -> Any:
    """
    Supports:
      - dict-like: geo["state_abbr"]
      - dataclass/obj: geo.state_abbr
      - pydantic v1: geo.dict()
      - pydantic v2: geo.model_dump()
    """
    if geo is None:
        return default

    # dict-like
    if isinstance(geo, dict):
        return geo.get(key, default)

    # pydantic v2
    if hasattr(geo, "model_dump") and callable(getattr(geo, "model_dump")):
        try:
            d = geo.model_dump()
            if isinstance(d, dict):
                return d.get(key, default)
        except Exception:
            pass

    # pydantic v1
    if hasattr(geo, "dict") and callable(getattr(geo, "dict")):
        try:
            d = geo.dict()
            if isinstance(d, dict):
                return d.get(key, default)
        except Exception:
            pass

    # attribute access
    if hasattr(geo, key):
        try:
            return getattr(geo, key)
        except Exception:
            return default

    return default


def geo_to_header(geo: Any, zip_code: str) -> Dict[str, Any]:
    return {
        "zip_code": zip_code,
        "place": geo_get(geo, "place"),
        "state_name": geo_get(geo, "state_name"),
        "state_abbr": geo_get(geo, "state_abbr"),
        "county_name": geo_get(geo, "county_name"),
        "county_fips": geo_get(geo, "county_fips"),
    }


# ---------- ILINet summary ----------

@dataclass(frozen=True)
class ILINetResult:
    region: str
    metric: str                  # "wili" preferred, fallback "ili"
    lookback_weeks: int
    points: int
    last3_median: Optional[float]
    prev3_median: Optional[float]
    risk: str                    # low/moderate/high/unknown
    trend: str                   # rising/falling/flat/unknown
    confidence: str              # high/medium/low
    note: Optional[str]
    recent: List[Dict[str, Any]]


def _today_epiweek() -> int:
    w = Week.fromdate(date.today())
    return w.year * 100 + w.week


def _epiweek_n_weeks_ago(n: int) -> int:
    w = Week.fromdate(date.today()) - n
    return w.year * 100 + w.week


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2 == 1:
        return float(s[m])
    return float((s[m - 1] + s[m]) / 2.0)


def _pct_rank(value: Optional[float], baseline: List[float]) -> Optional[float]:
    if value is None or not baseline:
        return None
    baseline_sorted = sorted(baseline)
    # percentile rank: fraction <= value
    count = 0
    for x in baseline_sorted:
        if x <= value:
            count += 1
        else:
            break
    return 100.0 * count / float(len(baseline_sorted))


def _risk_from_percentile(p: Optional[float]) -> str:
    if p is None:
        return "unknown"
    if p < 50:
        return "low"
    if p < 80:
        return "moderate"
    return "high"


def _trend(last3: Optional[float], prev3: Optional[float]) -> str:
    if last3 is None or prev3 is None:
        return "unknown"
    if prev3 == 0:
        return "unknown"
    ratio = last3 / prev3
    if ratio >= 1.15:
        return "rising"
    if ratio <= 0.85:
        return "falling"
    return "flat"


def _confidence(points: int, weeks_requested: int) -> str:
    if points >= min(weeks_requested, 12):
        return "high"
    if points >= min(weeks_requested, 6):
        return "medium"
    return "low"


def _fetch_fluview(
    region: str,
    epiweek_start: int,
    epiweek_end: int,
    timeout_s: int = 30,
    retries: int = 5,
) -> List[Dict[str, Any]]:
    params = {
        "regions": region,
        "epiweeks": f"{epiweek_start}-{epiweek_end}",
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(DELPHI_FLUVIEW_URL, params=params, timeout=timeout_s)
            r.raise_for_status()
            payload = r.json()
            if payload.get("result") != 1:
                raise RuntimeError(
                    f"Delphi fluview result={payload.get('result')} message={payload.get('message')}"
                )
            return payload.get("epidata", []) or []
        except Exception as e:
            last_err = e
            if attempt < retries:
                sleep_s = 0.7 * attempt
                print(f"[warn] FluView fetch failed (attempt {attempt}/{retries}): {e} — retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                raise RuntimeError(f"FluView fetch failed after {retries} attempts: {last_err}") from last_err

    return []


def _latest_issue_per_epiweek(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Delphi can return multiple revisions (issues). Keep the max issue per epiweek.
    """
    best: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        try:
            ew = int(row.get("epiweek"))
            issue = int(row.get("issue", 0))
        except Exception:
            continue
        if ew not in best or issue > int(best[ew].get("issue", 0)):
            best[ew] = row
    return [best[k] for k in sorted(best.keys())]


def build_ilinet_summary_for_zip(
    zip_code: str,
    weeks: int,
    lookback_weeks: int,
) -> Tuple[Dict[str, Any], ILINetResult]:
    geo = zip_to_county(zip_code)

    state_abbr = geo_get(geo, "state_abbr")
    if not state_abbr:
        raise RuntimeError("geo.py did not return state_abbr; cannot query FluView/ILINet.")

    region = str(state_abbr).lower()

    end_ew = _today_epiweek()
    start_ew = _epiweek_n_weeks_ago(lookback_weeks)

    rows = _fetch_fluview(region=region, epiweek_start=start_ew, epiweek_end=end_ew)
    rows = _latest_issue_per_epiweek(rows)

    metric = "wili"
    series: List[Tuple[int, float]] = []
    for row in rows:
        try:
            ew = int(row.get("epiweek"))
        except Exception:
            continue
        val = row.get(metric)
        if val is None:
            continue
        try:
            series.append((ew, float(val)))
        except Exception:
            continue

    if not series:
        metric = "ili"
        for row in rows:
            try:
                ew = int(row.get("epiweek"))
            except Exception:
                continue
            val = row.get(metric)
            if val is None:
                continue
            try:
                series.append((ew, float(val)))
            except Exception:
                continue

    series_sorted = sorted(series, key=lambda x: x[0])
    baseline_vals = [v for _, v in series_sorted]

    recent_weeks = series_sorted[-weeks:]
    recent_display = [{"epiweek": ew, metric: val} for ew, val in reversed(recent_weeks)]

    last6 = [v for _, v in series_sorted[-6:]]
    last3_vals = last6[-3:] if len(last6) >= 3 else []
    prev3_vals = last6[-6:-3] if len(last6) >= 6 else []

    last3_m = _median(last3_vals)
    prev3_m = _median(prev3_vals)

    p = _pct_rank(last3_m, baseline_vals)
    risk = _risk_from_percentile(p)
    tr = _trend(last3_m, prev3_m)

    points = len(recent_weeks)
    conf = _confidence(points=points, weeks_requested=weeks)

    note = None
    if len(series_sorted) < max(8, weeks // 2):
        note = "limited ILINet weekly history available in lookback window; interpretation may be less stable"

    header = geo_to_header(geo, zip_code)

    ilinet = ILINetResult(
        region=region,
        metric=metric,
        lookback_weeks=lookback_weeks,
        points=points,
        last3_median=last3_m,
        prev3_median=prev3_m,
        risk=risk,
        trend=tr,
        confidence=conf,
        note=note,
        recent=recent_display,
    )
    return header, ilinet


def _print_human(header: Dict[str, Any], ilinet: ILINetResult) -> None:
    print(f"ZIP: {header['zip_code']} -> {header.get('place')}, {header.get('state_name')} ({header.get('state_abbr')})")
    print(f"County: {header.get('county_name')} | FIPS: {header.get('county_fips')}\n")

    print("CDC FluView / ILINet (weekly outpatient ILI)")
    print(f"Region: {ilinet.region.upper()} (state-level)")
    print(f"Metric used: {ilinet.metric}")
    print(f"Lookback used: {ilinet.lookback_weeks} weeks")
    print(f"Recent points shown: {ilinet.points}")

    if ilinet.last3_median is not None:
        print(f"Last-3 median: {ilinet.last3_median:.3g}")
    if ilinet.prev3_median is not None:
        print(f"Prev-3 median: {ilinet.prev3_median:.3g}")

    print(f"Risk: {ilinet.risk} | Trend: {ilinet.trend} | Confidence: {ilinet.confidence}")
    if ilinet.note:
        print(f"Note: {ilinet.note}")

    print("\nMost recent weeks:")
    if not ilinet.recent:
        print("- (no ILINet points returned for this region in the requested window)")
    else:
        for row in ilinet.recent:
            ew = row["epiweek"]
            val = row[ilinet.metric]
            print(f"- {ew} | {val:.3g} | {ilinet.metric}")


def main() -> int:
    ap = argparse.ArgumentParser(description="CDC FluView / ILINet ingestion (state-level) for a ZIP code.")
    ap.add_argument("zip_code", help="5-digit ZIP code")
    ap.add_argument("--weeks", type=int, default=12, help="How many recent epiweeks to print (default: 12)")
    ap.add_argument("--lookback-weeks", type=int, default=104, help="Lookback window for baselining (default: 104)")
    ap.add_argument("--json", action="store_true", help="Emit JSON payload to stdout (after human summary)")
    args = ap.parse_args()

    header, ilinet = build_ilinet_summary_for_zip(
        zip_code=args.zip_code,
        weeks=max(1, args.weeks),
        lookback_weeks=max(12, args.lookback_weeks),
    )

    _print_human(header, ilinet)

    if args.json:
        payload = {
            **header,
            "generated_date": str(date.today()),
            "source": "delphi_epidata_fluview",
            "results": {
                "region": ilinet.region,
                "metric": ilinet.metric,
                "lookback_weeks": ilinet.lookback_weeks,
                "recent_points": ilinet.points,
                "last3_median": ilinet.last3_median,
                "prev3_median": ilinet.prev3_median,
                "risk": ilinet.risk,
                "trend": ilinet.trend,
                "confidence": ilinet.confidence,
                "note": ilinet.note,
                "recent": ilinet.recent,
            },
        }
        print("\n" + json.dumps(payload, indent=2, sort_keys=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

