#!/usr/bin/env python3
"""
CDC NWSS Wastewater ingestion via Socrata SODA.

Adds:
1) Confidence-aware rollup messaging (no scary "high" message when confidence is low)
2) --json-only (suppresses human output; prints JSON only)
3) Backend-friendly numeric scoring:
   - per pathogen: risk_score, trend_score, confidence_score, composite_score
   - rollup: overall_score + per-pathogen scores
4) Persistence:
   - writes JSON snapshot to data/snapshots/<zip>/<YYYY-MM-DD>_<hhmmss>_wastewater.json
   - writes/updates rolling latest file: data/snapshots/<zip>/latest_wastewater.json

Examples:
  python -m uvceed_alerts.cdc_wastewater 60614 --all --days 120
  python -m uvceed_alerts.cdc_wastewater 60614 --all --days 120 --json
  python -m uvceed_alerts.cdc_wastewater 60614 --all --json-only
  python -m uvceed_alerts.cdc_wastewater 60614 --all --json-only --persist
  python -m uvceed_alerts.cdc_wastewater 60614 --pathogen flu_a --days 120 --persist
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil.parser import isoparse

from uvceed_alerts.geo import zip_to_county
from uvceed_alerts.wastewater_risk import choose_adaptive_window


SODA_BASE = "https://data.cdc.gov/resource"
DEFAULT_TIMEOUT = 30


@dataclass(frozen=True)
class PathogenConfig:
    key: str
    dataset_id: str
    default_pcr_target: str


PATHOGENS: Dict[str, PathogenConfig] = {
    "covid": PathogenConfig(key="covid", dataset_id="j9g8-acpt", default_pcr_target="sars-cov-2"),
    # REQUIRED UPDATE per your request:
    "flu_a": PathogenConfig(key="flu_a", dataset_id="ymmh-divb", default_pcr_target="fluav"),
    "rsv": PathogenConfig(key="rsv", dataset_id="45cq-cw4i", default_pcr_target="rsv"),
}


@dataclass(frozen=True)
class Point:
    day: date
    conc_lin: Optional[float]
    conc: Optional[float]
    units: Optional[str]
    location: Optional[str]
    record_id: Optional[str]


@dataclass(frozen=True)
class PathogenResult:
    pathogen: str
    dataset_id: str
    pcr_target: str
    window_days: int
    daily_points: int
    metric: Optional[str]
    last7_median: Optional[float]
    prev7_median: Optional[float]
    risk: str
    trend: str
    confidence: str
    note: Optional[str]

    # Backend-friendly numeric scores
    risk_score: float
    trend_score: float
    confidence_score: float
    composite_score: float


@dataclass(frozen=True)
class SnapshotResult:
    zip_code: str
    place: str
    state_name: str
    state_abbr: str
    county_name: str
    county_fips: str
    generated_at: str
    days_requested: int
    results: List[PathogenResult]
    rollup: Dict[str, Any]


# ---------------------------
# Helpers / scoring
# ---------------------------

def _endpoint(dataset_id: str) -> str:
    return f"{SODA_BASE}/{dataset_id}.json"


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


def _parse_date(v: Any) -> Optional[date]:
    if not v:
        return None
    try:
        return isoparse(str(v)).date()
    except Exception:
        return None


def _parse_float(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _risk_score(level: str) -> float:
    # 0..1 scale
    m = {"unknown": 0.0, "low": 0.25, "moderate": 0.6, "high": 1.0}
    return float(m.get(level, 0.0))


def _trend_score(trend: str) -> float:
    # -0.25..+0.25 bump
    m = {"falling": -0.25, "stable": 0.0, "rising": 0.25, "unknown": 0.0}
    return float(m.get(trend, 0.0))


def _confidence_score(conf: str) -> float:
    m = {"low": 0.4, "medium": 0.7, "high": 1.0}
    return float(m.get(conf, 0.4))


def _composite_score(level: str, trend: str, conf: str) -> float:
    """
    Conservative composite in [0..1]:
    - Base: risk_score
    - Add/Sub trend bump
    - Multiply by confidence_score (downweights uncertain areas)
    """
    base = _risk_score(level)
    bump = _trend_score(trend)
    conf_w = _confidence_score(conf)
    raw = max(0.0, min(1.0, base + bump))
    return round(raw * conf_w, 6)


def _rank_level(level: str) -> int:
    return {"unknown": 0, "low": 1, "moderate": 2, "high": 3}.get(level, 0)


def _rank_conf(conf: str) -> int:
    return {"low": 1, "medium": 2, "high": 3}.get(conf, 1)


# ---------------------------
# CDC dataset access
# ---------------------------

def list_distinct_targets(dataset_id: str, limit: int = 2000) -> List[str]:
    params = {"$select": "distinct pcr_target", "$limit": limit}
    rows = _get_json(dataset_id, params)
    out: List[str] = []
    for r in rows:
        t = (r.get("pcr_target") or "").strip()
        if t:
            out.append(t)
    return sorted(set(out))


def fetch_county_series_dataset(
    dataset_id: str,
    county_fips: str,
    *,
    days: int,
    target: str,
    prefer_location: str = "wwtp",
    limit: int = 5000,
) -> List[Point]:
    since = (date.today() - timedelta(days=days)).isoformat()
    cf = str(county_fips).zfill(5)

    where = (
        f"sample_collect_date >= '{since}' "
        f"AND county_fips = '{cf}' "
        f"AND pcr_target = '{target}'"
    )

    params = {
        "$where": where,
        "$limit": limit,
        "$offset": 0,
        "$order": "sample_collect_date DESC",
        "$select": ",".join(
            [
                "sample_collect_date",
                "county_fips",
                "pcr_target",
                "sample_location",
                "pcr_target_avg_conc",
                "pcr_target_avg_conc_lin",
                "pcr_target_units",
                "record_id",
            ]
        ),
    }

    rows = _get_json(dataset_id, params)

    pts: List[Point] = []
    for r in rows:
        d = _parse_date(r.get("sample_collect_date"))
        if not d:
            continue
        pts.append(
            Point(
                day=d,
                conc_lin=_parse_float(r.get("pcr_target_avg_conc_lin")),
                conc=_parse_float(r.get("pcr_target_avg_conc")),
                units=(r.get("pcr_target_units") or None),
                location=(r.get("sample_location") or None),
                record_id=(r.get("record_id") or None),
            )
        )

    loc = prefer_location.lower().strip()
    wwtp_pts = [p for p in pts if (p.location or "").lower() == loc]
    return wwtp_pts if wwtp_pts else pts


# ---------------------------
# Core execution
# ---------------------------

def run_one(
    geo,
    *,
    pathogen_key: str,
    days_requested: int,
    pcr_target_override: Optional[str],
) -> Tuple[PathogenResult, List[Any]]:
    cfg = PATHOGENS[pathogen_key]
    dataset_id = cfg.dataset_id
    target = pcr_target_override or cfg.default_pcr_target

    def _fetch_fn(county_fips: str, *, days: int, target: str, prefer_location: str):
        return fetch_county_series_dataset(
            dataset_id,
            county_fips,
            days=days,
            target=target,
            prefer_location=prefer_location,
        )

    days_used, daily, risk = choose_adaptive_window(
        fetch_fn=_fetch_fn,
        county_fips=geo.county_fips,
        target=target,
        prefer_location="wwtp",
        windows=[days_requested, 90, 120, 180] if days_requested != 60 else [60, 90, 120, 180],
        min_daily_points_for_trend=14,
        min_daily_points_for_risk=8,
    )

    comp = _composite_score(risk.level, risk.trend, risk.confidence)

    pr = PathogenResult(
        pathogen=pathogen_key,
        dataset_id=dataset_id,
        pcr_target=target,
        window_days=days_used,
        daily_points=risk.points_used,
        metric=risk.metric,
        last7_median=risk.last7_median,
        prev7_median=risk.prev7_median,
        risk=risk.level,
        trend=risk.trend,
        confidence=risk.confidence,
        note=risk.note,
        risk_score=_risk_score(risk.level),
        trend_score=_trend_score(risk.trend),
        confidence_score=_confidence_score(risk.confidence),
        composite_score=comp,
    )
    return pr, daily


def print_human_header(geo) -> None:
    print(f"ZIP: {geo.zip_code} -> {geo.place}, {geo.state_name} ({geo.state_abbr})")
    print(f"County: {geo.county_name} | FIPS: {geo.county_fips}\n")


def print_human(
    pr: PathogenResult,
    daily: List[Any],
    *,
    days_requested: int,
    show: int = 25,
) -> None:
    print(f"CDC Wastewater (dataset {pr.dataset_id})")
    print(f"Pathogen: {pr.pathogen}")
    print(f"pcr_target: {pr.pcr_target}")
    print(f"Window requested: {days_requested} days | Window used: last {pr.window_days} days")
    print(f"Daily points (days w/ measurements): {pr.daily_points}")
    print(f"Metric used: {pr.metric}")
    if pr.last7_median is not None:
        print(f"Last-7 median: {pr.last7_median:.3g}")
    if pr.prev7_median is not None:
        print(f"Prev-7 median: {pr.prev7_median:.3g}")
    print(f"Risk: {pr.risk} | Trend: {pr.trend} | Confidence: {pr.confidence}")
    if pr.note:
        print(f"Note: {pr.note}")
    print(f"Scores: composite={pr.composite_score:.3f} (risk={pr.risk_score:.2f}, trend={pr.trend_score:+.2f}, conf={pr.confidence_score:.2f})")
    print()

    print("Most recent daily medians:")
    for dstat in daily[-show:]:
        print(f"- {dstat.day.isoformat()} | {dstat.value:.3g} | {dstat.metric} | n={dstat.n}")
    print()


# ---------------------------
# Rollup (confidence-aware messaging)
# ---------------------------

def rollup_overall(results: List[PathogenResult]) -> Dict[str, Any]:
    if not results:
        return {
            "overall_level": "unknown",
            "overall_trend": "unknown",
            "overall_confidence": "low",
            "overall_score": 0.0,
            "suggestion": "No wastewater signal available for your area.",
            "per_pathogen_scores": {},
        }

    # Level: max
    overall_level = "unknown"
    for r in results:
        if _rank_level(r.risk) > _rank_level(overall_level):
            overall_level = r.risk

    # Trend: conservative
    trends = [r.trend for r in results if r.trend != "unknown"]
    if any((r.risk == "high" and r.trend == "rising") for r in results):
        overall_trend = "rising"
    elif "rising" in trends:
        overall_trend = "rising"
    elif "falling" in trends and "rising" not in trends:
        overall_trend = "falling"
    elif "stable" in trends:
        overall_trend = "stable"
    else:
        overall_trend = "unknown"

    # Confidence: minimum among pathogens with data
    confs = [r.confidence for r in results if r.daily_points > 0]
    if not confs:
        overall_conf = "low"
    else:
        overall_conf = min(confs, key=_rank_conf)

    # Overall score: max composite among pathogens (conservative)
    overall_score = max((r.composite_score for r in results), default=0.0)

    per_scores = {r.pathogen: r.composite_score for r in results}

    # Confidence-aware suggestions (key change)
    if overall_level == "unknown":
        suggestion = "No wastewater signal available for your area."
    elif overall_conf == "low":
        # soften language when data is sparse
        if overall_level in ("high", "moderate"):
            suggestion = (
                "Limited wastewater sampling in your area. Signals suggest elevated respiratory activity, "
                "but confidence is low. Consider normal precautions; if you’re concerned, you may choose extra disinfection."
            )
        else:
            suggestion = (
                "Limited wastewater sampling in your area. Signals suggest low activity, but confidence is low. "
                "Maintain normal cleaning habits."
            )
    else:
        # medium/high confidence
        if overall_level == "high":
            suggestion = "High respiratory activity detected in your area. Consider increasing disinfection of high-touch surfaces."
        elif overall_level == "moderate":
            suggestion = "Moderate respiratory activity detected in your area. Consider extra disinfection in shared spaces."
        else:
            suggestion = "Low respiratory activity detected in your area. Maintain normal cleaning habits."

    return {
        "overall_level": overall_level,
        "overall_trend": overall_trend,
        "overall_confidence": overall_conf,
        "overall_score": round(float(overall_score), 6),
        "suggestion": suggestion,
        "per_pathogen_scores": per_scores,
    }


# ---------------------------
# Persistence
# ---------------------------

def persist_snapshot(snapshot: SnapshotResult, base_dir: Path, zip_code: str) -> Dict[str, str]:
    """
    Writes:
      data/snapshots/<zip>/<YYYY-MM-DD>_<HHMMSS>_wastewater.json
      data/snapshots/<zip>/latest_wastewater.json
    Returns paths as strings.
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    zip_dir = base_dir / "snapshots" / zip_code
    zip_dir.mkdir(parents=True, exist_ok=True)

    dated_path = zip_dir / f"{ts}_wastewater.json"
    latest_path = zip_dir / "latest_wastewater.json"

    payload = json.dumps(asdict(snapshot), indent=2, default=str)

    dated_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    return {"snapshot_path": str(dated_path), "latest_path": str(latest_path)}


# ---------------------------
# CLI
# ---------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="CDC NWSS wastewater by ZIP (county-scoped).")
    ap.add_argument("zip", help="5-digit ZIP (e.g., 60614)")
    ap.add_argument("--days", type=int, default=60, help="initial lookback days (default 60)")
    ap.add_argument("--pathogen", choices=sorted(PATHOGENS.keys()), default="covid", help="dataset to use (default covid)")
    ap.add_argument("--all", action="store_true", help="run covid + flu_a + rsv in one command")
    ap.add_argument("--pcr-target", default=None, help="override pcr_target string (single-pathogen mode only)")
    ap.add_argument("--list-targets", action="store_true", help="list distinct pcr_target values for this pathogen dataset and exit")
    ap.add_argument("--show", type=int, default=25, help="how many daily points to print (per pathogen)")
    ap.add_argument("--json", action="store_true", help="emit JSON snapshot (printed after human output unless --json-only)")
    ap.add_argument("--json-only", action="store_true", help="emit JSON only (suppresses human output)")
    ap.add_argument("--persist", action="store_true", help="write snapshot JSON to data/snapshots/<zip>/ ...")
    ap.add_argument("--data-dir", default="data", help="base directory for persistence (default: ./data)")
    args = ap.parse_args()

    if args.json_only:
        args.json = True

    cfg = PATHOGENS[args.pathogen]

    if args.list_targets:
        targets = list_distinct_targets(cfg.dataset_id)
        print(f"Dataset {cfg.dataset_id} pcr_target values ({len(targets)}):")
        for t in targets:
            print(f"- {t}")
        return 0

    geo = zip_to_county(args.zip)

    results: List[PathogenResult] = []
    daily_by_pathogen: Dict[str, List[Any]] = {}

    if not args.json_only:
        print_human_header(geo)

    if args.all:
        for pkey in ["covid", "flu_a", "rsv"]:
            pr, daily = run_one(
                geo,
                pathogen_key=pkey,
                days_requested=args.days,
                pcr_target_override=None,
            )
            results.append(pr)
            daily_by_pathogen[pkey] = daily
            if not args.json_only:
                print_human(pr, daily, days_requested=args.days, show=args.show)
    else:
        pr, daily = run_one(
            geo,
            pathogen_key=args.pathogen,
            days_requested=args.days,
            pcr_target_override=args.pcr_target,
        )
        results.append(pr)
        daily_by_pathogen[args.pathogen] = daily
        if not args.json_only:
            print_human(pr, daily, days_requested=args.days, show=args.show)

    roll = rollup_overall(results)

    if not args.json_only:
        print("Overall respiratory snapshot:")
        print(f"- overall_level: {roll['overall_level']}")
        print(f"- overall_trend: {roll['overall_trend']}")
        print(f"- overall_confidence: {roll['overall_confidence']}")
        print(f"- overall_score: {roll['overall_score']}")
        print(f"- suggestion: {roll['suggestion']}")
        print()

    if args.json:
        snap = SnapshotResult(
            zip_code=geo.zip_code,
            place=geo.place,
            state_name=geo.state_name,
            state_abbr=geo.state_abbr,
            county_name=geo.county_name,
            county_fips=geo.county_fips,
            generated_at=datetime.now().isoformat(timespec="seconds"),
            days_requested=args.days,
            results=results,
            rollup=roll,
        )

        persist_info: Optional[Dict[str, str]] = None
        if args.persist:
            base_dir = Path(args.data_dir)
            persist_info = persist_snapshot(snap, base_dir, geo.zip_code)
            # include file paths in JSON output for cron debugging
            roll_with_paths = dict(roll)
            roll_with_paths["persist"] = persist_info
            snap = SnapshotResult(
                zip_code=snap.zip_code,
                place=snap.place,
                state_name=snap.state_name,
                state_abbr=snap.state_abbr,
                county_name=snap.county_name,
                county_fips=snap.county_fips,
                generated_at=snap.generated_at,
                days_requested=snap.days_requested,
                results=snap.results,
                rollup=roll_with_paths,
            )

        print(json.dumps(asdict(snap), indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

