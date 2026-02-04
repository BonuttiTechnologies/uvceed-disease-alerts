#!/usr/bin/env python3
"""
cdc_wastewater.py

CDC NWSS wastewater signal by ZIP -> county-level series, with adaptive window selection.
Optional JSON output + local persistence + Postgres persistence.

DB persistence:
  --db  -> persists snapshot to Postgres using DATABASE_URL (see uvceed_alerts/db.py)
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from uvceed_alerts.config import CDC_APP_TOKEN
from uvceed_alerts.geo import zip_to_county

# --- DB persistence (optional) ---
# You must provide uvceed_alerts/db.py with:
#   connect(), ensure_schema(conn), insert_wastewater_snapshot(conn, snapshot_dict)
try:
    from uvceed_alerts.db import connect, ensure_schema, insert_wastewater_snapshot  # type: ignore
except Exception:
    connect = None
    ensure_schema = None
    insert_wastewater_snapshot = None


# ---------------------------
# Dataset config
# ---------------------------

@dataclass(frozen=True)
class PathogenCfg:
    key: str
    dataset_id: str
    default_pcr_target: str


# CDC NWSS on data.cdc.gov (Socrata)
PATHOGENS: Dict[str, PathogenCfg] = {
    # NOTE: dataset may change; these IDs match your current implementation
    "covid": PathogenCfg("covid", "j9g8-acpt", "sars-cov-2"),
    "flu_a": PathogenCfg("flu_a", "j9g8-acpt", "influenza-a"),
    "rsv": PathogenCfg("rsv", "j9g8-acpt", "rsv"),
}


# ---------------------------
# Output dataclasses
# ---------------------------

@dataclass
class PathogenResult:
    pathogen: str
    dataset_id: str
    pcr_target: str
    window_days: int
    daily_points: int
    metric: str
    last7_median: Optional[float]
    prev7_median: Optional[float]
    risk: str
    trend: str
    confidence: str
    note: Optional[str]
    risk_score: float
    trend_score: float
    confidence_score: float
    composite_score: float


@dataclass
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
# Socrata helpers
# ---------------------------

SOCRATA_BASE = "https://data.cdc.gov/resource"


def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if CDC_APP_TOKEN:
        h["X-App-Token"] = CDC_APP_TOKEN
    return h


def _get_json(dataset_id: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    r = requests.get(url, params=params, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    # expected formats include "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS.000"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception:
            return None


def _parse_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# ---------------------------
# Risk scoring / trend
# ---------------------------

def _rank_level(level: str) -> int:
    return {"unknown": 0, "low": 1, "moderate": 2, "high": 3}.get(level, 0)


def _rank_conf(conf: str) -> int:
    return {"low": 0, "moderate": 1, "high": 2}.get(conf, 0)


def _risk_score(level: str) -> float:
    return {"unknown": 0.0, "low": 0.25, "moderate": 0.60, "high": 1.00}.get(level, 0.0)


def _trend_score(trend: str) -> float:
    return {"unknown": 0.0, "stable": 0.0, "flat": 0.0, "falling": -0.25, "rising": 0.25}.get(trend, 0.0)


def _confidence_score(conf: str) -> float:
    return {"low": 0.25, "moderate": 0.60, "high": 1.00}.get(conf, 0.25)


def _composite_score(level: str, trend: str, conf: str) -> float:
    # weighted average-ish: risk dominates, then trend, then confidence
    return round(
        0.70 * _risk_score(level) + 0.15 * (0.5 + _trend_score(trend)) + 0.15 * _confidence_score(conf),
        6,
    )


@dataclass
class DailyStat:
    day: date
    value: float
    metric: str
    n: int


@dataclass
class RiskResult:
    level: str
    trend: str
    confidence: str
    metric: str
    last7_median: Optional[float]
    prev7_median: Optional[float]
    points_used: int
    note: Optional[str]


def _median_safe(vals: List[float]) -> Optional[float]:
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return float(median(vals))


def _trend_from_medians(last7: Optional[float], prev7: Optional[float]) -> str:
    if last7 is None or prev7 is None:
        return "unknown"
    # simple direction with small deadband
    if prev7 == 0:
        return "unknown"
    ratio = last7 / prev7
    if ratio >= 1.15:
        return "rising"
    if ratio <= 0.85:
        return "falling"
    return "stable"


def _risk_level_from_value(val: Optional[float]) -> str:
    if val is None:
        return "unknown"
    # crude buckets; you can tune later
    # (values vary by dataset/location; treat this as a relative heuristic)
    if val >= 2.5e5:
        return "high"
    if val >= 8.0e4:
        return "moderate"
    return "low"


def compute_risk(daily: List[DailyStat]) -> RiskResult:
    if not daily:
        return RiskResult(
            level="unknown",
            trend="unknown",
            confidence="low",
            metric="pcr_target_avg_conc_lin",
            last7_median=None,
            prev7_median=None,
            points_used=0,
            note="no wastewater data returned",
        )

    values = [d.value for d in daily]
    # last 7 non-empty points vs previous 7 non-empty points
    last7 = values[-7:] if len(values) >= 7 else values
    prev7 = values[-14:-7] if len(values) >= 14 else []

    last7_med = _median_safe(last7)
    prev7_med = _median_safe(prev7)

    trend = _trend_from_medians(last7_med, prev7_med)
    level = _risk_level_from_value(last7_med)

    # confidence based on points
    pts = len(daily)
    if pts >= 21:
        conf = "high"
    elif pts >= 14:
        conf = "moderate"
    else:
        conf = "low"

    note = None
    if pts < 14:
        note = "limited data points for trend; confidence reduced"

    return RiskResult(
        level=level,
        trend=trend if conf != "low" else ("unknown" if trend != "unknown" else "unknown"),
        confidence=conf,
        metric=daily[-1].metric if daily else "pcr_target_avg_conc_lin",
        last7_median=last7_med,
        prev7_median=prev7_med,
        points_used=pts,
        note=note,
    )


# ---------------------------
# Adaptive window selection
# ---------------------------

def choose_adaptive_window(
    *,
    fetch_fn: Callable[..., List[DailyStat]],
    county_fips: str,
    target: str,
    prefer_location: str,
    windows: List[int],
    min_daily_points_for_trend: int,
    min_daily_points_for_risk: int,
) -> Tuple[int, List[DailyStat], RiskResult]:
    last_note = None
    for w in windows:
        daily = fetch_fn(county_fips, days=w, target=target, prefer_location=prefer_location)
        risk = compute_risk(daily)
        last_note = risk.note
        if risk.points_used >= min_daily_points_for_trend:
            return w, daily, risk
        # If we at least have enough to assign risk but not trend, keep trying larger window.
        if risk.points_used >= min_daily_points_for_risk:
            # keep searching for better trend signal, but retain current
            best = (w, daily, risk)

    # If we never hit trend threshold, return best we got (or the last attempt)
    if "best" in locals():
        return best
    # fall back to last attempted window
    if windows:
        w = windows[-1]
        daily = fetch_fn(county_fips, days=w, target=target, prefer_location=prefer_location)
        risk = compute_risk(daily)
        if not risk.note and last_note:
            risk.note = last_note
        return w, daily, risk

    return 0, [], compute_risk([])


# ---------------------------
# Data fetch
# ---------------------------

def list_distinct_targets(dataset_id: str) -> List[str]:
    params = {"$select": "distinct pcr_target", "$limit": 5000}
    rows = _get_json(dataset_id, params)
    out = []
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
) -> List[DailyStat]:
    """
    Pull raw rows for county_fips and pcr_target, then produce daily medians.
    """
    end = date.today()
    start = end - timedelta(days=days)

    params = {
        "$limit": 50000,
        "$offset": 0,
        "$order": "sample_collect_date ASC",
        "$where": (
            f"county_fips='{county_fips}' AND "
            f"lower(pcr_target)='{target.lower()}' AND "
            f"sample_collect_date >= '{start.isoformat()}'"
        ),
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

    # Coerce into points
    points: List[Tuple[date, float, str]] = []
    for r in rows:
        d = _parse_date(r.get("sample_collect_date"))
        if not d:
            continue
        v = _parse_float(r.get("pcr_target_avg_conc_lin"))
        if v is None:
            continue
        loc = (r.get("sample_location") or "").strip()
        points.append((d, v, loc))

    if not points:
        return []

    # Prefer a specific location if present (matching is exact case-insensitive)
    pref = prefer_location.lower().strip()
    preferred = [(d, v) for (d, v, loc) in points if loc.lower() == pref]
    use_points = preferred if preferred else [(d, v) for (d, v, _) in points]

    # Daily median
    by_day: Dict[date, List[float]] = {}
    for d, v in use_points:
        by_day.setdefault(d, []).append(v)

    daily: List[DailyStat] = []
    for d in sorted(by_day.keys()):
        vals = by_day[d]
        daily.append(DailyStat(day=d, value=float(median(vals)), metric="pcr_target_avg_conc_lin", n=len(vals)))

    return daily


# ---------------------------
# Core execution
# ---------------------------

def run_one(
    geo,
    *,
    pathogen_key: str,
    days_requested: int,
    pcr_target_override: Optional[str],
) -> Tuple[PathogenResult, List[DailyStat]]:
    cfg = PATHOGENS[pathogen_key]
    dataset_id = cfg.dataset_id
    target = pcr_target_override or cfg.default_pcr_target

    def _fetch_fn(county_fips: str, *, days: int, target: str, prefer_location: str) -> List[DailyStat]:
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


def print_human(pr: PathogenResult, daily: List[DailyStat], *, days_requested: int, show: int = 25) -> None:
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
    print(
        f"Scores: composite={pr.composite_score:.3f} "
        f"(risk={pr.risk_score:.2f}, trend={pr.trend_score:+.2f}, conf={pr.confidence_score:.2f})"
    )
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

    # Confidence-aware suggestions
    if overall_level == "unknown":
        suggestion = "No wastewater signal available for your area."
    elif overall_conf == "low":
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
# Persistence (files)
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
    ap.add_argument("--data-dir", default="data", help="base directory for file persistence (default: ./data)")
    ap.add_argument("--db", action="store_true", help="persist snapshot to Postgres (uses DATABASE_URL)")
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
    daily_by_pathogen: Dict[str, List[DailyStat]] = {}

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

    # Build snapshot if needed for JSON and/or DB persistence
    snap: Optional[SnapshotResult] = None
    if args.json or args.db or args.persist:
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

    # File persistence
    if snap and args.persist:
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

    # DB persistence
    if args.db:
        if not os.getenv("DATABASE_URL"):
            raise SystemExit("DATABASE_URL is not set. Export DATABASE_URL before using --db.")
        if connect is None or ensure_schema is None or insert_wastewater_snapshot is None:
            raise SystemExit("uvceed_alerts.db is missing required functions (connect/ensure_schema/insert_wastewater_snapshot).")

        assert snap is not None  # guaranteed above
        with connect() as conn:
            ensure_schema(conn)
            snapshot_id = insert_wastewater_snapshot(conn, asdict(snap))

        if not args.json_only:
            print(f"Saved snapshot to DB (wastewater_snapshots.id={snapshot_id})")
            print()

        # optionally include snapshot_id in JSON output
        if snap and args.json:
            roll2 = dict(snap.rollup)
            roll2["db"] = {"wastewater_snapshots_id": snapshot_id}
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
                rollup=roll2,
            )

    # JSON output
    if args.json:
        assert snap is not None
        print(json.dumps(asdict(snap), indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

