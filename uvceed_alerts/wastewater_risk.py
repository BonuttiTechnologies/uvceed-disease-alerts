from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple


# We’ll use the existing “Point” concept from cdc_wastewater.py:
#   Point(day, conc_lin, conc, units, location, record_id)
# But this module stays decoupled and works with any object
# shaped like: {"day": date, "conc_lin": float|None, "conc": float|None}

@dataclass(frozen=True)
class DailyStat:
    day: date
    value: float
    metric: str
    n: int  # number of raw samples contributing to this day


@dataclass(frozen=True)
class RiskResult:
    level: str            # low/moderate/high/unknown
    trend: str            # rising/stable/falling/unknown
    confidence: str       # high/medium/low
    last7_median: Optional[float]
    prev7_median: Optional[float]
    metric: Optional[str]
    points_used: int      # number of daily points available
    note: Optional[str] = None


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    v = sorted(vals)
    return float(v[len(v) // 2])


def build_daily_median(points: List[object]) -> List[DailyStat]:
    """
    Build DailyStat list from raw points.
    Prefer conc_lin when available, else conc.
    """
    by_day: Dict[date, List[Tuple[Optional[float], Optional[float]]]] = {}
    for p in points:
        d = getattr(p, "day", None) or p.get("day")  # supports dataclass or dict
        if not d:
            continue
        conc_lin = getattr(p, "conc_lin", None) if hasattr(p, "conc_lin") else p.get("conc_lin")
        conc = getattr(p, "conc", None) if hasattr(p, "conc") else p.get("conc")
        by_day.setdefault(d, []).append((conc_lin, conc))

    out: List[DailyStat] = []
    for d in sorted(by_day.keys()):
        rows = by_day[d]
        vals_lin = [x[0] for x in rows if x[0] is not None]
        vals = [x[1] for x in rows if x[1] is not None]

        if vals_lin:
            med = _median(vals_lin)
            if med is not None:
                out.append(DailyStat(day=d, value=med, metric="pcr_target_avg_conc_lin", n=len(vals_lin)))
        elif vals:
            med = _median(vals)
            if med is not None:
                out.append(DailyStat(day=d, value=med, metric="pcr_target_avg_conc", n=len(vals)))

    return out


def compute_confidence(daily: List[DailyStat]) -> Tuple[str, Optional[str]]:
    """
    Confidence is based on density + recency coverage.
    This is a v1 heuristic (simple, explainable).

    high:
      - >= 18 daily points in window, and
      - median daily sample count >= 3
    medium:
      - >= 10 daily points, and
      - median daily sample count >= 2
    low:
      - anything less
    """
    if not daily:
        return "low", "no measurements available"

    points = len(daily)
    ns = [d.n for d in daily]
    med_n = _median([float(x) for x in ns]) or 0.0

    if points >= 18 and med_n >= 3:
        return "high", None
    if points >= 10 and med_n >= 2:
        return "medium", None
    return "low", "sparse measurements (limited sampling frequency/sites)"


def compute_risk(
    daily: List[DailyStat],
    *,
    drop_last_days: int = 2,
) -> RiskResult:
    """
    Produces:
      - level: low/moderate/high based on percentile within available window
      - trend: rising/stable/falling only when sufficient data for 14-point comparison
      - confidence: high/medium/low based on sampling density
    """
    if not daily:
        return RiskResult(
            level="unknown",
            trend="unknown",
            confidence="low",
            last7_median=None,
            prev7_median=None,
            metric=None,
            points_used=0,
            note="no wastewater measurements in window",
        )

    # Guardrail: newest days can be incomplete/late-reporting.
    daily_eff = daily[:-drop_last_days] if len(daily) > drop_last_days else daily
    if not daily_eff:
        daily_eff = daily  # fallback

    confidence, conf_note = compute_confidence(daily_eff)

    metric = daily_eff[-1].metric
    vals = [d.value for d in daily_eff]
    points_used = len(vals)

    # Trend gating: need >= 14 *daily points* (not calendar days) for a fair comparison
    trend = "unknown"
    last7 = None
    prev7 = None
    if points_used >= 14:
        last7 = _median(vals[-7:])
        prev7 = _median(vals[-14:-7])
        if last7 is not None and prev7 is not None and prev7 != 0:
            pct = (last7 - prev7) / abs(prev7)
            if pct > 0.20:
                trend = "rising"
            elif pct < -0.20:
                trend = "falling"
            else:
                trend = "stable"

    # Level: percentile rank within window (simple and local)
    # Note: ties possible; use sorted position by value
    w_sorted = sorted(vals)
    current = vals[-1]
    # percentile rank based on last occurrence index for stability with duplicates
    idx = max(i for i, v in enumerate(w_sorted) if v <= current)
    pct_rank = idx / max(1, (len(w_sorted) - 1))

    if pct_rank < 0.33:
        level = "low"
    elif pct_rank < 0.67:
        level = "moderate"
    else:
        level = "high"

    note_parts = []
    if conf_note:
        note_parts.append(conf_note)
    if trend == "unknown" and points_used < 14:
        note_parts.append("trend suppressed (insufficient data points)")

    return RiskResult(
        level=level,
        trend=trend,
        confidence=confidence,
        last7_median=last7,
        prev7_median=prev7,
        metric=metric,
        points_used=points_used,
        note="; ".join(note_parts) if note_parts else None,
    )


def choose_adaptive_window(
    fetch_fn,
    county_fips: str,
    *,
    target: str = "sars-cov-2",
    prefer_location: str = "wwtp",
    windows: List[int] = [60, 90, 120, 180],
    min_daily_points_for_trend: int = 14,
    min_daily_points_for_risk: int = 8,
) -> Tuple[int, List[DailyStat], RiskResult]:
    """
    fetch_fn signature should match:
      fetch_fn(county_fips, days=..., target=..., prefer_location=...) -> List[Point]

    We expand the lookback window until we have enough daily points.
    """
    best_days = windows[-1]
    best_daily: List[DailyStat] = []
    best_risk: Optional[RiskResult] = None

    for days in windows:
        pts = fetch_fn(county_fips, days=days, target=target, prefer_location=prefer_location)
        daily = build_daily_median(pts)

        # Need a minimum to compute a meaningful percentile/risk at all
        if len(daily) < min_daily_points_for_risk:
            best_days, best_daily, best_risk = days, daily, compute_risk(daily)
            continue

        risk = compute_risk(daily)

        # If we have enough points to support trend, stop expanding
        if len(daily) >= min_daily_points_for_trend:
            return days, daily, risk

        # Otherwise keep going but remember latest
        best_days, best_daily, best_risk = days, daily, risk

    return best_days, best_daily, best_risk or compute_risk(best_daily)

