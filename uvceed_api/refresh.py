import json
import subprocess
import datetime as dt
import sys
from typing import Dict, List, Optional, Tuple, Any

from . import config
from .db import (
    advisory_key,
    advisory_unlock,
    insert_signal_snapshot,
    latest_snapshots,
    mark_zip_refreshed,
    try_advisory_lock,
)

UTC = dt.timezone.utc

def _is_stale(row: Optional[dict], ttl_hours: float) -> bool:
    if not row:
        return True
    ga = row.get("generated_at")
    if not isinstance(ga, dt.datetime):
        return True
    age = dt.datetime.now(UTC) - ga.astimezone(UTC)
    return age.total_seconds() > ttl_hours * 3600

def _run_cmd(args: List[str], timeout_s: int) -> Tuple[int, str, str]:
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout_s)
    return p.returncode, p.stdout, p.stderr

def _parse_dt(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            d = dt.datetime.fromisoformat(v)
            return d if d.tzinfo else d.replace(tzinfo=UTC)
        except Exception:
            pass
    return dt.datetime.now(UTC)

def _extract_meta(signal_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if signal_type == "wastewater":
        rollup = payload.get("rollup") or {}
        meta["risk_level"] = rollup.get("overall_level") or payload.get("risk")
        meta["trend"] = rollup.get("overall_trend") or payload.get("trend")
        meta["confidence"] = rollup.get("overall_confidence") or payload.get("confidence")
        meta["composite_score"] = rollup.get("overall_score")
        meta["state"] = payload.get("state_abbr")
        meta["county_fips"] = payload.get("county_fips")
        meta["pathogen"] = None
    elif signal_type == "nssp_ed_visits":
        scores = payload.get("scores") or {}
        meta["risk_level"] = payload.get("risk")
        meta["trend"] = payload.get("trend")
        meta["confidence"] = payload.get("confidence")
        meta["composite_score"] = scores.get("composite_score")
        meta["state"] = payload.get("state_abbr")
        meta["county_fips"] = payload.get("county_fips")
        meta["pathogen"] = payload.get("pathogen") or payload.get("metric_used")
    return meta

def refresh_zip(conn, zip_code: str, force: bool = False) -> Tuple[bool, Dict[str, str]]:
    """Refresh signal snapshots for zip_code.

    Runs uvceed_alerts ingestion scripts WITHOUT --db, then inserts results
    into signal_snapshots from this process. This avoids lock contention
    between the API request transaction and a child process writing to DB.
    """
    errors: Dict[str, str] = {}
    refreshed_any = False

    current = latest_snapshots(conn, zip_code, config.SIGNAL_TYPES)

    needed: List[str] = []
    for st in config.SIGNAL_TYPES:
        if force:
            needed.append(st)
        else:
            ttl = config.TTL_HOURS_WASTEWATER if st == "wastewater" else config.TTL_HOURS_NSSP_ED_VISITS
            if _is_stale(current.get(st), ttl):
                needed.append(st)

    for st in needed:
        key = advisory_key(zip_code, st)
        if not try_advisory_lock(conn, key):
            continue

        try:
            if st == "wastewater":
                cmd = [sys.executable, "-m", "uvceed_alerts.cdc_wastewater", zip_code, "--json"]
            elif st == "nssp_ed_visits":
                cmd = [
                    sys.executable, "-m", "uvceed_alerts.cdc_nssp_ed_visits",
                    zip_code,
                    "--pathogen", config.NSSP_PATHOGEN,
                    "--weeks", str(config.NSSP_WEEKS),
                    "--json-only",
                ]
            else:
                continue

            rc, out, err = _run_cmd(cmd, config.REFRESH_TIMEOUT_SECONDS)
            if rc != 0:
                errors[st] = (err.strip() or out.strip() or f"refresh failed with exit_code={rc}")[:1200]
                continue

            try:
                payload = json.loads(out)
            except Exception:
                # If non-JSON noise got printed, try last JSON-looking line
                last = None
                for line in reversed(out.splitlines()):
                    s = line.strip()
                    if s.startswith("{") and s.endswith("}"):
                        last = s
                        break
                if not last:
                    raise
                payload = json.loads(last)

            gen_at = _parse_dt(payload.get("generated_at"))
            meta = _extract_meta(st, payload)

            insert_signal_snapshot(
                conn,
                zip_code=zip_code,
                signal_type=st,
                generated_at=gen_at,
                payload=payload,
                risk_level=meta.get("risk_level"),
                trend=meta.get("trend"),
                confidence=meta.get("confidence"),
                composite_score=meta.get("composite_score"),
                pathogen=meta.get("pathogen"),
                state=meta.get("state"),
                county_fips=meta.get("county_fips"),
            )
            refreshed_any = True

        except subprocess.TimeoutExpired:
            errors[st] = f"refresh timed out after {config.REFRESH_TIMEOUT_SECONDS}s"
        except Exception as e:
            errors[st] = str(e)[:1200]
        finally:
            advisory_unlock(conn, key)

    if refreshed_any:
        mark_zip_refreshed(conn, zip_code)

    return refreshed_any, errors
