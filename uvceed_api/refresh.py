import subprocess
import time
import datetime as dt
from typing import Dict, List, Optional, Tuple

from . import config
from .db import (
    advisory_key,
    advisory_unlock,
    latest_snapshots,
    mark_zip_refreshed,
    try_advisory_lock,
)

UTC = dt.timezone.utc

def _is_stale(row: Optional[dict], ttl_hours: float) -> bool:
    if not row:
        return True
    ga = row.get("generated_at")
    if ga is None:
        return True
    # psycopg2 returns datetime objects for timestamptz
    if isinstance(ga, dt.datetime):
        age = dt.datetime.now(UTC) - ga.astimezone(UTC)
        return age.total_seconds() > ttl_hours * 3600
    return True

def _run_cmd(args: List[str], timeout_s: int) -> Tuple[int, str, str]:
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout_s)
    return p.returncode, p.stdout, p.stderr

def refresh_zip(conn, zip_code: str, force: bool = False) -> Tuple[bool, Dict[str, str]]:
    """Refresh configured signal types for a zip_code.

    Returns: (refreshed_any, errors_by_signal_type)
    """
    errors: Dict[str, str] = {}
    refreshed_any = False

    # Determine which signal types need refresh
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
        got_lock = try_advisory_lock(conn, key)
        if not got_lock:
            # Another worker is already refreshing. Skip; caller will re-query.
            continue

        try:
            if st == "wastewater":
                cmd = ["python3", "-m", "uvceed_alerts.cdc_wastewater", zip_code, "--db", "--json"]
            elif st == "nssp_ed_visits":
                cmd = [
                    "python3", "-m", "uvceed_alerts.cdc_nssp_ed_visits",
                    zip_code,
                    "--pathogen", config.NSSP_PATHOGEN,
                    "--weeks", str(config.NSSP_WEEKS),
                    "--db",
                    "--json-only",
                ]
            else:
                continue

            rc, out, err = _run_cmd(cmd, config.REFRESH_TIMEOUT_SECONDS)
            if rc != 0:
                errors[st] = (err.strip() or out.strip() or f"refresh failed with exit_code={rc}")[:500]
            else:
                refreshed_any = True
        except subprocess.TimeoutExpired:
            errors[st] = f"refresh timed out after {config.REFRESH_TIMEOUT_SECONDS}s"
        except Exception as e:
            errors[st] = str(e)[:500]
        finally:
            advisory_unlock(conn, key)

    if refreshed_any:
        mark_zip_refreshed(conn, zip_code)
    return refreshed_any, errors
