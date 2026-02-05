import re
import datetime as dt
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from ..auth import require_api_key
from ..db import db_conn, ensure_phase3_schema, latest_snapshots, upsert_zip_request
from ..models import LatestSignalsOut, RefreshIn, SignalOut
from ..refresh import refresh_zip

router = APIRouter()

ZIP_RE = re.compile(r"^\d{5}$")
UTC = dt.timezone.utc

def _normalize_row(st: str, row: Optional[dict]) -> SignalOut:
    if not row:
        return SignalOut(signal_type=st, risk="unknown", trend="unknown", confidence="low", generated_at=None, payload=None)
    ga = row.get("generated_at")
    ga_iso = ga.astimezone(UTC).isoformat().replace("+00:00", "Z") if isinstance(ga, dt.datetime) else None
    return SignalOut(
        signal_type=st,
        risk=(row.get("risk_level") or "unknown"),
        trend=(row.get("trend") or "unknown"),
        confidence=(row.get("confidence") or "low"),
        generated_at=ga_iso,
        payload=row.get("payload"),
    )

@router.get("/signals/latest", response_model=LatestSignalsOut)
def signals_latest(
    zip: str = Query(..., description="5-digit ZIP"),
    _: None = Depends(require_api_key),
):
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="zip must be a 5-digit string")
    with db_conn() as conn:
        ensure_phase3_schema(conn)
        upsert_zip_request(conn, zip)

        # read-through cache: if missing/stale -> refresh and then re-read
        refreshed, errors = refresh_zip(conn, zip, force=False)

        rows = latest_snapshots(conn, zip, ["wastewater", "nssp_ed_visits"])
        signals = {st: _normalize_row(st, rows.get(st)) for st in ["wastewater", "nssp_ed_visits"]}

        # Choose a top-level generated_at as the newest among signals that have it
        newest = None
        for st, s in signals.items():
            if s.generated_at:
                if newest is None or s.generated_at > newest:
                    newest = s.generated_at

        # If everything is missing and refresh errors occurred, surface a 503
        if all(v.payload is None for v in signals.values()) and errors:
            raise HTTPException(status_code=503, detail={"message": "refresh failed and no cached data exists", "errors": errors})

        return LatestSignalsOut(zip_code=zip, generated_at=newest, signals=signals, refreshed=bool(refreshed), errors=(errors or None))

@router.post("/signals/refresh", response_model=LatestSignalsOut)
def signals_refresh(body: RefreshIn, _: None = Depends(require_api_key)):
    zip = body.zip
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="zip must be a 5-digit string")
    with db_conn() as conn:
        ensure_phase3_schema(conn)
        upsert_zip_request(conn, zip)

        refreshed, errors = refresh_zip(conn, zip, force=True)

        rows = latest_snapshots(conn, zip, ["wastewater", "nssp_ed_visits"])
        signals = {st: _normalize_row(st, rows.get(st)) for st in ["wastewater", "nssp_ed_visits"]}

        newest = None
        for st, s in signals.items():
            if s.generated_at:
                if newest is None or s.generated_at > newest:
                    newest = s.generated_at

        if all(v.payload is None for v in signals.values()) and errors:
            raise HTTPException(status_code=503, detail={"message": "refresh failed and no cached data exists", "errors": errors})

        return LatestSignalsOut(zip_code=zip, generated_at=newest, signals=signals, refreshed=bool(refreshed), errors=(errors or None))
