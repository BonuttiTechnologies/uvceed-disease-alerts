# UVCeed Phase 3 API (read-through cache)

## What this provides
- `GET /health`
- `GET /signals/latest?zip=60614`
  - Read-through cache: if missing/stale -> runs ingestion scripts and populates `signal_snapshots`
- `POST /signals/refresh`
  - Forces refresh

Signals returned (option 2):
- `wastewater`
- `nssp_ed_visits`

## Environment
Required:
- `DATABASE_URL` (Postgres)
Optional but recommended:
- `UVCEED_API_KEY` (if set, endpoints require `Authorization: Bearer <key>`)

Tuning:
- `UVCEED_TTL_HOURS_WASTEWATER` (default 12)
- `UVCEED_TTL_HOURS_NSSP_ED_VISITS` (default 12)
- `UVCEED_NSSP_WEEKS` (default 16)
- `UVCEED_NSSP_PATHOGEN` (default combined)
- `UVCEED_REFRESH_TIMEOUT_SECONDS` (default 55)

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally
```bash
export DATABASE_URL='postgresql://...'
export UVCEED_API_KEY='devkey'   # optional
python3 -m uvceed_api.db_migrate
python3 -m uvicorn uvceed_api.main:app --host 0.0.0.0 --port 8000
```

## Smoke test
```bash
scripts/run_phase3_smoketests.sh
```

## Cron (daily refresh of requested ZIPs)
```bash
REPO_ROOT=/home/uvceed/uvceed-disease-alerts scripts/install_cron_daily_refresh.sh
```

This installs a cron that runs:
- `python3 -m uvceed_api.db_migrate`
- `python3 -m uvceed_api.cli_refresh_requested --days 30`
