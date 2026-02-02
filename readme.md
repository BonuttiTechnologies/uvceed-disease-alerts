# UVCeed Disease Alerts – CDC Signal Ingestion (Wastewater + FluView/ILINet)

This repo contains Python modules that ingest public CDC respiratory activity signals and produce:
1) Human-readable summaries for development/testing
2) Machine-readable JSON payloads suitable for a backend → mobile pipeline

## Project layout

- `uvceed_alerts/geo.py`
  - ZIP → place/state/county lookup
  - Returns a `GeoResult` object (used by all ingest modules)

- `uvceed_alerts/cdc_wastewater.py`
  - CDC NWSS wastewater ingestion via CDC Socrata datasets
  - Supports multi-pathogen queries (COVID, Flu A, RSV)
  - Produces risk/trend/confidence + rollup suggestion
  - Flags: `--pathogen`, `--all`, `--days`, `--json`

- `uvceed_alerts/wastewater_risk.py`
  - Risk/trend/confidence scoring logic used by `cdc_wastewater.py`
  - Handles sparse/no-data cases conservatively

- `uvceed_alerts/cdc_fluview_ilinet.py`
  - FluView/ILINet ingestion via Delphi Epidata FluView endpoint
  - Provides state-level ILINet (wili preferred, fallback ili)
  - Flags: `--weeks`, `--lookback-weeks`, `--json`

## Setup

Create and activate a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

