#!/usr/bin/env bash
set -Eeuo pipefail

# Installs a daily cron that refreshes requested ZIPs.
# Requires:
#   - DATABASE_URL set in the environment loaded by this script (or sourced from .env)
#   - Repo at REPO_ROOT
#   - python3 available

REPO_ROOT="${REPO_ROOT:-/home/uvceed/uvceed-disease-alerts}"
OUT_LOG="${OUT_LOG:-/home/uvceed/cdc_cron_runs/refresh_requested.log}"
DAYS="${DAYS:-30}"

mkdir -p "$(dirname "$OUT_LOG")"

CRON_LINE="15 6 * * * cd $REPO_ROOT && /usr/bin/env bash -lc 'python3 -m uvceed_api.db_migrate && python3 -m uvceed_api.cli_refresh_requested --days $DAYS' >> $OUT_LOG 2>&1"

( crontab -l 2>/dev/null | grep -v 'uvceed_api.cli_refresh_requested' || true; echo "$CRON_LINE" ) | crontab -

echo "Installed cron:"
echo "$CRON_LINE"
