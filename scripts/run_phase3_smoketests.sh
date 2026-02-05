#!/usr/bin/env bash
set -Eeuo pipefail

# Phase 3 smoke tests:
# - Ensures schema
# - Starts API locally
# - Calls /health
# - Calls /signals/latest for a zip, expecting signals object with wastewater + nssp_ed_visits
#
# Requires env:
#   DATABASE_URL
#   UVCEED_API_KEY (optional; if set, tests include auth header)
#
# Optional:
#   TEST_ZIP (default 60614)
#   PORT (default 8001)

TEST_ZIP="${TEST_ZIP:-60614}"
PORT="${PORT:-8001}"
BASE_URL="http://127.0.0.1:${PORT}"

AUTH_HEADER=()
if [[ -n "${UVCEED_API_KEY:-}" ]]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${UVCEED_API_KEY}")
fi

echo "Ensuring schema..."
python3 -m uvceed_api.db_migrate

echo "Starting API on ${BASE_URL}..."
python3 -m uvicorn uvceed_api.main:app --host 127.0.0.1 --port "${PORT}" --log-level warning &
PID=$!
cleanup() { kill "$PID" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# wait for server
for i in {1..40}; do
  if curl -fsS "${BASE_URL}/health" "${AUTH_HEADER[@]}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

echo "GET /health"
curl -fsS "${BASE_URL}/health" "${AUTH_HEADER[@]}" | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))'

echo "GET /signals/latest?zip=${TEST_ZIP}"
resp="$(curl -fsS "${BASE_URL}/signals/latest?zip=${TEST_ZIP}" "${AUTH_HEADER[@]}")"
echo "$resp" | python3 -c 'import sys,json; j=json.load(sys.stdin); assert "signals" in j and "wastewater" in j["signals"] and "nssp_ed_visits" in j["signals"]; print(json.dumps(j, indent=2))'

echo "OK: Phase 3 smoke tests passed."
