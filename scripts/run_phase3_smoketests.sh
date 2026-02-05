#!/usr/bin/env bash
set -Eeuo pipefail

# Phase 3 smoke tests (v2)
# - Ensures schema
# - Starts API locally
# - Calls /health
# - Calls /signals/latest for a KNOWN zip (default 60614)
# - Prompts user for a NEW zip and calls /signals/latest for it
#   - If the NEW zip had no cached data, expects refreshed=true (read-through cache)
#
# Requires env:
#   DATABASE_URL
# Optional:
#   UVCEED_API_KEY (if set, tests include Authorization header)
#
# Optional env overrides:
#   PORT=8001
#   KNOWN_ZIP=60614
#   BASE_URL=http://127.0.0.1:8001

PORT="${PORT:-8001}"
KNOWN_ZIP="${KNOWN_ZIP:-60614}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT}}"

AUTH_HEADER=()
if [[ -n "${UVCEED_API_KEY:-}" ]]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${UVCEED_API_KEY}")
fi

prompt_zip () {
  local prompt="$1"
  local default_val="$2"
  local z
  read -r -p "${prompt} [${default_val}]: " z || true
  z="${z:-$default_val}"
  if [[ ! "$z" =~ ^[0-9]{5}$ ]]; then
    echo "ERROR: ZIP must be a 5-digit string (got: $z)" >&2
    exit 2
  fi
  echo "$z"
}

echo "Ensuring schema..."
python3 -m uvceed_api.db_migrate

echo "Starting API on ${BASE_URL}..."
python3 -m uvicorn uvceed_api.main:app --host 127.0.0.1 --port "${PORT}" --log-level warning &
PID=$!
cleanup() { kill "$PID" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# wait for server
for i in {1..60}; do
  if curl -fsS "${BASE_URL}/health" "${AUTH_HEADER[@]}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

echo
echo "GET /health"
curl -fsS "${BASE_URL}/health" "${AUTH_HEADER[@]}" | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))'

echo
echo "GET /signals/latest?zip=${KNOWN_ZIP}  (known ZIP smoke test)"
resp_known="$(curl -fsS "${BASE_URL}/signals/latest?zip=${KNOWN_ZIP}" "${AUTH_HEADER[@]}")"
echo "$resp_known" | python3 - <<'PY'
import json,sys
j=json.load(sys.stdin)
assert "signals" in j, "missing signals"
for k in ("wastewater","nssp_ed_visits"):
    assert k in j["signals"], f"missing {k}"
print(json.dumps(j, indent=2))
PY

echo
NEW_ZIP="$(prompt_zip "Enter a NEW ZIP to test read-through cache (ideally not already in DB)" "62401")"

echo
echo "GET /signals/latest?zip=${NEW_ZIP}  (new ZIP read-through cache test)"
resp_new="$(curl -fsS "${BASE_URL}/signals/latest?zip=${NEW_ZIP}" "${AUTH_HEADER[@]}")"
echo "$resp_new" | python3 - <<'PY'
import json,sys
j=json.load(sys.stdin)
assert "signals" in j, "missing signals"
for k in ("wastewater","nssp_ed_visits"):
    assert k in j["signals"], f"missing {k}"
# We *expect* refreshed=True when the ZIP is genuinely new/missing or stale.
# If the ZIP already exists and is fresh, refreshed may be False. We'll warn rather than fail hard.
if not j.get("refreshed", False):
    print("WARN: refreshed=false. This likely means the ZIP already had fresh cached data in signal_snapshots.")
print(json.dumps(j, indent=2))
PY

echo
echo "OK: Phase 3 smoke tests (v2) passed."
