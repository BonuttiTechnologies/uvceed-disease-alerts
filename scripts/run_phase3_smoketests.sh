#!/usr/bin/env bash
set -euo pipefail

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8001}"
BASE="http://${API_HOST}:${API_PORT}"

echo "Ensuring schema..."
python -m uvceed_api.db_migrate
echo "OK: Phase 3 schema ensured (signal_snapshots + zip_requests + indexes)."

echo "Starting API on ${BASE}..."
uvicorn uvceed_api.main:app --host "${API_HOST}" --port "${API_PORT}" --log-level warning &
API_PID=$!
cleanup() { kill "${API_PID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# wait for health
for i in $(seq 1 40); do
  code="$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/health" || true)"
  if [[ "${code}" == "200" ]]; then break; fi
  sleep 0.25
done

echo
echo "GET /health  -> HTTP $(curl -s -o /dev/null -w "%{http_code}" "${BASE}/health")"
echo "---- body ----"
curl -s "${BASE}/health"; echo
echo "--------------"
curl -s "${BASE}/health" | python -c 'import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))'
echo

ZIP_KNOWN="${ZIP_KNOWN:-60614}"
echo "Known ZIP smoke test (shape check)"
echo
CODE_KNOWN="$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/signals/latest?zip=${ZIP_KNOWN}")"
echo "GET /signals/latest?zip=${ZIP_KNOWN}  -> HTTP ${CODE_KNOWN}"
echo "---- body ----"
BODY_KNOWN="$(curl -s "${BASE}/signals/latest?zip=${ZIP_KNOWN}")"
echo "${BODY_KNOWN}"; echo
echo "--------------"
if [[ "${CODE_KNOWN}" != "200" ]]; then
  echo "ERROR: known zip call failed" >&2
  exit 1
fi
echo "${BODY_KNOWN}" | python -c 'import sys,json; obj=json.load(sys.stdin); assert "zip_code" in obj; assert "signals" in obj; print(json.dumps(obj, indent=2))'
echo

DEFAULT_NEW="${ZIP_NEW_DEFAULT:-62401}"
read -r -p "Enter a NEW ZIP to test read-through cache (ideally not already in DB) [${DEFAULT_NEW}]: " ZIP_NEW
ZIP_NEW="${ZIP_NEW:-$DEFAULT_NEW}"

echo
echo "New ZIP read-through cache test (should refresh if missing/stale)"
echo
CODE_NEW="$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/signals/latest?zip=${ZIP_NEW}")"
echo "GET /signals/latest?zip=${ZIP_NEW}  -> HTTP ${CODE_NEW}"
echo "---- body ----"
BODY_NEW="$(curl -s "${BASE}/signals/latest?zip=${ZIP_NEW}")"
echo "${BODY_NEW}"; echo
echo "--------------"
if [[ "${CODE_NEW}" != "200" ]]; then
  echo "ERROR: /signals/latest failed for new zip ${ZIP_NEW}" >&2
  exit 1
fi
echo "${BODY_NEW}" | python -c 'import sys,json; obj=json.load(sys.stdin); assert "zip_code" in obj; assert "signals" in obj; print(json.dumps(obj, indent=2))'

echo
echo "OK: Phase 3 smoke tests passed."
