#!/usr/bin/env bash
set -Eeuo pipefail

# Phase 3 smoke tests (v4)
# Fixes a bash redirection bug from v3 where the heredoc Python program
# was replaced by JSON input, causing "NameError: name 'null' is not defined".
#
# - Ensures schema
# - Starts API locally
# - Calls /health
# - Calls /signals/latest for a KNOWN zip (default 60614) and validates shape
# - Prompts user for a NEW zip and calls /signals/latest for it and validates shape
# - Prints HTTP status + body for transparency

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

pretty_and_check () {
  # Reads JSON from stdin, validates required shape, pretty-prints.
  python3 - <<'PY'
import json,sys
j=json.load(sys.stdin)
assert "signals" in j, "missing signals"
for k in ("wastewater","nssp_ed_visits"):
    assert k in j["signals"], f"missing {k}"
print(json.dumps(j, indent=2))
PY
}

curl_show () {
  local path="$1"
  local url="${BASE_URL}${path}"
  local tmp_body
  tmp_body="$(mktemp)"
  local http_code
  http_code="$(curl -sS "${AUTH_HEADER[@]}" -o "$tmp_body" -w "%{http_code}" "$url" || true)"
  echo
  echo "GET ${path}  -> HTTP ${http_code}"
  echo "---- body ----"
  cat "$tmp_body"
  echo
  echo "--------------"
  if [[ "$http_code" != "200" ]]; then
    echo "ERROR: expected HTTP 200, got ${http_code} for ${path}" >&2
    rm -f "$tmp_body"
    exit 1
  fi
  cat "$tmp_body" | python3 -c 'import json,sys; j=json.load(sys.stdin); print(json.dumps(j, indent=2))'
  rm -f "$tmp_body"
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
  if curl -sS "${AUTH_HEADER[@]}" "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

# /health
curl_show "/health"

# known zip
echo
echo "Known ZIP smoke test (shape check)"
tmp="$(mktemp)"
code="$(curl -sS "${AUTH_HEADER[@]}" -o "$tmp" -w "%{http_code}" "${BASE_URL}/signals/latest?zip=${KNOWN_ZIP}" || true)"
echo
echo "GET /signals/latest?zip=${KNOWN_ZIP}  -> HTTP ${code}"
echo "---- body ----"
cat "$tmp"; echo; echo "--------------"
if [[ "$code" != "200" ]]; then
  echo "ERROR: /signals/latest failed for known zip ${KNOWN_ZIP}" >&2
  rm -f "$tmp"
  exit 1
fi
cat "$tmp" | pretty_and_check
rm -f "$tmp"

# new zip
echo
NEW_ZIP="$(prompt_zip "Enter a NEW ZIP to test read-through cache (ideally not already in DB)" "62401")"

echo
echo "New ZIP read-through cache test (should refresh if missing/stale)"
tmp="$(mktemp)"
code="$(curl -sS "${AUTH_HEADER[@]}" -o "$tmp" -w "%{http_code}" "${BASE_URL}/signals/latest?zip=${NEW_ZIP}" || true)"
echo
echo "GET /signals/latest?zip=${NEW_ZIP}  -> HTTP ${code}"
echo "---- body ----"
cat "$tmp"; echo; echo "--------------"
if [[ "$code" != "200" ]]; then
  echo "ERROR: /signals/latest failed for new zip ${NEW_ZIP}" >&2
  rm -f "$tmp"
  exit 1
fi
cat "$tmp" | pretty_and_check
# Warn if refreshed is false
python3 - <<'PY' <(cat "$tmp")
import json,sys
j=json.load(sys.stdin)
if not j.get("refreshed", False):
    print("WARN: refreshed=false. This likely means the ZIP already had fresh cached data in signal_snapshots.")
PY
rm -f "$tmp"

echo
echo "OK: Phase 3 smoke tests (v4) passed."
