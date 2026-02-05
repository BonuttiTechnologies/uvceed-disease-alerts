#!/usr/bin/env bash
set -Eeuo pipefail

TEST_ZIP="${TEST_ZIP:-60614}"
TEST_STATE="${TEST_STATE:-IL}"
OUTBASE="${OUTDIR:-./cdc_phase2_runs}"
PYTHON="${PYTHON:-python3}"

if [[ -n "${REPO_ROOT:-}" ]]; then
  ROOT="$REPO_ROOT"
else
  ROOT="$(pwd)"
  if [[ ! -d "$ROOT/uvceed_alerts" ]]; then
    for _ in 1 2 3 4; do
      ROOT="$(cd "$ROOT/.." && pwd)"
      [[ -d "$ROOT/uvceed_alerts" ]] && break
    done
  fi
fi

if [[ ! -d "$ROOT/uvceed_alerts" ]]; then
  echo "ERROR: Could not locate repo root containing ./uvceed_alerts"
  echo "Run from repo root or set REPO_ROOT=/path/to/repo"
  exit 2
fi

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
RUN_DIR="$OUTBASE/$STAMP"
LOG_DIR="$RUN_DIR/logs"
mkdir -p "$LOG_DIR"

SUMMARY_TSV="$RUN_DIR/summary.tsv"
SUMMARY_JSONL="$RUN_DIR/summary.jsonl"
ENV_TXT="$RUN_DIR/env.txt"
DB_CHECKS="$RUN_DIR/db_checks.json"

{
  echo "utc_timestamp=$STAMP"
  echo "repo_root=$ROOT"
  echo "python=$($PYTHON -V 2>&1 | head -n1)"
  echo "test_zip=$TEST_ZIP"
  echo "test_state=$TEST_STATE"
  echo "DATABASE_URL_set=$([[ -n "${DATABASE_URL:-}" ]] && echo yes || echo no)"
  echo "CDC_APP_TOKEN_set=$([[ -n "${CDC_APP_TOKEN:-}" ]] && echo yes || echo no)"
} | tee "$ENV_TXT" >/dev/null

printf "test_id\tlabel\tmode\ttarget\targs\texit_code\tduration_s\tstdout_bytes\tstderr_bytes\tstatus\n" > "$SUMMARY_TSV"

run_case () {
  local label="$1"; shift
  local mode="$1"; shift
  local target="$1"; shift

  local test_id
  test_id="$(printf "%s" "${label}_${mode}" | tr ' /:' '___' | tr -cd '[:alnum:]_-' )"
  local out="$LOG_DIR/${test_id}.out"
  local err="$LOG_DIR/${test_id}.err"

  local start end dur exit_code
  start="$(date +%s)"

  set +e
  if [[ "$mode" == "module" ]]; then
    "$PYTHON" -u -m "$target" "$@" >"$out" 2>"$err"
  else
    "$PYTHON" -u "$target" "$@" >"$out" 2>"$err"
  fi
  exit_code=$?
  set -e

  end="$(date +%s)"
  dur="$(( end - start ))"

  local so se
  so="$(wc -c <"$out" | tr -d ' ')"
  se="$(wc -c <"$err" | tr -d ' ')"

  local status="ok"
  if [[ "$exit_code" -ne 0 ]]; then status="fail"; fi

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$test_id" "$label" "$mode" "$target" "$*" "$exit_code" "$dur" "$so" "$se" "$status" \
    >> "$SUMMARY_TSV"

  printf '{"test_id":"%s","label":"%s","mode":"%s","target":"%s","args":"%s","exit_code":%s,"duration_s":%s,"stdout_file":"%s","stderr_file":"%s","status":"%s"}\n' \
    "$test_id" "$label" "$mode" "$target" "$*" \
    "$exit_code" "$dur" \
    "logs/${test_id}.out" "logs/${test_id}.err" "$status" \
    >> "$SUMMARY_JSONL"
}

run_db_case () {
  local label="$1"; shift
  local mode="$1"; shift
  local target="$1"; shift
  if [[ -z "${DATABASE_URL:-}" ]]; then
    run_case "${label} (skipped_no_DATABASE_URL)" "$mode" "$target" "$@"
    return 0
  fi
  run_case "$label" "$mode" "$target" "$@"
}

SCRIPTS_DIR="$ROOT/uvceed_alerts"

FILE_WW="$SCRIPTS_DIR/cdc_wastewater.py"
FILE_VIS="$SCRIPTS_DIR/cdc_nssp_ed_visits.py"
FILE_TRAJ="$SCRIPTS_DIR/cdc_nssp_ed_trajectories.py"

MOD_WW="uvceed_alerts.cdc_wastewater"
MOD_VIS="uvceed_alerts.cdc_nssp_ed_visits"
MOD_TRAJ="uvceed_alerts.cdc_nssp_ed_trajectories"

echo "Running Phase 2 DB consistency tests..."
echo "Output: $RUN_DIR"
echo

run_case    "wastewater json (no db)" file   "$FILE_WW"   "$TEST_ZIP" --json
run_db_case "wastewater json (db)"    file   "$FILE_WW"   "$TEST_ZIP" --db --json
run_case    "wastewater json (no db)" module "$MOD_WW"    "$TEST_ZIP" --json
run_db_case "wastewater json (db)"    module "$MOD_WW"    "$TEST_ZIP" --db --json

for p in combined covid flu rsv; do
  run_case    "nssp_ed_visits ${p} json (no db)" file   "$FILE_VIS" "$TEST_ZIP" --pathogen "$p" --weeks 16 --json-only
  run_db_case "nssp_ed_visits ${p} json (db)"    file   "$FILE_VIS" "$TEST_ZIP" --pathogen "$p" --weeks 16 --db --json-only
  run_case    "nssp_ed_visits ${p} json (no db)" module "$MOD_VIS"  "$TEST_ZIP" --pathogen "$p" --weeks 16 --json-only
  run_db_case "nssp_ed_visits ${p} json (db)"    module "$MOD_VIS"  "$TEST_ZIP" --pathogen "$p" --weeks 16 --db --json-only
done

run_case    "nssp_ed_trajectories describe" file   "$FILE_TRAJ" --describe --state "$TEST_STATE"
run_case    "nssp_ed_trajectories describe" module "$MOD_TRAJ"  --describe --state "$TEST_STATE"
for p in combined covid flu rsv; do
  run_case    "nssp_ed_trajectories ${p} json (no db)" file   "$FILE_TRAJ" "$TEST_ZIP" --pathogen "$p" --weeks 16 --json
  run_db_case "nssp_ed_trajectories ${p} json (db)"    file   "$FILE_TRAJ" "$TEST_ZIP" --pathogen "$p" --weeks 16 --db --json
  run_case    "nssp_ed_trajectories ${p} json (no db)" module "$MOD_TRAJ"  "$TEST_ZIP" --pathogen "$p" --weeks 16 --json
  run_db_case "nssp_ed_trajectories ${p} json (db)"    module "$MOD_TRAJ"  "$TEST_ZIP" --pathogen "$p" --weeks 16 --db --json
done

if [[ -n "${DATABASE_URL:-}" ]]; then
  TEST_ZIP="$TEST_ZIP" "$PYTHON" - <<'PY' > "$DB_CHECKS"
import os, json
import psycopg2
from psycopg2.extras import RealDictCursor

zip_code = os.environ.get("TEST_ZIP", "60614")
url = os.environ["DATABASE_URL"]

q_index = (
    "select indexname, indexdef from pg_indexes "
    "where tablename='signal_snapshots' "
    "and indexname='idx_signal_snapshots_zip_type_time'"
)
q_counts = (
    "select signal_type, count(*) as n "
    "from signal_snapshots where zip_code=%s "
    "group by signal_type order by signal_type"
)
q_latest = (
    "select distinct on (signal_type) "
    "signal_type, generated_at, risk_level, trend, confidence "
    "from signal_snapshots where zip_code=%s "
    "order by signal_type, generated_at desc"
)

out = {"zip_code": zip_code, "checks": {}}

with psycopg2.connect(url, cursor_factory=RealDictCursor) as conn:
    with conn.cursor() as cur:
        cur.execute(q_index)
        out["checks"]["index"] = cur.fetchone()
        cur.execute(q_counts, (zip_code,))
        out["checks"]["counts_by_type"] = cur.fetchall()
        cur.execute(q_latest, (zip_code,))
        out["checks"]["latest_by_type"] = cur.fetchall()

print(json.dumps(out, indent=2, default=str))
PY
fi

BUNDLE="$OUTBASE/phase2_smoketests_${STAMP}.tar.gz"
tar -C "$RUN_DIR/.." -czf "$BUNDLE" "$(basename "$RUN_DIR")"

echo
echo "Done."
echo "Bundle for upload: $BUNDLE"
