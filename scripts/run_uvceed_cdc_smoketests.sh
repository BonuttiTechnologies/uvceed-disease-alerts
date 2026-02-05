#!/usr/bin/env bash
set -Eeuo pipefail

# ------------------------------------------------------------
# UVCeed CDC smoke-test runner for DigitalOcean
#
# What it does:
# - Runs each cdc_*.py script in several CLI configurations (human + json + db when supported)
# - Captures stdout/stderr, exit code, and duration per test
# - Writes a compact summary (TSV + JSONL) plus full logs
# - Bundles everything into a single tar.gz you can upload back to ChatGPT
#
# Usage:
#   chmod +x run_uvceed_cdc_smoketests.sh
#   ./run_uvceed_cdc_smoketests.sh
#
# Optional env:
#   TEST_ZIP=60614            # default ZIP used for tests
#   TEST_STATE=IL             # default state for --describe tests
#   OUTDIR=./cdc_test_runs    # base output directory
#   PYTHON=python3            # python executable
#   REPO_ROOT=/path/to/repo   # if not running from repo root
# ------------------------------------------------------------

TEST_ZIP="${TEST_ZIP:-60614}"
TEST_STATE="${TEST_STATE:-IL}"
OUTBASE="${OUTDIR:-./cdc_test_runs}"
PYTHON="${PYTHON:-python3}"

# Determine repo root (so imports like `from uvceed_alerts.geo import ...` work).
# Prefer explicit REPO_ROOT, else walk up until we see uvceed_alerts/ folder.
if [[ -n "${REPO_ROOT:-}" ]]; then
  ROOT="$REPO_ROOT"
else
  ROOT="$(pwd)"
  if [[ ! -d "$ROOT/uvceed_alerts" ]]; then
    # walk up 4 levels max
    for _ in 1 2 3 4; do
      ROOT="$(cd "$ROOT/.." && pwd)"
      [[ -d "$ROOT/uvceed_alerts" ]] && break
    done
  fi
fi

if [[ ! -d "$ROOT/uvceed_alerts" ]]; then
  echo "ERROR: Could not locate repo root containing ./uvceed_alerts"
  echo "Run this from the repo root or set REPO_ROOT=/path/to/repo"
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

{
  echo "utc_timestamp=$STAMP"
  echo "repo_root=$ROOT"
  echo "python=$($PYTHON -V 2>&1 | head -n1)"
  echo "test_zip=$TEST_ZIP"
  echo "test_state=$TEST_STATE"
  echo "DATABASE_URL_set=$([[ -n "${DATABASE_URL:-}" ]] && echo yes || echo no)"
  echo "CDC_APP_TOKEN_set=$([[ -n "${CDC_APP_TOKEN:-}" ]] && echo yes || echo no)"
} | tee "$ENV_TXT" >/dev/null

printf "test_id\tlabel\tscript\targs\texit_code\tduration_s\tstdout_bytes\tstderr_bytes\tstatus\n" > "$SUMMARY_TSV"

# Run a single command, capturing stdout/stderr to files, and write a row to the summaries.
run_case () {
  local label="$1"; shift
  local script_path="$1"; shift

  local test_id
  test_id="$(printf "%s" "$label" | tr ' /:' '___' | tr -cd '[:alnum:]_-' )"
  local out="$LOG_DIR/${test_id}.out"
  local err="$LOG_DIR/${test_id}.err"

  local start end dur exit_code
  start="$(date +%s)"

  # shellcheck disable=SC2068
  set +e
  "$PYTHON" -u "$script_path" $@ >"$out" 2>"$err"
  exit_code=$?
  set -e

  end="$(date +%s)"
  dur="$(( end - start ))"

  local so se
  so="$(wc -c <"$out" | tr -d ' ')"
  se="$(wc -c <"$err" | tr -d ' ')"

  local status="ok"
  if [[ "$exit_code" -ne 0 ]]; then
    status="fail"
  fi

  # TSV row
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$test_id" "$label" "$(basename "$script_path")" "$*" "$exit_code" "$dur" "$so" "$se" "$status" \
    >> "$SUMMARY_TSV"

  # JSONL row
  # Note: Keep JSON simple; don't include full stdout/stderr (they're in files)
  printf '{"test_id":"%s","label":"%s","script":"%s","args":"%s","exit_code":%s,"duration_s":%s,"stdout_file":"%s","stderr_file":"%s","status":"%s"}\n' \
    "$test_id" \
    "$(printf "%s" "$label" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])')" \
    "$(basename "$script_path")" \
    "$(printf "%s" "$*" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])')" \
    "$exit_code" "$dur" \
    "logs/${test_id}.out" "logs/${test_id}.err" "$status" \
    >> "$SUMMARY_JSONL"
}

# Helper: skip db tests cleanly if DATABASE_URL is not set
run_db_case () {
  local label="$1"; shift
  local script_path="$1"; shift
  if [[ -z "${DATABASE_URL:-}" ]]; then
    local test_id
    test_id="$(printf "%s" "$label" | tr ' /:' '___' | tr -cd '[:alnum:]_-' )"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$test_id" "$label" "$(basename "$script_path")" "$*" "0" "0" "0" "0" "skipped_no_DATABASE_URL" \
      >> "$SUMMARY_TSV"
    printf '{"test_id":"%s","label":"%s","script":"%s","args":"%s","exit_code":0,"duration_s":0,"stdout_file":"","stderr_file":"","status":"skipped_no_DATABASE_URL"}\n' \
      "$test_id" \
      "$(printf "%s" "$label" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])')" \
      "$(basename "$script_path")" \
      "$(printf "%s" "$*" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])')" \
      >> "$SUMMARY_JSONL"
    return 0
  fi
  run_case "$label" "$script_path" "$@"
}

# Locate scripts (prefer the canonical ones in uvceed_alerts/)
SCRIPTS_DIR="$ROOT/uvceed_alerts"

declare -A S
S["cdc_nssp_ed_visits"]="$SCRIPTS_DIR/cdc_nssp_ed_visits.py"
S["cdc_nssp_ed_trajectories"]="$SCRIPTS_DIR/cdc_nssp_ed_trajectories.py"
S["cdc_fluview_ilinet"]="$SCRIPTS_DIR/cdc_fluview_ilinet.py"
S["cdc_fluview_severity"]="$SCRIPTS_DIR/cdc_fluview_severity.py"
S["cdc_wastewater"]="$SCRIPTS_DIR/cdc_wastewater.py"

# If any are missing, try current directory (useful if you copied scripts next to this runner)
for k in "${!S[@]}"; do
  if [[ ! -f "${S[$k]}" ]]; then
    if [[ -f "./${k}.py" ]]; then
      S[$k]="./${k}.py"
    fi
  fi
done

# Validate
missing=0
for k in cdc_nssp_ed_visits cdc_nssp_ed_trajectories cdc_fluview_ilinet cdc_fluview_severity cdc_wastewater; do
  if [[ ! -f "${S[$k]}" ]]; then
    echo "WARN: Missing script: ${S[$k]}"
    missing=1
  fi
done
if [[ "$missing" -eq 1 ]]; then
  echo "ERROR: One or more scripts not found. Check repo layout or adjust SCRIPTS_DIR/REPO_ROOT."
  exit 2
fi

echo "Running smoke tests..."
echo "Output: $RUN_DIR"
echo

# ------------------------------------------------------------
# cdc_nssp_ed_visits.py
# ------------------------------------------------------------
run_case    "nssp_ed_visits human combined" "${S[cdc_nssp_ed_visits]}" "$TEST_ZIP" --pathogen combined --weeks 16
run_case    "nssp_ed_visits json combined"  "${S[cdc_nssp_ed_visits]}" "$TEST_ZIP" --pathogen combined --weeks 16 --json
run_case    "nssp_ed_visits json-only"      "${S[cdc_nssp_ed_visits]}" "$TEST_ZIP" --pathogen combined --weeks 16 --json-only
run_db_case "nssp_ed_visits db json-only"   "${S[cdc_nssp_ed_visits]}" "$TEST_ZIP" --pathogen combined --weeks 16 --db --json-only

# also test other pathogens quickly
for p in covid flu rsv; do
  run_case "nssp_ed_visits json ${p}" "${S[cdc_nssp_ed_visits]}" "$TEST_ZIP" --pathogen "$p" --weeks 16 --json-only
done

# ------------------------------------------------------------
# cdc_nssp_ed_trajectories.py
# ------------------------------------------------------------
run_case "nssp_ed_trajectories describe" "${S[cdc_nssp_ed_trajectories]}" --describe --state "$TEST_STATE"
for p in combined covid flu rsv; do
  run_case "nssp_ed_trajectories human ${p}" "${S[cdc_nssp_ed_trajectories]}" "$TEST_ZIP" --pathogen "$p" --weeks 16
  run_case "nssp_ed_trajectories json ${p}"  "${S[cdc_nssp_ed_trajectories]}" "$TEST_ZIP" --pathogen "$p" --weeks 16 --json
done

# ------------------------------------------------------------
# cdc_fluview_ilinet.py
# ------------------------------------------------------------
run_case "fluview_ilinet human" "${S[cdc_fluview_ilinet]}" "$TEST_ZIP" --weeks 12 --lookback-weeks 104
run_case "fluview_ilinet json"  "${S[cdc_fluview_ilinet]}" "$TEST_ZIP" --weeks 12 --lookback-weeks 104 --json

# ------------------------------------------------------------
# cdc_fluview_severity.py
# ------------------------------------------------------------
run_case "fluview_severity human" "${S[cdc_fluview_severity]}" "$TEST_ZIP" --weeks 104 --recent 12
run_case "fluview_severity json"  "${S[cdc_fluview_severity]}" "$TEST_ZIP" --weeks 104 --recent 12 --json

# ------------------------------------------------------------
# cdc_wastewater.py
# ------------------------------------------------------------
run_case    "wastewater human default" "${S[cdc_wastewater]}" "$TEST_ZIP"
run_case    "wastewater json default"  "${S[cdc_wastewater]}" "$TEST_ZIP" --json
run_case    "wastewater json all"      "${S[cdc_wastewater]}" "$TEST_ZIP" --all --json
run_db_case "wastewater db json"       "${S[cdc_wastewater]}" "$TEST_ZIP" --db --json
run_db_case "wastewater db all json"   "${S[cdc_wastewater]}" "$TEST_ZIP" --db --all --json

# ------------------------------------------------------------
# Package results for upload
# ------------------------------------------------------------
BUNDLE="$OUTBASE/cdc_smoketests_${STAMP}.tar.gz"
tar -C "$RUN_DIR/.." -czf "$BUNDLE" "$(basename "$RUN_DIR")"

echo
echo "Done."
echo "Bundle for upload: $BUNDLE"
echo "Key files inside bundle:"
echo "  - $(basename "$RUN_DIR")/summary.tsv"
echo "  - $(basename "$RUN_DIR")/summary.jsonl"
echo "  - $(basename "$RUN_DIR")/env.txt"
echo "  - $(basename "$RUN_DIR")/logs/*.out and *.err"
