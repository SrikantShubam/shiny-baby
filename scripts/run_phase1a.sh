# #!/usr/bin/env bash
# # Phoenix Phase-1A runner with project-structure and sanity checks
# # Usage:
# #   ./run_phase1a.sh --input legacy.json --testdir ./test_dir [--venv ./phoenix_env] [--outdir ./out]
# #
# # Notes:
# # - Prefers to find preproc_v6_2.py and validate_phoenix_v6_2.py inside --testdir.
# #   Falls back to the script directory, then /mnt/data.
# # - Performs schema sanity checks on input JSON before running.
# # - Emits a brief operational summary at the end.

# set -euo pipefail

# # -------- Pretty printing --------
# RED=$'\e[31m'; GRN=$'\e[32m'; YEL=$'\e[33m'; BLU=$'\e[34m'; DIM=$'\e[2m'; RST=$'\e[0m'
# die(){ echo "${RED}✖${RST} $*" >&2; exit 1; }
# ok(){ echo "${GRN}✔${RST} $*"; }
# warn(){ echo "${YEL}!${RST} $*"; }
# info(){ echo "${BLU}›${RST} $*"; }

# # -------- Locate script & defaults --------
# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PREPROC_DEFAULT="$SCRIPT_DIR/preproc_v6_2.py"
# VALIDATOR_DEFAULT="$SCRIPT_DIR/validate_phoenix_v6_2.py"
# VENV_DEFAULT="$SCRIPT_DIR/phoenix_env"
# OUTDIR_DEFAULT="$SCRIPT_DIR/out"
# TESTDIR_DEFAULT="$SCRIPT_DIR/test_dir"

# INPUT=""
# VENV_DIR="$VENV_DEFAULT"
# OUTDIR="$OUTDIR_DEFAULT"
# TEST_DIR="$TESTDIR_DEFAULT"
# PYTHON_BIN="python3"

# # -------- Parse args --------
# while [[ $# -gt 0 ]]; do
#   case "$1" in
#     --input) INPUT="${2:-}"; shift 2;;
#     --venv) VENV_DIR="${2:-}"; shift 2;;
#     --outdir) OUTDIR="${2:-}"; shift 2;;
#     --testdir) TEST_DIR="${2:-}"; shift 2;;
#     --python) PYTHON_BIN="${2:-}"; shift 2;;
#     -h|--help)
#       cat << USAGE
# Phoenix Phase-1A Runner
# Usage:
#   $0 --input <legacy.json|basename.json> [--venv <dir>] [--outdir <dir>] [--testdir <dir>] [--python <python_bin>]

# Options:
#   --input    Path or basename of legacy JSON file (required)
#   --venv     Virtualenv directory (default: $VENV_DEFAULT)
#   --outdir   Output directory (default: $OUTDIR_DEFAULT)
#   --testdir  Directory to search for inputs & runner files (default: $TESTDIR_DEFAULT)
#   --python   Python interpreter (default: python3)
# USAGE
#       exit 0;;
#     *) die "Unknown argument: $1";;
#   esac
# done

# [[ -n "$INPUT" ]] || die "Missing --input <legacy.json>"

# # -------- Resolve & check test dir --------
# [[ -d "$TEST_DIR" ]] || die "Test directory not found: $TEST_DIR"

# # -------- Resolve input file (search inside TEST_DIR if needed) --------
# if [[ ! -f "$INPUT" ]]; then
#   CAND="$TEST_DIR/$(basename "$INPUT")"
#   if [[ -f "$CAND" ]]; then
#     INPUT="$CAND"
#   else
#     HIT="$(find "$TEST_DIR" -type f -name "$(basename "$INPUT")" -print -quit || true)"
#     if [[ -n "${HIT:-}" && -f "$HIT" ]]; then
#       INPUT="$HIT"
#     else
#       die "Input not found: $INPUT (also searched under $TEST_DIR)"
#     fi
#   fi
# fi

# # -------- Check input file --------
# [[ -f "$INPUT" ]] || die "Input file not found after search: $INPUT"
# [[ -s "$INPUT" ]] || die "Input file is empty: $INPUT"

# # -------- Ensure runner files exist (preproc & validator) --------
# PREPROC="$PREPROC_DEFAULT"
# VALIDATOR="$VALIDATOR_DEFAULT"

# # Prefer files from TEST_DIR
# if [[ ! -f "$PREPROC" ]]; then
#   if [[ -f "$TEST_DIR/preproc_v6_2.py" ]]; then
#     PREPROC="$TEST_DIR/preproc_v6_2.py"
#     ok "Using preproc_v6_2.py from $TEST_DIR"
#   else
#     HIT="$(find "$TEST_DIR" -type f -name "preproc_v6_2.py" -print -quit || true)"
#     if [[ -n "${HIT:-}" && -f "$HIT" ]]; then PREPROC="$HIT"; ok "Using preproc_v6_2.py from $HIT"; fi
#   fi
# fi
# if [[ ! -f "$PREPROC" ]]; then
#   if [[ -f "/mnt/data/preproc_v6_2.py" ]]; then
#     cp -f "/mnt/data/preproc_v6_2.py" "$PREPROC_DEFAULT"
#     PREPROC="$PREPROC_DEFAULT"
#     ok "Materialized preproc_v6_2.py from /mnt/data"
#   else
#     die "preproc_v6_2.py not found in $TEST_DIR, $SCRIPT_DIR or /mnt/data"
#   fi
# fi

# if [[ ! -f "$VALIDATOR" ]]; then
#   if [[ -f "$TEST_DIR/validate_phoenix_v6_2.py" ]]; then
#     VALIDATOR="$TEST_DIR/validate_phoenix_v6_2.py"
#     ok "Using validate_phoenix_v6_2.py from $TEST_DIR"
#   else
#     HIT="$(find "$TEST_DIR" -type f -name "validate_phoenix_v6_2.py" -print -quit || true)"
#     if [[ -n "${HIT:-}" && -f "$HIT" ]]; then VALIDATOR="$HIT"; ok "Using validate_phoenix_v6_2.py from $HIT"; fi
#   fi
# fi
# if [[ ! -f "$VALIDATOR" ]]; then
#   if [[ -f "/mnt/data/validate_phoenix_v6_2.py" ]]; then
#     cp -f "/mnt/data/validate_phoenix_v6_2.py" "$VALIDATOR_DEFAULT"
#     VALIDATOR="$VALIDATOR_DEFAULT"
#     ok "Materialized validate_phoenix_v6_2.py from /mnt/data"
#   else
#     die "validate_phoenix_v6_2.py not found in $TEST_DIR, $SCRIPT_DIR or /mnt/data"
#   fi
# fi

# # -------- Prepare project structure --------
# mkdir -p "$OUTDIR" || true
# mkdir -p "$OUTDIR/logs" "$OUTDIR/preproc" "$OUTDIR/review" "$OUTDIR/patterns" || true
# PATTERNS_FILE="$OUTDIR/patterns/patterns.jsonl"
# [[ -f "$PATTERNS_FILE" ]] || { touch "$PATTERNS_FILE"; ok "Created empty patterns store: $PATTERNS_FILE"; }

# # -------- Optional venv (no heavy deps required for Phase-1A) --------
# if [[ ! -d "$VENV_DIR" ]]; then
#   info "Creating clean virtualenv at $VENV_DIR"
#   "$PYTHON_BIN" -m venv "$VENV_DIR" || die "Failed to create venv"
# fi
# # shellcheck source=/dev/null
# source "$VENV_DIR/bin/activate" || die "Failed to activate venv"

# # Minimal health checks
# python --version || die "Python not working in venv"
# [[ -x "$VENV_DIR/bin/python" ]] || die "Unexpected: venv python missing"

# # -------- Schema sanity check on input JSON --------
# python - "$INPUT" << 'PY' || exit 1
# import json, sys
# path=sys.argv[1]
# with open(path,'r',encoding='utf-8') as f:
#     try:
#         data=json.load(f)
#     except Exception as e:
#         print(f"JSON parse error: {e}", file=sys.stderr); sys.exit(2)

# if not isinstance(data, dict) or not data:
#     print("Schema error: top-level must be a non-empty object", file=sys.stderr); sys.exit(3)
# key=next(iter(data.keys()))
# meta=data[key]
# if not isinstance(meta, dict) or "tables" not in meta:
#     print("Schema error: missing 'tables' under top-level key", file=sys.stderr); sys.exit(4)
# tabs=meta.get("tables", [])
# if not isinstance(tabs, list):
#     print("Schema error: 'tables' must be a list", file=sys.stderr); sys.exit(5)
# if len(tabs)==0:
#     print("Warning: tables list is empty", file=sys.stderr)
# print(f"OK schema: {len(tabs)} tables under key '{key}'")
# PY

# ok "Input JSON passed schema sanity checks"

# # -------- Run validation (binary PASS/FAIL) --------
# # Pass the resolved PREPROC path into the validator to avoid hardcoded locations.
# VAL_JSON="$(python "$VALIDATOR" --input "$INPUT" --preproc "$PREPROC")" || { echo "$VAL_JSON"; die "Validator execution failed"; }

# # Parse validator JSON safely via Python
# python - "$VAL_JSON" << 'PY' || exit 1
# import json, sys
# raw=sys.argv[1]
# try:
#   obj=json.loads(raw)
# except Exception as e:
#   print("Validator did not return JSON:", raw[:4000], file=sys.stderr); sys.exit(10)

# print(json.dumps(obj, ensure_ascii=False))
# if not obj.get("PASS", False):
#   print("Validator reported FAIL; reasons:", obj.get("reasons"), file=sys.stderr); sys.exit(11)
# PY
# ok "Validation PASS"

# # -------- Run preprocessor to produce final normalized output --------
# BASENAME="$(basename "$INPUT")"
# OUTFILE="$OUTDIR/preproc/${BASENAME%.json}.preproc_v6_2.json"

# python "$PREPROC" --input "$INPUT" --output "$OUTFILE" > "$OUTDIR/logs/run.log" 2>&1 || {
#   tail -n 200 "$OUTDIR/logs/run.log" >&2
#   die "Preprocessor failed (see logs)"
# }
# [[ -f "$OUTFILE" ]] || die "Preprocessor did not write expected output: $OUTFILE"

# # -------- Post-run sanity & summary --------
# python - "$OUTFILE" << 'PY' || exit 1
# import json, sys
# path=sys.argv[1]
# with open(path,'r',encoding='utf-8') as f:
#     res=json.load(f)
# proc=res.get("processed_count",0); skip=res.get("skipped_count",0)
# if proc==0:
#     print("ERROR: 0 tables processed; investigate triage thresholds or input", file=sys.stderr); sys.exit(20)
# # brief metrics
# pages=set(t.get("page_number") for t in res.get("processed",[]))
# print(json.dumps({
#   "output": path,
#   "processed": proc,
#   "skipped": skip,
#   "pages_with_tables": sorted([p for p in pages if p is not None])[:20]
# }, ensure_ascii=False))
# PY

# ok "Phase-1A run complete"
# echo
# info "Artifacts:"
# echo "  Output JSON : $OUTFILE"
# echo "  Log         : $OUTDIR/logs/run.log"
# echo "  Patterns    : $PATTERNS_FILE"
# echo
# ok "Phoenix Phase-1A is operational."






















#!/usr/bin/env bash
# Phoenix Phase-1A unified runner (single or batch) with project-structure checks
# Examples:
#   scripts/run_phase1a.sh --input data/sah.json --svr
#   scripts/run_phase1a.sh --indir data --svr --rollup
#   scripts/run_phase1a.sh --inputs "data/sah.json data/motisons.json" --svr
set -euo pipefail

# ---------- pretty ----------
RED=$'\e[31m'; GRN=$'\e[32m'; YEL=$'\e[33m'; BLU=$'\e[34m'; RST=$'\e[0m'
die(){ echo "${RED}✖${RST} $*" >&2; exit 1; }
ok(){  echo "${GRN}✔${RST} $*"; }
warn(){ echo "${YEL}!${RST} $*"; }
info(){ echo "${BLU}›${RST} $*"; }

# ---------- paths & defaults ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VENV_DIR="$PROJECT_ROOT/phoenix_env"
OUTDIR="$PROJECT_ROOT/out"
PYTHON_BIN="python3"

# tools (we will resolve these robustly below)
PREPROC=""
VALIDATOR=""
AUDIT=""

# inputs (choose one of: --input, multiple --input, --inputs "a b", or --indir dir)
declare -a INPUTS=()
INDIR=""
DO_SVR=false
DO_ROLLUP=false

usage() {
  cat <<USAGE
Phoenix Phase-1A Runner (single or batch)

Usage:
  $0 [--input <file.json>]... [--inputs "<f1 f2 ...>"] [--indir <dir>]
     [--svr] [--rollup] [--venv <dir>] [--outdir <dir>] [--python <bin>]

Options:
  --input     Add a single input file (can be passed multiple times)
  --inputs    Quoted space-separated list of files
  --indir     Directory to scan for *.json and *.json.gz
  --svr       Also generate SVR for each processed file
  --rollup    After SVRs, produce cross-dossier roll-up
  --venv      Virtualenv directory (default: $VENV_DIR)
  --outdir    Output directory (default: $OUTDIR)
  --python    Python interpreter (default: python3)

Notes:
- Uses canonical tools from phoenix/: surgeon, validator, audit.
- Writes preproc outputs under out/preproc/, SVRs under out/svr/<name>/
USAGE
}

# ---------- arg parse ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)   INPUTS+=("$2"); shift 2;;
    --inputs)  read -r -a TMP <<<"$2"; INPUTS+=("${TMP[@]}"); shift 2;;
    --indir)   INDIR="$2"; shift 2;;
    --svr)     DO_SVR=true; shift;;
    --rollup)  DO_ROLLUP=true; shift;;
    --venv)    VENV_DIR="$2"; shift 2;;
    --outdir)  OUTDIR="$2"; shift 2;;
    --python)  PYTHON_BIN="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) die "Unknown arg: $1 (try --help)";;
  esac
done

# ---------- collect inputs if --indir given ----------
if [[ -n "$INDIR" ]]; then
  [[ -d "$INDIR" ]] || die "--indir not a directory: $INDIR"
  mapfile -t DIRFILES < <(find "$INDIR" -maxdepth 1 -type f \( -name '*.json' -o -name '*.json.gz' \) | sort)
  if [[ ${#DIRFILES[@]} -eq 0 ]]; then die "No *.json or *.json.gz in $INDIR"; fi
  INPUTS+=("${DIRFILES[@]}")
fi

[[ ${#INPUTS[@]} -gt 0 ]] || die "No inputs provided. Use --input, --inputs, or --indir."

# ---------- resolve tool paths (prefer canonical phoenix/*) ----------
try_pick() {
  local -a CANDIDATES=("$@")
  for p in "${CANDIDATES[@]}"; do
    if [[ -f "$p" ]]; then echo "$p"; return 0; fi
  done
  return 1
}

PREPROC="$(try_pick \
  "$PROJECT_ROOT/phoenix/surgeon/preproc_v6_2.py" \
  "$SCRIPT_DIR/preproc_v6_2.py" \
  "/mnt/data/preproc_v6_2.py" )" || die "preproc_v6_2.py not found"
VALIDATOR="$(try_pick \
  "$PROJECT_ROOT/phoenix/validator/validate_phoenix_v6_2.py" \
  "$SCRIPT_DIR/validate_phoenix_v6_2.py" \
  "/mnt/data/validate_phoenix_v6_2.py" )" || die "validate_phoenix_v6_2.py not found"
AUDIT="$(try_pick \
  "$PROJECT_ROOT/phoenix/audit/blind_faith_svr.py" \
  "$SCRIPT_DIR/blind_faith_svr.py" \
  "/mnt/data/blind_faith_svr.py" )" || { $DO_SVR && die "audit script not found"; }

ok "Using PREPROC   : $PREPROC"
ok "Using VALIDATOR : $VALIDATOR"
$DO_SVR && ok "Using AUDIT    : $AUDIT"

# ---------- prepare dirs & venv ----------
mkdir -p "$OUTDIR/logs" "$OUTDIR/preproc" "$OUTDIR/review" "$OUTDIR/patterns" "$OUTDIR/svr" || true
PATTERNS_FILE="$OUTDIR/patterns/patterns.jsonl"
[[ -f "$PATTERNS_FILE" ]] || { touch "$PATTERNS_FILE"; ok "Created: $PATTERNS_FILE"; }

mkdir -p "$OUTDIR/review" "$OUTDIR/patterns" || true
[[ -f "$OUTDIR/review/learning_events.jsonl" ]] || touch "$OUTDIR/review/learning_events.jsonl"


if [[ ! -d "$VENV_DIR" ]]; then
  info "Creating virtualenv: $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR" || die "venv create failed"
fi
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate" || die "venv activate failed"
python --version || die "Python not working in venv"

# ---------- helpers ----------
strip_name() {
  # strip .json or .json.gz from basename
  local b n
  b="$(basename "$1")"
  n="${b%.json}"
  [[ "$b" == "$n" ]] && n="${b%.json.gz}"
  echo "$n"
}

schema_check() {
  python - "$1" << 'PY'
import json,sys
p=sys.argv[1]
with open(p,'r',encoding='utf-8') as f:
  try:
    d=json.load(f)
  except Exception as e:
    print(f"JSON parse error: {e}", file=sys.stderr); sys.exit(2)
if not isinstance(d,dict) or not d: print("Schema error: top-level must be non-empty object", file=sys.stderr); sys.exit(3)
k=next(iter(d))
meta=d[k]
if not isinstance(meta,dict) or "tables" not in meta: print("Schema error: missing 'tables' under top-level key", file=sys.stderr); sys.exit(4)
tabs=meta.get("tables",[])
if not isinstance(tabs,list): print("Schema error: 'tables' must be a list", file=sys.stderr); sys.exit(5)
print(f"OK schema: {len(tabs)} tables under key '{k}'")
PY
}

# ---------- run each input ----------
declare -a SVR_PATHS=()
for F in "${INPUTS[@]}"; do
  [[ -f "$F" ]] || die "Input not found: $F"
  [[ -s "$F" ]] || die "Input is empty: $F"

  info "Processing: $F"
  schema_check "$F" >/dev/null || die "Schema check failed for $F"
  ok "Schema ✓"

  # validator (parametrize preproc path to avoid hardcoded locations)
 VAL_JSON="$(python "$VALIDATOR" --input "$F" --preproc "$PREPROC")" || { echo "$VAL_JSON"; warn "Validator execution error for $F — skipping"; continue; }

if ! python -c 'import json,sys;o=json.loads(sys.stdin.read());print(json.dumps(o,ensure_ascii=False));exit(0 if o.get("PASS") else 11)' <<<"$VAL_JSON"; then
  REASONS="$(python - <<'PY' "$VAL_JSON"
import json,sys;o=json.loads(sys.argv[1]); print(",".join(o.get("reasons",[])))
PY
  )"
  warn "Validator FAIL for $F — skipping (reasons: $REASONS)"
  continue
fi


  ok "Validation ✓"

  NAME="$(strip_name "$F")"
  OUT_PRE="$OUTDIR/preproc/${NAME}.preproc_v6_2.json"

  # run preprocessor
  python "$PREPROC" --input "$F" --output "$OUT_PRE" > "$OUTDIR/logs/run.log" 2>&1 || {
    tail -n 200 "$OUTDIR/logs/run.log" >&2
    die "Preprocessor failed for $F (see logs)"
  }
  [[ -f "$OUT_PRE" ]] || die "Preprocessor did not write: $OUT_PRE"
  ok "Preproc → $OUT_PRE"

  # sanity on output
  python - "$OUT_PRE" << 'PY' || exit 1
import json,sys
p=sys.argv[1]; r=json.load(open(p,'r',encoding='utf-8'))
proc=r.get('processed_count',0); skip=r.get('skipped_count',0)
if proc==0: print("ERROR: 0 tables processed", file=sys.stderr); sys.exit(20)
print(f'Processed={proc} Skipped={skip}')
PY

  # optional SVR
  if $DO_SVR; then
    OUT_SVR_DIR="$OUTDIR/svr/$NAME"
    mkdir -p "$OUT_SVR_DIR"
    python "$AUDIT" --input "$OUT_PRE" --outdir "$OUT_SVR_DIR" >/dev/null
    [[ -f "$OUT_SVR_DIR/SVR_blind_faith.json" ]] || die "SVR not created for $F"
    SVR_PATHS+=("$OUT_SVR_DIR/SVR_blind_faith.json")
    ok "SVR → $OUT_SVR_DIR/SVR_blind_faith.json"
  fi

done

# ---------- optional roll-up ----------
if $DO_ROLLUP; then
  if [[ ${#SVR_PATHS[@]} -eq 0 ]]; then
    warn "No SVRs to roll up (skipping --rollup)"; exit 0
  fi
  if [[ -f "$SCRIPT_DIR/cross_dossier_rollup.py" ]]; then
    python "$SCRIPT_DIR/cross_dossier_rollup.py" --inputs "${SVR_PATHS[@]}" --out "$OUTDIR/svr/rollup.json"
  else
    # fallback inline rollup
    python - "${SVR_PATHS[@]}" "$OUTDIR/svr/rollup.json" << 'PY'
import json,sys,statistics,pathlib
svrs=[json.load(open(p,'r',encoding='utf-8')) for p in sys.argv[1:-1]]
out=sys.argv[-1]
comp=[s.get('composite_score',0.0) for s in svrs]
doc=[s.get('dossier_source','unknown') for s in svrs]
roll={
  "docs": doc,
  "composite_avg": round(statistics.mean(comp),4) if comp else 0.0,
  "target_composite": 0.78,
  "meets_target": (statistics.mean(comp)>=0.78) if comp else False
}
pathlib.Path(out).write_text(json.dumps(roll,indent=2),encoding='utf-8')
print(f"rollup → {out}")
PY
  fi
  ok "Roll-up complete → $OUTDIR/svr/rollup.json"
fi

ok "Phase-1A run complete."
echo
echo "Artifacts:"
echo "  Out dir     : $OUTDIR"
echo "  Logs        : $OUTDIR/logs/run.log"
$DO_SVR && echo "  SVRs        : ${SVR_PATHS[*]:-none}"












#usage

# A) Full batch over data/ (keeps going if a file fails)
# chmod +x scripts/run_phase1a.sh

# scripts/run_phase1a.sh \
#   --indir data \
#   --svr --rollup \
#   --venv ./phoenix_env \
#   --outdir ./out

# B) Single file (e.g., Sah)
# scripts/run_phase1a.sh \
#   --input data/sah.json \
#   --svr \
#   --venv ./phoenix_env \
#   --outdir ./out



# C) Peek results after a run
# # What passed validator
# for d in out/svr/*; do
#   [ -f "$d/SVR_blind_faith.json" ] && jq -r '.dossier_source+" => composite=" + (.composite_score|tostring)' "$d/SVR_blind_faith.json"
# done

# # Rollup (if any SVRs were created)
# [ -f out/svr/rollup.json ] && cat out/svr/rollup.json

# # Learning events getting written
# tail -n 20 out/review/learning_events.jsonl || true