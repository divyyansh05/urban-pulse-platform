#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Urban Pulse Platform — Phase 1 Git Push Script
# Run AFTER setup_phase1.sh: bash scripts/push_phase1.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

GREEN='\033[0;32m'; BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'
ok()     { echo -e "  ${GREEN}✅ $1${RESET}"; }
info()   { echo -e "  ${BLUE}ℹ️  $1${RESET}"; }
header() { echo -e "\n${BOLD}$1${RESET}\n$(printf '─%.0s' {1..60})"; }

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

header "Creating branch feat/phase-1-batch-ingestion"
git checkout -b feat/phase-1-batch-ingestion

header "Staging all Phase 1 files"
git add \
  ingestion/ \
  transformation/seeds/ \
  orchestration/dags/batch_ingestion_dag.py \
  tests/__init__.py \
  tests/unit/__init__.py \
  tests/unit/test_schemas.py \
  docs/decisions/ADR-002-batch-ingestion-pattern.md \
  README.md

info "Files to be committed:"
git status --short

header "Committing"
git commit -m "feat(phase-1): batch ingestion foundation

Ingesters (all 4 sources):
- NYC 311: incremental 25h lookback, Socrata pagination, 10K/page
- NYC Crime: daily 2-day lookback, NYPD complaint data
- Open-Meteo Weather: hourly for NYC + London, columnar unpack
- AirNow EPA: hourly for 3 NYC locations, multi-row per timestamp

Architecture:
- BaseIngester: retry (3x exponential backoff), run logging,
  schema evolution detection, dry_run mode for testing
- S3Writer: NDJSON to s3://raw/{source}/{year}/{month}/{day}/{run_id}.ndjson
- BigQueryLoader: streaming insert with 500-row batching
- IngestionRunLog: every run recorded to raw._ingestion_log

Validation (Pydantic):
- NYC311Raw: borough validation, location flags, PII fields preserved
- NYCCrimeRaw: law_cat_cd validation, date/time kept separate
- WeatherObservationRaw: city enum, columnar→row conversion
- AirNowObservationRaw: nested category dict parsing, AQI range check
- All models: extra='allow' for schema evolution safety

Orchestration:
- 4 Airflow DAGs: 311 (*/15min), Crime (daily 06:00), Weather (hourly :05), AirNow (hourly :10)
- Independent tasks — one failure does not block others
- Retry with exponential backoff on all tasks

Seeds:
- borough_zip_lookup.csv (NYC zip → borough mapping)
- tfl_severity_lookup.csv (TfL severity code → description)
- wmo_weather_code_lookup.csv (WMO code → weather description)
- complaint_type_sla.csv (expected resolution hours per complaint type)

Tests:
- 32 unit tests covering all schemas, S3 key generation, ingester logic
- Dry run mode tested without cloud calls
- Error isolation verified: bad records rejected, pipeline continues

Docs:
- ADR-002: batch ingestion pattern decision
- README: Phase 1 marked complete"

header "Pushing to GitHub"
git push origin feat/phase-1-batch-ingestion

header "Creating and merging PR"
gh pr create \
  --title "feat(phase-1): batch ingestion foundation" \
  --body "$(cat << 'PREOF'
## Phase 1: Batch Ingestion Foundation

### What's in this PR

**4 production-grade batch ingesters:**
- NYC 311 (incremental, 25h lookback, Socrata pagination)
- NYC Crime (daily, NYPD complaint data)
- Open-Meteo Weather (hourly, NYC + London, columnar unpack)
- AirNow EPA (hourly, 3 NYC locations)

**Architecture pattern (Extract → Validate → Stage → Load):**
- `BaseIngester` with retry, schema evolution detection, run logging
- `S3Writer` for durable NDJSON checkpoints
- `BigQueryLoader` with streaming insert batching

**Pydantic schemas for all 4 sources** — handles real-world messiness:
- Null boroughs, separate date/time fields, nested category dicts, columnar arrays

**4 Airflow DAGs** with independent schedules and error isolation

**4 seed CSVs** for reference data (borough lookup, TfL severity, WMO codes, complaint SLAs)

**32 unit tests** — all run without cloud calls (dry_run mode)

### Concepts demonstrated
Schema validation at pipeline boundary · Idempotent append-only raw layer ·
Retry with exponential backoff · Schema evolution detection ·
S3 as durable checkpoint · BigQuery streaming inserts · Airflow DAG design

Closes Phase 1
PREOF
)" \
  --base main

gh pr merge feat/phase-1-batch-ingestion \
  --squash \
  --delete-branch \
  --admin

git checkout main
git pull origin main

echo ""
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo -e "${GREEN}${BOLD}  PHASE 1 COMPLETE — Pushed to main${RESET}"
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo ""
git log --oneline -5
echo ""
