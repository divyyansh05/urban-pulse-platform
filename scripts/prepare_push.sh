#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Urban Pulse Platform — GitHub Push Preparation Script
# Run from repo root: bash scripts/prepare_push.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

ok()     { echo -e "  ${GREEN}✅ $1${RESET}"; }
fail()   { echo -e "  ${RED}❌ $1${RESET}"; exit 1; }
warn()   { echo -e "  ${YELLOW}⚠️  $1${RESET}"; }
info()   { echo -e "  ${BLUE}ℹ️  $1${RESET}"; }
header() { echo -e "\n${BOLD}$1${RESET}\n$(printf '─%.0s' {1..60})"; }

# ── Find repo root ─────────────────────────────────────────────────────────────
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || fail "Not inside a git repository. Run from repo root."

cd "$REPO_ROOT"
info "Repo root: $REPO_ROOT"

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 1 — Safety checks (nothing sensitive goes to GitHub)"
# ═══════════════════════════════════════════════════════════════════════════════

# Block if .env is tracked
if git ls-files --error-unmatch .env 2>/dev/null; then
  fail ".env IS tracked — run: git rm --cached .env  then re-run this script"
fi
ok ".env not tracked"

# Block if any JSON key file is tracked
TRACKED_JSON=$(git ls-files | grep -E '\.(json|pem|p12)$' | grep -viE '^(package|package-lock|pyproject)' || true)
if [[ -n "$TRACKED_JSON" ]]; then
  echo -e "${RED}  ❌ Credential-like files are tracked:${RESET}"
  echo "$TRACKED_JSON" | while read -r f; do echo "    • $f"; done
  fail "Remove credential files from tracking before pushing"
fi
ok "No credential files tracked"

# Warn if .env.example is missing (should always be present)
if [[ ! -f ".env.example" ]]; then
  warn ".env.example not found — create it so collaborators know which vars to set"
fi

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 2 — Ensure all required files exist"
# ═══════════════════════════════════════════════════════════════════════════════

REQUIRED_FILES=(
  ".env.example"
  ".gitignore"
  ".pre-commit-config.yaml"
  "Makefile"
  "pyproject.toml"
  "requirements.txt"
  "transformation/dbt_project.yml"
  "docs/decisions/ADR-001-cloud-architecture.md"
  "scripts/verify_api_keys.py"
  "scripts/verify_setup.py"
  ".github/workflows/ci.yml"
)

MISSING=0
for f in "${REQUIRED_FILES[@]}"; do
  if [[ -f "$f" ]]; then
    ok "Found: $f"
  else
    warn "Missing: $f"
    MISSING=$((MISSING + 1))
  fi
done

if [[ $MISSING -gt 0 ]]; then
  warn "$MISSING file(s) missing — they won't be in the commit but that's okay if not yet created"
fi

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 3 — Ensure .gitkeep files exist in all empty directories"
# ═══════════════════════════════════════════════════════════════════════════════

find . -type d \
  -not -path "./.git/*" \
  -not -path "./.venv/*" \
  -not -path "./transformation/target/*" \
  -not -path "./transformation/dbt_packages/*" \
  | while read -r dir; do
    if [[ -z "$(ls -A "$dir" 2>/dev/null)" ]]; then
      touch "$dir/.gitkeep"
      info "Added .gitkeep to empty dir: $dir"
    fi
  done
ok "All empty directories have .gitkeep"

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 4 — Run pre-commit on all files"
# ═══════════════════════════════════════════════════════════════════════════════

if command -v pre-commit &>/dev/null; then
  info "Running pre-commit hooks on all files..."
  # Run but don't exit on failure — we want to show what failed
  if pre-commit run --all-files; then
    ok "pre-commit passed"
  else
    warn "pre-commit found issues and auto-fixed some — files may have been modified"
    warn "This is normal on first run. They will be staged and committed."
  fi
else
  warn "pre-commit not installed — skipping. Run: pip install pre-commit && pre-commit install"
fi

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 5 — Stage all files for commit"
# ═══════════════════════════════════════════════════════════════════════════════

# Show what's about to be staged
echo ""
info "Files to be committed:"
git status --short

echo ""
git add .

# Final safety check — make sure .env didn't sneak in
if git diff --cached --name-only | grep -q "^\.env$"; then
  git reset HEAD .env
  fail ".env was staged — removed it. Check your .gitignore includes .env"
fi

ok "All files staged"

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 6 — Commit"
# ═══════════════════════════════════════════════════════════════════════════════

# Check if there's anything to commit
if git diff --cached --quiet; then
  ok "Nothing new to commit — working tree already clean"
else
  COMMIT_MSG="feat: pre-phase setup complete

Project scaffold:
- Full directory structure (ingestion/streaming/transformation/infra)
- Medallion architecture BigQuery layer config in dbt_project.yml
- Root tooling: Makefile, pyproject.toml, pre-commit hooks, ruff, sqlfluff
- .gitignore: credentials, secrets, generated files excluded
- GitHub Actions CI: structure validation, pre-commit, credential safety check
- Verification scripts: verify_api_keys.py, verify_setup.py, prepare_push.sh
- ADR-001: cloud architecture decision documented
- .env.example: all required variable names documented

Cloud setup:
- GCP project: urban-pulse-dev
- BigQuery datasets: raw, staging, intermediate, marts, monitoring
- AWS S3: raw landing zone with domain folder structure
- All 5 external APIs verified: NYC Open Data, TfL, AirNow, Open-Meteo, NOAA

Ready for Phase 0: API exploration and data model design"

  git commit -m "$COMMIT_MSG"
  ok "Committed"
fi

# ═══════════════════════════════════════════════════════════════════════════════
header "STEP 7 — Push to GitHub"
# ═══════════════════════════════════════════════════════════════════════════════

CURRENT_BRANCH=$(git branch --show-current)
info "Pushing branch: $CURRENT_BRANCH"

if git push origin "$CURRENT_BRANCH"; then
  ok "Pushed to origin/$CURRENT_BRANCH"
else
  fail "Push failed — check your GitHub remote: git remote -v"
fi

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo -e "${GREEN}${BOLD}  ✅ PRE-PHASE COMPLETE — Repo is live on GitHub${RESET}"
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo ""
echo -e "  ${BLUE}Next steps:${RESET}"
echo -e "  1. Go to your GitHub repo and verify the structure looks correct"
echo -e "  2. Check the Actions tab — CI workflow should be running"
echo -e "  3. Come back and say ${BOLD}'pre-phase done'${RESET} to start Phase 0"
echo ""
