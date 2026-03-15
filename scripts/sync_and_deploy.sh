#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

# Vercel reliability controls
COOLDOWN_FILE="$LOG_DIR/vercel_cooldown_until_epoch.txt"
COOLDOWN_REASON_FILE="$LOG_DIR/vercel_cooldown_reason.txt"
# 26h is intentionally >24h to be safe across reset boundaries.
VERCEL_CAP_COOLDOWN_SECS="${VERCEL_CAP_COOLDOWN_SECS:-93600}"
SITE_URL="${HEALTHBOARD_SITE_URL:-https://gaginonricky.com}"
export HEALTHBOARD_SITE_URL="$SITE_URL"

cd "$REPO_DIR"

# Ensure commits/deploy metadata use a Vercel team-authorized identity.
export GIT_AUTHOR_NAME="Jean Eric Gagnon"
export GIT_AUTHOR_EMAIL="jean.eric.gagnon619@gmail.com"
export GIT_COMMITTER_NAME="Jean Eric Gagnon"
export GIT_COMMITTER_EMAIL="jean.eric.gagnon619@gmail.com"

# Single-run lock: prevent overlapping cron/manual deploys.
LOCK_FILE="$LOG_DIR/deploy.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "INFO: deploy lock busy; skipping overlapping run"
  exit 0
fi

# Optional pull step (disabled by default to avoid duplicate pull/deploy cycles).
# Enable explicitly when needed:
#   RUN_KPI_PULL_FIRST=1 bash health-board/scripts/sync_and_deploy.sh
if [ "${RUN_KPI_PULL_FIRST:-0}" = "1" ]; then
  "$REPO_DIR/../ads-ops/scripts/run_kpi_pull.sh" >> "$LOG_DIR/full-kpi-pull.log" 2>&1 || true
else
  echo "INFO: skipping embedded KPI pull (RUN_KPI_PULL_FIRST!=1)"
fi

# Build website snapshot from latest exports
python3 "$REPO_DIR/scripts/pull_kpis.py"

# Pull creative metadata for concept-level analysis (best effort)
python3 "$REPO_DIR/scripts/fetch_creative_metadata.py" >> "$LOG_DIR/creative-meta.log" 2>&1 || true

# Run analyzer (Phase 1 autopilot foundation)
python3 "$REPO_DIR/scripts/analyze_kpis.py" >> "$LOG_DIR/analyze-kpis.log" 2>&1 || true

# Bring in expanded ads-ops payload (geo/age/device/placement breakdowns) when available.
ADSOPS_LATEST="$REPO_DIR/../ads-ops/dashboard/data/latest.json"
if [ -f "$ADSOPS_LATEST" ]; then
  cp "$ADSOPS_LATEST" "$REPO_DIR/data/adsops_latest.json"
fi

if ! git diff --quiet -- data/kpi_latest.json data/adsops_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json 2>/dev/null; then
  git add data/kpi_latest.json data/adsops_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json 2>/dev/null || git add data/kpi_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json
  git commit -m "data: refresh KPI snapshot $(date '+%Y-%m-%d %H:%M %Z')" || true
  git push origin main || echo "WARN: git push failed (non-interactive credentials); continuing to deploy"

  # Circuit breaker: after 2 consecutive deploy failures, pause auto-deploy for 2h.
  STATE_FILE="$LOG_DIR/deploy-state.env"
  NOW_EPOCH="$(date +%s)"
  COOLDOWN_SECS=7200
  FAILURE_THRESHOLD=2

  if [ -f "$STATE_FILE" ]; then
    # shellcheck disable=SC1090
    source "$STATE_FILE"
  fi

  FAILURES="${FAILURES:-0}"
  LAST_FAIL_EPOCH="${LAST_FAIL_EPOCH:-0}"

  if [ "$FAILURES" -ge "$FAILURE_THRESHOLD" ] && [ $((NOW_EPOCH - LAST_FAIL_EPOCH)) -lt "$COOLDOWN_SECS" ]; then
    REMAINING=$((COOLDOWN_SECS - (NOW_EPOCH - LAST_FAIL_EPOCH)))
    echo "WARN: deploy circuit open (${FAILURES} consecutive failures). Cooldown ${REMAINING}s remaining; skipping deploy."
    exit 0
  fi

  if vercel --prod --yes --scope eric-gagnons-projects >> "$LOG_DIR/deploy.log" 2>&1; then
    cat > "$STATE_FILE" <<EOF
FAILURES=0
LAST_FAIL_EPOCH=0
EOF
  else
    FAILURES=$((FAILURES + 1))
    cat > "$STATE_FILE" <<EOF
FAILURES=${FAILURES}
LAST_FAIL_EPOCH=${NOW_EPOCH}
EOF
    echo "WARN: vercel deploy failed (consecutive failures: ${FAILURES})"
  fi
else
  echo "No data changes; skipping deploy"
fi
