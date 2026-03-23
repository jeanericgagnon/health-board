#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

SITE_URL="${HEALTHBOARD_SITE_URL:-https://gaginonricky.com}"
export HEALTHBOARD_SITE_URL="$SITE_URL"

cd "$REPO_DIR"

# Ensure commits/deploy metadata use a Vercel team-authorized identity.
export GIT_AUTHOR_NAME="Jean Eric Gagnon"
export GIT_AUTHOR_EMAIL="jean.eric.gagnon619@gmail.com"
export GIT_COMMITTER_NAME="Jean Eric Gagnon"
export GIT_COMMITTER_EMAIL="jean.eric.gagnon619@gmail.com"

# Single-run lock: prevent overlapping cron/manual deploys.
LOCK_DIR="$LOG_DIR/deploy.lockdir"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "INFO: deploy lock busy; skipping overlapping run"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" >/dev/null 2>&1 || true' EXIT

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

# Pull competitor follower baselines (best effort)
python3 "$REPO_DIR/scripts/fetch_competitor_followers_blastup.py" >> "$LOG_DIR/creative-meta.log" 2>&1 || true

# Run analyzer (Phase 1 autopilot foundation)
python3 "$REPO_DIR/scripts/analyze_kpis.py" >> "$LOG_DIR/analyze-kpis.log" 2>&1 || true

# Bring in expanded ads-ops payload (geo/age/device/placement breakdowns) when available.
ADSOPS_LATEST="$REPO_DIR/../ads-ops/dashboard/data/latest.json"
if [ -f "$ADSOPS_LATEST" ]; then
  cp "$ADSOPS_LATEST" "$REPO_DIR/data/adsops_latest.json"
fi

# Commit only if data changed.
if ! git diff --quiet -- data/kpi_latest.json data/adsops_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl data/trend_intelligence_latest.json data/decision_state_latest.json data/creative_attribution_latest.json data/budget_movement_audit.json data/forecasting_tiles_latest.json data/fatigue_radar_latest.json data/winner_durability_latest.json 2>/dev/null; then
  git add data/kpi_latest.json data/adsops_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl data/trend_intelligence_latest.json data/decision_state_latest.json data/creative_attribution_latest.json data/budget_movement_audit.json data/forecasting_tiles_latest.json data/fatigue_radar_latest.json data/winner_durability_latest.json 2>/dev/null || git add data/kpi_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl
  git commit -m "data: refresh KPI snapshot $(date '+%Y-%m-%d %H:%M %Z')" || true
  git push origin main || echo "WARN: git push failed (non-interactive credentials); continuing to deploy"
else
  echo "INFO: no git data diff; publishing current build anyway"
fi

# Publish every cycle (single attempt).
vercel --prod --yes --scope eric-gagnons-projects >> "$LOG_DIR/deploy.log" 2>&1 || echo "WARN: vercel deploy failed"

# Post-deploy health ping: verify live endpoint serves a parseable updated_at.
python3 - <<'PY' >> "$LOG_DIR/deploy.log" 2>&1 || true
import json,urllib.request,os
site=os.environ.get('HEALTHBOARD_SITE_URL','https://gaginonricky.com').rstrip('/')
url=f"{site}/data/kpi_latest.json?ts=healthcheck"
try:
    with urllib.request.urlopen(url, timeout=15) as r:
        body=r.read().decode('utf-8','replace')
    j=json.loads(body)
    ua=j.get('updated_at')
    if ua:
        print(f"INFO: health ping ok; live updated_at={ua}")
    else:
        print("WARN: health ping missing updated_at")
except Exception as e:
    print(f"WARN: health ping failed: {e}")
PY
