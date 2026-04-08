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
# Current live source is the local duplicate Meta pull pipeline under:
#   /Users/ericsysclaw/.openclaw/workspace/ops/gaginonricky-kpi-dup
# Enable explicitly when needed:
#   RUN_KPI_PULL_FIRST=1 bash health-board/scripts/sync_and_deploy.sh
OPS_DUP_DIR="/Users/ericsysclaw/.openclaw/workspace/ops/gaginonricky-kpi-dup"
if [ "${RUN_KPI_PULL_FIRST:-0}" = "1" ]; then
  if [ -x "$OPS_DUP_DIR/run_meta_pull.sh" ]; then
    "$OPS_DUP_DIR/run_meta_pull.sh" >> "$LOG_DIR/full-kpi-pull.log" 2>&1 || true
  else
    echo "WARN: meta pull runner missing at $OPS_DUP_DIR/run_meta_pull.sh" >> "$LOG_DIR/full-kpi-pull.log"
  fi
else
  echo "INFO: skipping embedded KPI pull (RUN_KPI_PULL_FIRST!=1)"
fi

# Pull follower baseline first so KPI payload can use live baseline/day gain.
python3 "$REPO_DIR/scripts/fetch_followers_blastup.py" >> "$LOG_DIR/followers.log" 2>&1 || true

# Build website snapshot from latest exports (ad-level metrics are the primary grain)
python3 "$REPO_DIR/scripts/pull_kpis.py"

# Pull creative metadata for concept-level analysis (best effort)
python3 "$REPO_DIR/scripts/fetch_creative_metadata.py" >> "$LOG_DIR/creative-meta.log" 2>&1 || true

# Pull competitor follower baselines (best effort)
python3 "$REPO_DIR/scripts/fetch_competitor_followers_blastup.py" >> "$LOG_DIR/creative-meta.log" 2>&1 || true

# Generate follower geo payload status/file so the dashboard has an explicit source state.
python3 "$REPO_DIR/scripts/fetch_follower_demographics_city.py" >> "$LOG_DIR/followers.log" 2>&1 || true

# Run analyzer (Phase 1 autopilot foundation)
python3 "$REPO_DIR/scripts/analyze_kpis.py" >> "$LOG_DIR/analyze-kpis.log" 2>&1 || true

# Archive outputs + Desktop backups after generation.
python3 "$REPO_DIR/scripts/archive_and_backup.py" >> "$LOG_DIR/archive-and-backup.log" 2>&1 || true

# Build flywheel analytics join/view export (voice/hook/script + CPC trends)
FLYWHEEL_SCRIPT="$OPS_DUP_DIR/scripts/setup_flywheel_analytics.py"
if [ -f "$FLYWHEEL_SCRIPT" ]; then
  python3 "$FLYWHEEL_SCRIPT" >> "$LOG_DIR/analyze-kpis.log" 2>&1 || true
else
  echo "INFO: flywheel analytics script not found at $FLYWHEEL_SCRIPT; skipping" >> "$LOG_DIR/analyze-kpis.log"
fi

# Mirror flywheel export into health-board data for deploy
if [ -f "$REPO_DIR/data/flywheel_latest_metrics.json" ]; then
  : # already present
elif [ -f "$REPO_DIR/../health-board/data/flywheel_latest_metrics.json" ]; then
  cp "$REPO_DIR/../health-board/data/flywheel_latest_metrics.json" "$REPO_DIR/data/flywheel_latest_metrics.json"
fi

# Bring in the current Meta payloads from the active local Meta pull pipeline.
ADSOPS_LATEST="$OPS_DUP_DIR/data/adsops_latest.json"
if [ -f "$ADSOPS_LATEST" ]; then
  cp "$ADSOPS_LATEST" "$REPO_DIR/data/adsops_latest.json"
else
  echo "WARN: adsops payload missing at $ADSOPS_LATEST" >> "$LOG_DIR/analyze-kpis.log"
fi

META_ROLLUPS_LATEST="$OPS_DUP_DIR/data/meta_rollups_latest.json"
if [ -f "$META_ROLLUPS_LATEST" ]; then
  cp "$META_ROLLUPS_LATEST" "$REPO_DIR/data/meta_rollups_latest.json"
else
  echo "WARN: meta rollups payload missing at $META_ROLLUPS_LATEST" >> "$LOG_DIR/analyze-kpis.log"
fi

META_ENTITY_ROLLUPS_LATEST="$OPS_DUP_DIR/data/meta_entity_rollups_latest.json"
if [ -f "$META_ENTITY_ROLLUPS_LATEST" ]; then
  cp "$META_ENTITY_ROLLUPS_LATEST" "$REPO_DIR/data/meta_entity_rollups_latest.json"
else
  echo "WARN: meta entity rollups payload missing at $META_ENTITY_ROLLUPS_LATEST" >> "$LOG_DIR/analyze-kpis.log"
fi

META_HEALTH_LATEST="$OPS_DUP_DIR/data/meta_health_latest.json"
if [ -f "$META_HEALTH_LATEST" ]; then
  cp "$META_HEALTH_LATEST" "$REPO_DIR/data/meta_health_latest.json"
else
  echo "WARN: meta health payload missing at $META_HEALTH_LATEST" >> "$LOG_DIR/analyze-kpis.log"
fi

# Commit only if data changed.
if ! git diff --quiet -- data/kpi_latest.json data/adsops_latest.json data/meta_rollups_latest.json data/meta_entity_rollups_latest.json data/meta_health_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl data/follower_demographics_city_latest.json data/trend_intelligence_latest.json data/decision_state_latest.json data/creative_attribution_latest.json data/budget_movement_audit.json data/forecasting_tiles_latest.json data/fatigue_radar_latest.json data/winner_durability_latest.json data/flywheel_latest_metrics.json 2>/dev/null; then
  git add data/kpi_latest.json data/adsops_latest.json data/meta_rollups_latest.json data/meta_entity_rollups_latest.json data/meta_health_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl data/follower_demographics_city_latest.json data/trend_intelligence_latest.json data/decision_state_latest.json data/creative_attribution_latest.json data/budget_movement_audit.json data/forecasting_tiles_latest.json data/fatigue_radar_latest.json data/winner_durability_latest.json data/flywheel_latest_metrics.json 2>/dev/null || git add data/kpi_latest.json data/analysis_latest.json data/analysis_brief.txt data/analysis_history.jsonl data/creative_metadata_latest.json data/competitor_followers_latest.json data/competitor_followers_history.jsonl data/follower_demographics_city_latest.json data/flywheel_latest_metrics.json data/meta_rollups_latest.json data/meta_entity_rollups_latest.json data/meta_health_latest.json
  git commit -m "data: refresh KPI snapshot $(date '+%Y-%m-%d %H:%M %Z')" || true
  git push origin main || echo "WARN: git push failed (non-interactive credentials); continuing to deploy"
else
  echo "INFO: no git data diff; publishing current build anyway"
fi

# Publish every cycle (single attempt).
VERCEL_BIN="$(command -v vercel || true)"
if [ -z "$VERCEL_BIN" ] && [ -x "/opt/homebrew/bin/vercel" ]; then
  VERCEL_BIN="/opt/homebrew/bin/vercel"
fi
if [ -z "$VERCEL_BIN" ]; then
  echo "WARN: vercel CLI not found in PATH or /opt/homebrew/bin/vercel" >> "$LOG_DIR/deploy.log"
else
  "$VERCEL_BIN" --prod --yes --scope eric-gagnons-projects >> "$LOG_DIR/deploy.log" 2>&1 || echo "WARN: vercel deploy failed" >> "$LOG_DIR/deploy.log"
fi

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
