#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

cd "$REPO_DIR"

# Pull full ads + follower stack first (Meta API, follower demographics, follower fallback)
"$REPO_DIR/../ads-ops/scripts/run_kpi_pull.sh" >> "$LOG_DIR/full-kpi-pull.log" 2>&1 || true

# Build website snapshot from latest exports
python3 "$REPO_DIR/scripts/pull_kpis.py"

# Bring in expanded ads-ops payload (geo/age/device/placement breakdowns) when available.
ADSOPS_LATEST="$REPO_DIR/../ads-ops/dashboard/data/latest.json"
if [ -f "$ADSOPS_LATEST" ]; then
  cp "$ADSOPS_LATEST" "$REPO_DIR/data/adsops_latest.json"
fi

if ! git diff --quiet -- data/kpi_latest.json data/adsops_latest.json 2>/dev/null; then
  git add data/kpi_latest.json data/adsops_latest.json 2>/dev/null || git add data/kpi_latest.json
  git commit -m "data: refresh KPI snapshot $(date '+%Y-%m-%d %H:%M %Z')" || true
  git push origin main || echo "WARN: git push failed (non-interactive credentials); continuing to deploy"
  vercel --prod --yes --scope eric-gagnons-projects >> "$LOG_DIR/deploy.log" 2>&1 || true
else
  echo "No data changes; skipping deploy"
fi
