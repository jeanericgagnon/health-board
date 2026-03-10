#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

cd "$REPO_DIR"

# Best-effort follower scrape fallback (Blastup) to keep blended CPF current.
python3 "$REPO_DIR/scripts/fetch_followers_blastup.py" >> "$LOG_DIR/follower-scrape.log" 2>&1 || true

python3 "$REPO_DIR/scripts/pull_kpis.py"

if ! git diff --quiet -- data/kpi_latest.json; then
  git add data/kpi_latest.json
  git commit -m "data: refresh KPI snapshot $(date '+%Y-%m-%d %H:%M %Z')"
  git push origin main
  vercel --prod --yes --scope eric-gagnons-projects >> "$LOG_DIR/deploy.log" 2>&1
else
  echo "No data changes; skipping deploy"
fi
