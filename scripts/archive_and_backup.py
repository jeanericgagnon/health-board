#!/usr/bin/env python3
import csv
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
DATA_DIR = REPO / 'data'
ARCHIVE_DIR = REPO / 'archive'
DESKTOP_ROOT = Path.home() / 'Desktop' / 'health-board-backups'
EXPORTS_DIR = WORKSPACE / 'exports' / 'meta-ads'
OPS_DUP_DIR = WORKSPACE / 'ops' / 'gaginonricky-kpi-dup'
DB_PATH = OPS_DUP_DIR / 'gaginonricky_kpi.sqlite'
FOLLOWERS_CSV = EXPORTS_DIR / 'followers_daily.csv'
FOLLOWER_SNAPSHOTS_JSONL = DATA_DIR / 'follower_snapshots_history.jsonl'
FOLLOWER_BASELINE_JSON = DATA_DIR / 'follower_baseline_latest.json'
KPI_HISTORY_JSONL = DATA_DIR / 'kpi_history.jsonl'
PT = ZoneInfo('America/Los_Angeles')

PUBLISH_FILES = [
    'kpi_latest.json',
    'analysis_latest.json',
    'analysis_brief.txt',
    'analysis_history.jsonl',
    'creative_metadata_latest.json',
    'competitor_followers_latest.json',
    'competitor_followers_history.jsonl',
    'trend_intelligence_latest.json',
    'decision_state_latest.json',
    'creative_attribution_latest.json',
    'budget_movement_audit.json',
    'forecasting_tiles_latest.json',
    'fatigue_radar_latest.json',
    'winner_durability_latest.json',
    'adsops_latest.json',
    'flywheel_latest_metrics.json',
    'manual_intraday_spend.json',
    'follower_baseline_latest.json',
    'follower_snapshots_history.jsonl',
    'kpi_history.jsonl',
]


def read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def append_jsonl(path: Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(obj) + '\n')


def latest_followers_total():
    if not FOLLOWERS_CSV.exists():
        return None
    try:
        rows = []
        with FOLLOWERS_CSV.open(newline='', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        rows = [r for r in rows if r.get('date') and r.get('followers_total') not in (None, '')]
        if not rows:
            return None
        rows.sort(key=lambda r: r['date'])
        return int(float(str(rows[-1]['followers_total']).replace(',', '')))
    except Exception:
        return None


def previous_day_followers_total():
    if not FOLLOWERS_CSV.exists():
        return None
    try:
        with FOLLOWERS_CSV.open(newline='', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        rows = [r for r in rows if r.get('date') and r.get('followers_total') not in (None, '')]
        rows.sort(key=lambda r: r['date'])
        today = datetime.now(PT).date().isoformat()
        prior = [r for r in rows if r['date'] < today]
        if not prior:
            return None
        return int(float(str(prior[-1]['followers_total']).replace(',', '')))
    except Exception:
        return None


def write_follower_baseline():
    current = latest_followers_total()
    baseline = previous_day_followers_total()
    now_utc = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    now_local = datetime.now(PT).isoformat()
    payload = {
        'updated_at': now_utc,
        'as_of_local': now_local,
        'current_followers_live': current,
        'baseline_followers': baseline if baseline is not None else current,
        'daily_gain_live': None if current is None else (current - (baseline if baseline is not None else current)),
        'source': str(FOLLOWERS_CSV),
    }
    FOLLOWER_BASELINE_JSON.write_text(json.dumps(payload, indent=2))
    if current is not None:
        append_jsonl(FOLLOWER_SNAPSHOTS_JSONL, {
            'pulled_at_utc': now_utc,
            'as_of_local': now_local,
            'followers_total': current,
            'baseline_followers': payload['baseline_followers'],
            'daily_gain_live': payload['daily_gain_live'],
        })
    return payload


def archive_outputs():
    now = datetime.now(PT)
    stamp = now.strftime('%Y%m%d-%H%M%S')
    daily_dir = ARCHIVE_DIR / now.strftime('%Y-%m-%d') / stamp
    desktop_dir = DESKTOP_ROOT / now.strftime('%Y-%m-%d') / stamp
    daily_dir.mkdir(parents=True, exist_ok=True)
    desktop_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        'created_at_utc': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'created_at_local': now.isoformat(),
        'repo': str(REPO),
        'files': [],
        'db_snapshot': None,
    }

    for name in PUBLISH_FILES:
        src = DATA_DIR / name
        if not src.exists():
            continue
        for target_root in (daily_dir, desktop_dir):
            shutil.copy2(src, target_root / name)
        manifest['files'].append(name)

    if DB_PATH.exists():
        db_name = f'gaginonricky_kpi_{stamp}.sqlite'
        shutil.copy2(DB_PATH, daily_dir / db_name)
        shutil.copy2(DB_PATH, desktop_dir / db_name)
        manifest['db_snapshot'] = db_name

    (daily_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    (desktop_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    return {'archive_dir': str(daily_dir), 'desktop_dir': str(desktop_dir), 'manifest': manifest}


def append_kpi_history():
    kpi = read_json(DATA_DIR / 'kpi_latest.json') or {}
    if not kpi:
        return None
    summary = kpi.get('summary') or {}
    payload = {
        'updated_at': kpi.get('updated_at'),
        'summary': {
            'total_spend': summary.get('total_spend'),
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'current_followers_live': summary.get('current_followers_live'),
            'baseline_followers': summary.get('baseline_followers'),
            'daily_gain_live': summary.get('daily_gain_live'),
            'blended_cost_per_follow': summary.get('blended_cost_per_follow'),
        },
        'campaign_count': len(kpi.get('campaigns') or []),
        'ad_decision_count': len(((kpi.get('decision_state') or {}).get('rows') or [])),
    }
    append_jsonl(KPI_HISTORY_JSONL, payload)
    return payload


def ensure_ad_metrics_indexes():
    if not DB_PATH.exists():
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ad_metrics_snapshot ON ad_metrics(snapshot_updated_at_utc)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ad_metrics_ad_id ON ad_metrics(ad_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ad_metrics_campaign_id ON ad_metrics(campaign_id)')
        conn.commit()
        conn.close()
    except Exception:
        pass


def main():
    ensure_ad_metrics_indexes()
    baseline = write_follower_baseline()
    history = append_kpi_history()
    archived = archive_outputs()
    print(json.dumps({
        'ok': True,
        'baseline': baseline,
        'kpi_history_appended': history is not None,
        'archive_dir': archived['archive_dir'],
        'desktop_dir': archived['desktop_dir'],
        'file_count': len(archived['manifest']['files']),
        'db_snapshot': archived['manifest']['db_snapshot'],
    }, indent=2))


if __name__ == '__main__':
    main()
