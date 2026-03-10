#!/usr/bin/env python3
import csv
import json
from datetime import datetime
from pathlib import Path

WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
ADS_DIR = WORKSPACE / 'exports' / 'meta-ads'
OUT = Path('/tmp/health-board/data/kpi_latest.json')


def num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def read_summary():
    p = ADS_DIR / 'summary_latest.json'
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def read_top_campaigns(limit=6):
    p = ADS_DIR / 'insights_latest.csv'
    if not p.exists():
        return []

    agg = {}
    with open(p, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            name = (r.get('campaign_name') or 'Unknown').strip() or 'Unknown'
            row = agg.setdefault(name, {'campaign': name, 'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0, 'ctr_sum': 0.0, 'rows': 0})
            row['spend'] += num(r.get('spend'))
            row['clicks'] += num(r.get('clicks'))
            row['impressions'] += num(r.get('impressions'))
            row['ctr_sum'] += num(r.get('ctr'))
            row['rows'] += 1

    out = []
    for v in agg.values():
        ctr = (v['ctr_sum'] / v['rows']) if v['rows'] else 0.0
        cpc = (v['spend'] / v['clicks']) if v['clicks'] > 0 else None
        out.append({
            'campaign': v['campaign'],
            'spend': round(v['spend'], 2),
            'clicks': int(v['clicks']),
            'impressions': int(v['impressions']),
            'ctr': round(ctr, 3),
            'cpc': None if cpc is None else round(cpc, 3),
        })

    out.sort(key=lambda x: x['spend'], reverse=True)
    return out[:limit]


def read_followers_series(limit=14):
    p = ADS_DIR / 'followers_daily.csv'
    if not p.exists():
        return []
    rows = []
    with open(p, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            d = r.get('date')
            t = r.get('followers_total')
            if not d or t in ('', None):
                continue
            rows.append({'date': d, 'followers_total': num(t)})
    rows.sort(key=lambda x: x['date'])
    return rows[-limit:]


def main():
    summary = read_summary()
    campaigns = read_top_campaigns()
    followers = read_followers_series()

    payload = {
        'updated_at': datetime.utcnow().isoformat() + 'Z',
        'summary': {
            'total_spend': summary.get('total_spend'),
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'total_follows': summary.get('total_follows'),
            'blended_cost_per_follow': summary.get('blended_cost_per_follow'),
            'since': summary.get('since'),
            'until': summary.get('until'),
        },
        'top_campaigns': campaigns,
        'followers_series': followers,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
