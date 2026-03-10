#!/usr/bin/env python3
import csv
import json
from datetime import datetime, UTC
from pathlib import Path

WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
ADS_DIR = WORKSPACE / 'exports' / 'meta-ads'
OUT = (Path(__file__).resolve().parents[1] / 'data' / 'kpi_latest.json')


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


def read_meta_config():
    p = ADS_DIR / 'config.json'
    if not p.exists():
        return {}
    try:
        cfg = json.loads(p.read_text())
        return {'ad_account_id': cfg.get('ad_account_id')}
    except Exception:
        return {}


def read_insights_rows():
    p = ADS_DIR / 'insights_latest.csv'
    if not p.exists():
        return []
    with open(p, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def aggregate_hierarchy(rows):
    campaigns = {}

    for r in rows:
        c = (r.get('campaign_name') or 'Unknown Campaign').strip() or 'Unknown Campaign'
        s = (r.get('adset_name') or 'Unknown Ad Set').strip() or 'Unknown Ad Set'
        a = (r.get('ad_name') or 'Unknown Ad').strip() or 'Unknown Ad'
        cid = (r.get('campaign_id') or '').strip()
        sid = (r.get('adset_id') or '').strip()
        aid = (r.get('ad_id') or '').strip()

        cobj = campaigns.setdefault(c, {
            'campaign': c,
            'campaign_id': cid,
            'spend': 0.0,
            'clicks': 0.0,
            'impressions': 0.0,
            'reach': 0.0,
            'ctr_sum': 0.0,
            'cpm_sum': 0.0,
            'cpc_sum': 0.0,
            'rows': 0,
            'adsets': {}
        })
        sobj = cobj['adsets'].setdefault(s, {
            'adset': s,
            'adset_id': sid,
            'spend': 0.0,
            'clicks': 0.0,
            'impressions': 0.0,
            'reach': 0.0,
            'ctr_sum': 0.0,
            'cpm_sum': 0.0,
            'cpc_sum': 0.0,
            'rows': 0,
            'ads': {}
        })
        aobj = sobj['ads'].setdefault(a, {
            'ad': a,
            'ad_id': aid,
            'spend': 0.0,
            'clicks': 0.0,
            'impressions': 0.0,
            'reach': 0.0,
            'ctr_sum': 0.0,
            'cpm_sum': 0.0,
            'cpc_sum': 0.0,
            'rows': 0,
        })

        for obj in (cobj, sobj, aobj):
            obj['spend'] += num(r.get('spend'))
            obj['clicks'] += num(r.get('clicks'))
            obj['impressions'] += num(r.get('impressions'))
            obj['reach'] += num(r.get('reach'))
            obj['ctr_sum'] += num(r.get('ctr'))
            obj['cpm_sum'] += num(r.get('cpm'))
            obj['cpc_sum'] += num(r.get('cpc'))
            obj['rows'] += 1

    def finalize_node(node):
        rows = node['rows'] or 1
        ctr = node['ctr_sum'] / rows
        cpm = node['cpm_sum'] / rows
        cpc = node['spend'] / node['clicks'] if node['clicks'] > 0 else None
        out = {
            k: v for k, v in node.items()
            if k not in {'adsets', 'ads', 'ctr_sum', 'cpm_sum', 'cpc_sum', 'rows'}
        }
        out.update({
            'spend': round(out['spend'], 2),
            'clicks': int(out['clicks']),
            'impressions': int(out['impressions']),
            'reach': int(out['reach']),
            'ctr': round(ctr, 3),
            'cpm': round(cpm, 3),
            'cpc': None if cpc is None else round(cpc, 3),
        })
        return out

    campaign_list = []
    for c in campaigns.values():
        c_out = finalize_node(c)
        adsets = []
        for s in c['adsets'].values():
            s_out = finalize_node(s)
            ads = [finalize_node(a) for a in s['ads'].values()]
            ads.sort(key=lambda x: x['spend'], reverse=True)
            s_out['ads'] = ads
            adsets.append(s_out)
        adsets.sort(key=lambda x: x['spend'], reverse=True)
        c_out['adsets'] = adsets
        campaign_list.append(c_out)

    campaign_list.sort(key=lambda x: x['spend'], reverse=True)
    return campaign_list


def read_followers_series(limit=30):
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


def build_spend_series(rows, limit=30):
    by_date = {}
    for r in rows:
        d = (r.get('date_start') or '').strip()
        if not d:
            continue
        by_date[d] = by_date.get(d, 0.0) + num(r.get('spend'))
    out = [{'date': d, 'spend': round(v, 2)} for d, v in sorted(by_date.items())]
    return out[-limit:]


def main():
    summary = read_summary()
    meta = read_meta_config()
    rows = read_insights_rows()
    campaigns = aggregate_hierarchy(rows)
    followers = read_followers_series()
    spend_series = build_spend_series(rows)

    payload = {
        'updated_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
        'summary': {
            'ad_account_id': meta.get('ad_account_id'),
            'total_spend': summary.get('total_spend'),
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'total_follows': summary.get('total_follows'),
            'blended_cost_per_follow': summary.get('blended_cost_per_follow'),
            'since': summary.get('since'),
            'until': summary.get('until'),
        },
        'campaigns': campaigns,
        'top_campaigns': campaigns[:10],
        'followers_series': followers,
        'spend_series': spend_series,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
