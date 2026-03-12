#!/usr/bin/env python3
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
ADS_DIR = WORKSPACE / 'exports' / 'meta-ads'
DB_PATH = WORKSPACE / 'ads-ops' / 'db' / 'kpi.sqlite'
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


def read_followers_series(limit=120):
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


def follower_daily_series(followers_rows):
    out = []
    prev = None
    for r in followers_rows:
        cur = num(r.get('followers_total'))
        daily = None if prev is None else cur - prev
        out.append({'date': r['date'], 'followers_per_day': daily, 'followers_total': cur})
        prev = cur
    return out


def build_spend_series(rows, limit=30):
    by_date = {}
    for r in rows:
        d = (r.get('date_start') or '').strip()
        if not d:
            continue
        by_date[d] = by_date.get(d, 0.0) + num(r.get('spend'))
    out = [{'date': d, 'spend': round(v, 2)} for d, v in sorted(by_date.items())]
    return out[-limit:]


def read_live_followers_stats():
    if not DB_PATH.exists():
        return {'current_followers_live': None, 'daily_gain_live': None, 'baseline_followers': None}

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        latest = conn.execute(
            """
            SELECT pulled_at_utc, follower_count
            FROM follower_snapshots
            WHERE username='thesocial.study'
            ORDER BY pulled_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
        if not latest:
            conn.close()
            return {'current_followers_live': None, 'daily_gain_live': None, 'baseline_followers': None}

        now_local = datetime.now(ZoneInfo('America/Los_Angeles'))
        day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = day_start_local.astimezone(timezone.utc).isoformat()

        baseline = conn.execute(
            """
            SELECT follower_count
            FROM follower_snapshots
            WHERE username='thesocial.study' AND pulled_at_utc < ?
            ORDER BY pulled_at_utc DESC
            LIMIT 1
            """,
            (day_start_utc,)
        ).fetchone()
        conn.close()

        current = int(latest['follower_count'])
        baseline_count = int(baseline['follower_count']) if baseline else current
        return {
            'current_followers_live': current,
            'daily_gain_live': current - baseline_count,
            'baseline_followers': baseline_count,
        }
    except Exception:
        return {'current_followers_live': None, 'daily_gain_live': None, 'baseline_followers': None}


def build_insights(summary, campaigns, followers_daily):
    insights = []
    if campaigns:
        best = sorted([c for c in campaigns if c.get('cpc') is not None], key=lambda x: x['cpc'])[0]
        worst = sorted([c for c in campaigns if c.get('cpc') is not None], key=lambda x: x['cpc'], reverse=True)[0]
        insights.append({'type': 'working', 'text': f"{best['campaign']} is most efficient (CPC ${best['cpc']:.2f}, CTR {best['ctr']:.2f}%)."})
        insights.append({'type': 'not_working', 'text': f"{worst['campaign']} is least efficient (CPC ${worst['cpc']:.2f}); consider reducing spend."})

    cpf = summary.get('blended_cost_per_follow')
    if cpf is None:
        insights.append({'type': 'data_gap', 'text': 'Follower attribution is still limited; blended CPF will improve as daily follower history grows.'})

    valid = [x for x in followers_daily if x.get('followers_per_day') is not None]
    if valid:
        latest = valid[-1]['followers_per_day']
        if latest is not None and latest < 0:
            insights.append({'type': 'alert', 'text': f'Latest daily followers is negative ({latest:.0f}); review creative fatigue and audience overlap.'})
        elif latest is not None:
            insights.append({'type': 'working', 'text': f'Latest daily followers: {latest:.0f}. Keep top campaign structure stable while testing one variable at a time.'})

    insights.append({'type': 'action', 'text': 'Keep budget under $60/day; shift budget from worst CPC campaign to best CPC campaign in small increments.'})
    return insights[:6]


def read_csv_rows(name):
    p = ADS_DIR / name
    if not p.exists():
        return []
    with open(p, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def read_follower_city_rows(limit=200):
    p = ADS_DIR / 'follower_demographics_city_latest.json'
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text())
        rows = d.get('rows') or []

        # Try to load earliest snapshot from today (PT) for true per-city daily gain.
        baseline_map = {}
        hist_dir = ADS_DIR / 'follower_demographics_city_history'
        if hist_dir.exists():
            today_pt = datetime.now(ZoneInfo('America/Los_Angeles')).date().isoformat()
            candidates = sorted(hist_dir.glob('follower_demographics_city_*.json'))
            day_files = []
            for f in candidates:
                try:
                    js = json.loads(f.read_text())
                    ts = js.get('updated_at', '')
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(ZoneInfo('America/Los_Angeles'))
                    if dt.date().isoformat() == today_pt:
                        day_files.append((dt, js))
                except Exception:
                    continue
            if day_files:
                day_files.sort(key=lambda x: x[0])
                first_js = day_files[0][1]
                for r in first_js.get('rows') or []:
                    c = (r.get('city') or '').strip()
                    if c:
                        baseline_map[c] = int(num(r.get('followers')))

        out = []
        for r in rows[:limit]:
            city = (r.get('city') or '').strip()
            if not city:
                continue
            cur = int(num(r.get('followers')))
            base = baseline_map.get(city, cur)
            out.append({'city': city, 'followers': cur, 'gained_today': cur - base})
        return out
    except Exception:
        return []


def top_breakdown(rows, dims, limit=12):
    agg = {}
    for r in rows:
        key = tuple((r.get(d) or 'Unknown').strip() or 'Unknown' for d in dims)
        item = agg.setdefault(key, {'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0, 'reach': 0.0})
        item['spend'] += num(r.get('spend'))
        item['clicks'] += num(r.get('clicks'))
        item['impressions'] += num(r.get('impressions'))
        item['reach'] += num(r.get('reach'))

    out = []
    for key, m in agg.items():
        clicks = m['clicks']
        impr = m['impressions']
        out.append({
            'label': ' / '.join(key),
            'spend': round(m['spend'], 2),
            'clicks': int(clicks),
            'impressions': int(impr),
            'reach': int(m['reach']),
            'ctr': round((clicks / impr) * 100, 3) if impr > 0 else 0.0,
            'cpc': round(m['spend'] / clicks, 3) if clicks > 0 else None,
        })

    out.sort(key=lambda x: x['spend'], reverse=True)
    return out[:limit]


def main():
    summary = read_summary()
    meta = read_meta_config()
    rows = read_insights_rows()
    campaigns = aggregate_hierarchy(rows)
    followers = read_followers_series()
    spend_series = build_spend_series(rows)
    live_followers = read_live_followers_stats()

    placement_rows = read_csv_rows('insights_placement_latest.csv')
    age_gender_rows = read_csv_rows('insights_age_gender_latest.csv')
    device_rows = read_csv_rows('insights_device_latest.csv')
    region_rows = read_csv_rows('insights_region_latest.csv')
    follower_city_rows = read_follower_city_rows()

    # Keep full follower history for trend continuity/backfill; ad insights can
    # have a different coverage window.
    followers_daily = follower_daily_series(followers)

    payload = {
        'updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'summary': {
            'ad_account_id': meta.get('ad_account_id'),
            'total_spend': summary.get('total_spend'),
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'total_follows': summary.get('total_follows'),
            'blended_cost_per_follow': summary.get('blended_cost_per_follow'),
            'since': summary.get('since'),
            'until': summary.get('until'),
            'current_followers_live': live_followers.get('current_followers_live'),
            'daily_gain_live': live_followers.get('daily_gain_live'),
            'baseline_followers': live_followers.get('baseline_followers'),
        },
        'campaigns': campaigns,
        'top_campaigns': campaigns[:10],
        'followers_series': followers,
        'followers_daily_series': followers_daily,
        'spend_series': spend_series,
        'insights': build_insights(summary, campaigns, followers_daily),
        'breakdowns': {
            'placement': top_breakdown(placement_rows, ['publisher_platform', 'platform_position']),
            'age_gender': top_breakdown(age_gender_rows, ['age', 'gender']),
            'device': top_breakdown(device_rows, ['device_platform']),
            'region': top_breakdown(region_rows, ['region']),
        },
        'follower_demographics': {
            'city': follower_city_rows,
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
