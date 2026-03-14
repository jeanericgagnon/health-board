#!/usr/bin/env python3
import csv
import json
import sqlite3
from datetime import datetime, timezone, date
from pathlib import Path
from zoneinfo import ZoneInfo

WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
ADS_DIR = WORKSPACE / 'exports' / 'meta-ads'
DB_PATH = WORKSPACE / 'ads-ops' / 'db' / 'kpi.sqlite'
DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
OUT = DATA_DIR / 'kpi_latest.json'
MANUAL_INTRADAY_SPEND_PATH = DATA_DIR / 'manual_intraday_spend.json'
ADSOPS_LATEST_PATH = WORKSPACE / 'ads-ops' / 'dashboard' / 'data' / 'latest.json'


def num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def read_adsops_latest():
    if not ADSOPS_LATEST_PATH.exists():
        return None
    try:
        return json.loads(ADSOPS_LATEST_PATH.read_text())
    except Exception:
        return None


def read_summary():
    # Prefer fresh ads-ops dashboard payload (DB-backed) when available.
    d = read_adsops_latest()
    if d:
        try:
            campaigns = d.get('campaign') or []
            total_spend = round(sum(num(x.get('spend')) for x in campaigns), 2)
            total_clicks = int(sum(num(x.get('clicks')) for x in campaigns))
            total_impressions = int(sum(num(x.get('impressions')) for x in campaigns))
            return {
                'since': None,
                'until': None,
                'rows': len(campaigns),
                'total_spend': total_spend,
                'total_clicks': total_clicks,
                'total_impressions': total_impressions,
                'total_follows': 0,
                'blended_cost_per_follow': None,
                'pulled_at': d.get('updated_at'),
            }
        except Exception:
            pass

    # Fallback: legacy exports summary
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


def read_manual_intraday_spend_override():
    if not MANUAL_INTRADAY_SPEND_PATH.exists():
        return None
    try:
        d = json.loads(MANUAL_INTRADAY_SPEND_PATH.read_text())
        m = d.get('campaign_spend') or {}
        if not isinstance(m, dict) or not m:
            return None
        as_of_local = d.get('as_of_local')
        apply_until_local_date = d.get('apply_until_local_date')
        if not apply_until_local_date and as_of_local:
            try:
                apply_until_local_date = datetime.fromisoformat(str(as_of_local)).date().isoformat()
            except Exception:
                apply_until_local_date = None
        return {
            'as_of_local': as_of_local,
            'apply_until_local_date': apply_until_local_date,
            'campaign_spend': {str(k): num(v) for k, v in m.items()},
            'total_spend': num(d.get('total_spend')) if d.get('total_spend') is not None else None,
            'source': d.get('source') or 'manual_user_input',
        }
    except Exception:
        return None


def read_insights_rows():
    # Primary source: latest raw ad-level payload from SQLite (contains date_start/date_stop).
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT payload_json
                FROM kpi_snapshots
                WHERE source='meta_marketing_api' AND level='ad'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            conn.close()
            if row and row['payload_json']:
                rows = json.loads(row['payload_json'])
                if isinstance(rows, list) and rows:
                    return rows
        except Exception:
            pass

    # Fallback: legacy CSV export.
    p = ADS_DIR / 'insights_latest.csv'
    if not p.exists():
        return []
    with open(p, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def action_count(row, action_type: str):
    raw = row.get('actions')
    if not raw:
        return 0.0
    try:
        arr = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(arr, list):
            return 0.0
        total = 0.0
        for a in arr:
            if (a or {}).get('action_type') == action_type:
                total += num((a or {}).get('value'))
        return total
    except Exception:
        return 0.0


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
            'link_clicks': 0.0,
            'outbound_clicks': 0.0,
            'landing_page_views': 0.0,
            'freq_num': 0.0,
            'freq_den': 0.0,
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
            'link_clicks': 0.0,
            'outbound_clicks': 0.0,
            'landing_page_views': 0.0,
            'freq_num': 0.0,
            'freq_den': 0.0,
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
            'link_clicks': 0.0,
            'outbound_clicks': 0.0,
            'landing_page_views': 0.0,
            'freq_num': 0.0,
            'freq_den': 0.0,
        })

        link_clicks = action_count(r, 'link_click') or num(r.get('inline_link_clicks'))
        outbound_clicks = action_count(r, 'outbound_click') or num(r.get('outbound_clicks'))
        lpv = action_count(r, 'landing_page_view')
        raw_freq = r.get('frequency')
        freq = num(raw_freq) if raw_freq not in (None, '') else None
        impr = num(r.get('impressions'))

        for obj in (cobj, sobj, aobj):
            obj['spend'] += num(r.get('spend'))
            obj['clicks'] += num(r.get('clicks'))
            obj['impressions'] += impr
            obj['reach'] += num(r.get('reach'))
            obj['ctr_sum'] += num(r.get('ctr'))
            obj['cpm_sum'] += num(r.get('cpm'))
            obj['cpc_sum'] += num(r.get('cpc'))
            obj['link_clicks'] += link_clicks
            obj['outbound_clicks'] += outbound_clicks
            obj['landing_page_views'] += lpv
            if freq is not None:
                obj['freq_num'] += (freq * impr)
                obj['freq_den'] += impr
            obj['rows'] += 1

    def finalize_node(node):
        rows = node['rows'] or 1
        ctr = node['ctr_sum'] / rows
        cpm = node['cpm_sum'] / rows
        cpc = node['spend'] / node['clicks'] if node['clicks'] > 0 else None
        out = {
            k: v for k, v in node.items()
            if k not in {'adsets', 'ads', 'ctr_sum', 'cpm_sum', 'cpc_sum', 'rows', 'freq_num', 'freq_den'}
        }
        freq_avg = (node.get('freq_num', 0.0) / node.get('freq_den', 0.0)) if node.get('freq_den', 0.0) > 0 else None
        out_clicks = out.get('outbound_clicks', 0.0)
        lpv = out.get('landing_page_views', 0.0)
        out.update({
            'spend': round(out['spend'], 2),
            'clicks': int(out['clicks']),
            'impressions': int(out['impressions']),
            'reach': int(out['reach']),
            'link_clicks': int(out.get('link_clicks', 0.0)),
            'outbound_clicks': int(out_clicks),
            'landing_page_views': int(lpv),
            'ctr': round(ctr, 3),
            'cpm': round(cpm, 3),
            'cpc': None if cpc is None else round(cpc, 3),
            'outbound_ctr': round((out_clicks / out['impressions']) * 100, 3) if out['impressions'] > 0 else 0.0,
            'lpv_rate': round((lpv / out_clicks) * 100, 3) if out_clicks > 0 else None,
            'cost_per_lpv': round(out['spend'] / lpv, 3) if lpv > 0 else None,
            'frequency_avg': None if freq_avg is None else round(freq_avg, 3),
            'first_time_impression_ratio': round((out['reach'] / out['impressions']) * 100, 3) if out['impressions'] > 0 else None,
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
    # Prefer live DB snapshots so chart/date advances even when manual CSV lags.
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT pulled_at_utc, follower_count
                FROM follower_snapshots
                WHERE username='thesocial.study'
                ORDER BY pulled_at_utc ASC, id ASC
            """).fetchall()
            conn.close()

            by_day = {}
            for r in rows:
                ts = r['pulled_at_utc']
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(ZoneInfo('America/Los_Angeles'))
                day = dt.date().isoformat()
                by_day[day] = int(r['follower_count'])  # latest snapshot wins for day

            out = [{'date': d, 'followers_total': v} for d, v in sorted(by_day.items())]
            if out:
                return out[-limit:]
        except Exception:
            pass

    # Fallback: legacy CSV backfill
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


def build_campaign_daily(rows, limit_days=30):
    # Keep raw daily campaign stats so UI can apply 1D/5D/7D/14D toggles.
    by_key = {}
    for r in rows:
        d = (r.get('date_start') or '').strip()
        cid = (r.get('campaign_id') or '').strip()
        cname = (r.get('campaign_name') or 'Unknown Campaign').strip() or 'Unknown Campaign'
        if not d or not cid:
            continue
        key = (d, cid, cname)
        m = by_key.setdefault(key, {
            'date': d, 'campaign_id': cid, 'campaign_name': cname,
            'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0,
            'landing_page_views': 0.0, 'freq_num': 0.0, 'freq_den': 0.0
        })
        impr = num(r.get('impressions'))
        freq_raw = r.get('frequency')
        freq = num(freq_raw) if freq_raw not in (None, '') else None
        lpv = action_count(r, 'landing_page_view')
        m['spend'] += num(r.get('spend'))
        m['clicks'] += num(r.get('clicks'))
        m['impressions'] += impr
        m['landing_page_views'] += lpv
        if freq is not None:
            m['freq_num'] += (freq * impr)
            m['freq_den'] += impr
    out = []
    for m in by_key.values():
        impr = m['impressions']
        clk = m['clicks']
        lpv = m['landing_page_views']
        freq_avg = (m['freq_num'] / m['freq_den']) if m['freq_den'] > 0 else None
        out.append({
            'date': m['date'],
            'campaign_id': m['campaign_id'],
            'campaign_name': m['campaign_name'],
            'spend': round(m['spend'], 2),
            'clicks': int(clk),
            'impressions': int(impr),
            'landing_page_views': int(lpv),
            'ctr': round((clk / impr) * 100, 3) if impr > 0 else 0.0,
            'cpc': round(m['spend'] / clk, 3) if clk > 0 else None,
            'cost_per_lpv': round(m['spend'] / lpv, 3) if lpv > 0 else None,
            'frequency_avg': None if freq_avg is None else round(freq_avg, 3),
        })
    out.sort(key=lambda x: (x['date'], x['campaign_name']))
    return out[-(limit_days * 10):]


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


def build_insights(summary, campaigns, followers_daily, spend_series, recommendations, data_health):
    insights = []
    if campaigns:
        cpc_ready = [c for c in campaigns if c.get('cpc') is not None]
        if cpc_ready:
            best = sorted(cpc_ready, key=lambda x: x['cpc'])[0]
            worst = sorted(cpc_ready, key=lambda x: x['cpc'], reverse=True)[0]
            insights.append({'type': 'working', 'text': f"{best['campaign']} is most efficient (CPC ${best['cpc']:.2f}, CTR {best['ctr']:.2f}%)."})
            insights.append({'type': 'not_working', 'text': f"{worst['campaign']} is least efficient (CPC ${worst['cpc']:.2f}); consider reducing spend."})

        high_freq = sorted([c for c in campaigns if c.get('frequency_avg') is not None], key=lambda x: x.get('frequency_avg') or 0, reverse=True)
        if high_freq and (high_freq[0].get('frequency_avg') or 0) >= 1.8:
            insights.append({'type': 'alert', 'text': f"Fatigue risk: {high_freq[0]['campaign']} frequency is {high_freq[0]['frequency_avg']:.2f}. Consider creative refresh or audience expansion."})

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

    if spend_series:
        today_spend = num(spend_series[-1].get('spend'))
        insights.append({'type': 'action', 'text': f'Pacing watch: spend today is ${today_spend:.2f} vs $60/day target.'})

    if recommendations:
        tags = ', '.join([f"{r['campaign']}: {r['tag']}" for r in recommendations[:3]])
        insights.append({'type': 'action', 'text': f'Auto recommendations: {tags}.'})

    if data_health.get('status') != 'ok':
        insights.append({'type': 'alert', 'text': f"Data health: {data_health.get('status')} ({data_health.get('reason')})."})

    return insights[:8]


def read_csv_rows(name):
    # Prefer fresh DB-backed breakdowns from ads-ops latest payload.
    d = read_adsops_latest()
    if d:
        b = d.get('breakdowns') or {}
        if name == 'insights_placement_latest.csv' and (b.get('placement') or []):
            return b.get('placement') or []
        if name == 'insights_age_gender_latest.csv' and (b.get('age_gender') or []):
            return b.get('age_gender') or []
        if name == 'insights_device_latest.csv' and (b.get('device') or []):
            return b.get('device') or []
        if name == 'insights_region_latest.csv' and (b.get('region') or []):
            return b.get('region') or []

    p = ADS_DIR / name
    if not p.exists():
        return []
    with open(p, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def video_metric_count(raw):
    if raw in (None, ''):
        return 0.0
    try:
        arr = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(arr, list) and arr:
            return num((arr[0] or {}).get('value'))
    except Exception:
        pass
    return 0.0


def build_creative_hook_leaderboard(rows, limit=12):
    ads = {}
    for r in rows:
        ad_id = (r.get('ad_id') or '').strip()
        ad_name = (r.get('ad_name') or 'Unknown Ad').strip() or 'Unknown Ad'
        if not ad_id:
            continue
        m = ads.setdefault(ad_id, {
            'ad_id': ad_id, 'ad_name': ad_name, 'campaign_name': (r.get('campaign_name') or '').strip(),
            'spend': 0.0, 'impressions': 0.0, 'clicks': 0.0, 'plays': 0.0, 'p25': 0.0
        })
        m['spend'] += num(r.get('spend'))
        m['impressions'] += num(r.get('impressions'))
        m['clicks'] += num(r.get('clicks'))
        m['plays'] += video_metric_count(r.get('video_play_actions'))
        m['p25'] += video_metric_count(r.get('video_p25_watched_actions'))

    out = []
    for m in ads.values():
        ctr = (m['clicks'] / m['impressions'] * 100) if m['impressions'] > 0 else 0.0
        cpc = (m['spend'] / m['clicks']) if m['clicks'] > 0 else None
        play_rate = (m['plays'] / m['impressions'] * 100) if m['impressions'] > 0 else 0.0
        hold_rate = (m['p25'] / m['plays'] * 100) if m['plays'] > 0 else 0.0
        score = (ctr * 0.5) + (play_rate * 0.25) + (hold_rate * 0.25) - ((cpc or 2.0) * 4)
        out.append({
            'ad_id': m['ad_id'], 'ad_name': m['ad_name'], 'campaign_name': m['campaign_name'],
            'spend': round(m['spend'], 2), 'impressions': int(m['impressions']), 'clicks': int(m['clicks']),
            'ctr': round(ctr, 3), 'cpc': None if cpc is None else round(cpc, 3),
            'play_rate_3s': round(play_rate, 3), 'hold_rate_25pct': round(hold_rate, 3),
            'score': round(score, 3)
        })
    out.sort(key=lambda x: x['score'], reverse=True)
    return out[:limit]


def _bucket_confidence(sample_points=0, spend=0.0, impressions=0.0):
    if sample_points >= 10 and spend >= 40 and impressions >= 20000:
        return 'high'
    if sample_points >= 6 and spend >= 20 and impressions >= 9000:
        return 'med'
    return 'low'


def build_creative_fatigue_diagnostics(rows, limit=12):
    def collect(entity='campaign'):
        by_key = {}
        for r in rows:
            d = (r.get('date_start') or '').strip()
            if not d:
                continue
            if entity == 'campaign':
                key = (r.get('campaign_id') or '').strip() or (r.get('campaign_name') or 'Unknown Campaign').strip()
                label = (r.get('campaign_name') or 'Unknown Campaign').strip() or 'Unknown Campaign'
            else:
                key = (r.get('ad_id') or '').strip() or (r.get('ad_name') or 'Unknown Ad').strip()
                label = (r.get('ad_name') or 'Unknown Ad').strip() or 'Unknown Ad'
            if not key:
                continue
            m = by_key.setdefault((key, label), {})
            cur = m.setdefault(d, {'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0, 'freq_num': 0.0, 'freq_den': 0.0})
            impr = num(r.get('impressions'))
            freq_raw = r.get('frequency')
            freq = num(freq_raw) if freq_raw not in (None, '') else None
            cur['spend'] += num(r.get('spend'))
            cur['clicks'] += num(r.get('clicks'))
            cur['impressions'] += impr
            if freq is not None:
                cur['freq_num'] += (freq * impr)
                cur['freq_den'] += impr

        out = []
        for (entity_id, label), daymap in by_key.items():
            days = sorted(daymap.keys())
            if len(days) < 4:
                continue
            recent_days = days[-3:]
            baseline_days = days[-10:-3] if len(days) > 6 else days[:-3]
            if not baseline_days:
                continue

            def aggregate(day_keys):
                spend = sum(daymap[d]['spend'] for d in day_keys)
                clicks = sum(daymap[d]['clicks'] for d in day_keys)
                impr = sum(daymap[d]['impressions'] for d in day_keys)
                freq_num = sum(daymap[d]['freq_num'] for d in day_keys)
                freq_den = sum(daymap[d]['freq_den'] for d in day_keys)
                return {
                    'spend': spend,
                    'clicks': clicks,
                    'impressions': impr,
                    'ctr': (clicks / impr * 100) if impr > 0 else 0.0,
                    'cpc': (spend / clicks) if clicks > 0 else None,
                    'frequency_avg': (freq_num / freq_den) if freq_den > 0 else None,
                }

            recent = aggregate(recent_days)
            base = aggregate(baseline_days)
            if recent['impressions'] < 1200 or recent['spend'] < 8:
                continue

            freq = recent.get('frequency_avg')
            freq_pressure = max(0.0, min(1.0, ((freq or 0.0) - 1.7) / 1.2))
            ctr_decay = max(0.0, ((base['ctr'] - recent['ctr']) / base['ctr'])) if base['ctr'] > 0 else 0.0
            if base.get('cpc') and recent.get('cpc'):
                cpc_decay = max(0.0, ((recent['cpc'] - base['cpc']) / base['cpc']))
            else:
                cpc_decay = 0.0

            score = (freq_pressure * 42.0) + (min(1.2, ctr_decay) * 34.0) + (min(1.2, cpc_decay) * 24.0)
            out.append({
                f'{entity}_id': entity_id,
                entity: label,
                'fatigue_score': round(min(100.0, max(0.0, score)), 1),
                'frequency_pressure': round(freq_pressure, 3),
                'ctr_decay': round(ctr_decay, 3),
                'cpc_decay': round(cpc_decay, 3),
                'recent_ctr': round(recent['ctr'], 3),
                'baseline_ctr': round(base['ctr'], 3),
                'recent_cpc': None if recent.get('cpc') is None else round(recent['cpc'], 3),
                'baseline_cpc': None if base.get('cpc') is None else round(base['cpc'], 3),
                'recent_frequency_avg': None if recent.get('frequency_avg') is None else round(recent['frequency_avg'], 3),
                'recent_spend': round(recent['spend'], 2),
                'recent_impressions': int(recent['impressions']),
                'confidence': _bucket_confidence(len(days), recent['spend'], recent['impressions']),
            })

        out.sort(key=lambda x: x.get('fatigue_score', 0), reverse=True)
        return out[:limit]

    return {
        'campaigns': collect('campaign'),
        'ads': collect('ad'),
        'method': 'frequency_pressure_plus_ctr_cpc_decay',
    }


def build_time_efficiency_diagnostics(rows):
    hourly_file = ADS_DIR / 'insights_hourly_latest.csv'
    if hourly_file.exists():
        try:
            with open(hourly_file, newline='', encoding='utf-8') as f:
                hrows = list(csv.DictReader(f))
            by_hour = {}
            for r in hrows:
                hour_key = str(r.get('hour') or r.get('hour_of_day') or r.get('hourly_stats_aggregated_by_advertiser_time_zone') or '').strip()
                if not hour_key:
                    continue
                m = by_hour.setdefault(hour_key, {'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0})
                m['spend'] += num(r.get('spend'))
                m['clicks'] += num(r.get('clicks'))
                m['impressions'] += num(r.get('impressions'))
            points = []
            for hour_key, m in by_hour.items():
                ctr = (m['clicks'] / m['impressions'] * 100) if m['impressions'] > 0 else 0.0
                cpc = (m['spend'] / m['clicks']) if m['clicks'] > 0 else None
                score = (ctr * 8) - ((cpc or 1.5) * 5)
                points.append({'bucket': hour_key, 'spend': round(m['spend'], 2), 'ctr': round(ctr, 3), 'cpc': None if cpc is None else round(cpc, 3), 'efficiency_score': round(score, 3)})
            points.sort(key=lambda x: x['efficiency_score'], reverse=True)
            return {
                'source': 'hourly',
                'confidence': 'high' if len(points) >= 12 else 'med',
                'best_windows': points[:5],
                'worst_windows': sorted(points, key=lambda x: x['efficiency_score'])[:5],
                'notes': 'Using hourly breakdown from insights_hourly_latest.csv',
            }
        except Exception:
            pass

    by_dow = {}
    for r in rows:
        d = (r.get('date_start') or '').strip()
        if not d:
            continue
        try:
            dow = datetime.fromisoformat(d).strftime('%a')
        except Exception:
            continue
        m = by_dow.setdefault(dow, {'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0, 'days': set()})
        m['spend'] += num(r.get('spend'))
        m['clicks'] += num(r.get('clicks'))
        m['impressions'] += num(r.get('impressions'))
        m['days'].add(d)

    points = []
    for dow, m in by_dow.items():
        ctr = (m['clicks'] / m['impressions'] * 100) if m['impressions'] > 0 else 0.0
        cpc = (m['spend'] / m['clicks']) if m['clicks'] > 0 else None
        score = (ctr * 8) - ((cpc or 1.5) * 5)
        points.append({
            'bucket': dow,
            'spend': round(m['spend'], 2),
            'ctr': round(ctr, 3),
            'cpc': None if cpc is None else round(cpc, 3),
            'efficiency_score': round(score, 3),
            'sample_days': len(m['days']),
        })

    points.sort(key=lambda x: x['efficiency_score'], reverse=True)
    coverage = sum(x.get('sample_days', 0) for x in points)
    return {
        'source': 'day_of_week_fallback',
        'confidence': 'med' if coverage >= 14 else 'low',
        'best_windows': points[:3],
        'worst_windows': sorted(points, key=lambda x: x['efficiency_score'])[:3],
        'notes': 'Hourly data unavailable; fallback uses day-of-week efficiency.',
    }


def build_attribution_confidence(summary, data_health, campaigns):
    outbound = int(sum(num(c.get('outbound_clicks')) for c in campaigns))
    lpv = int(sum(num(c.get('landing_page_views')) for c in campaigns))
    follows = int(num(summary.get('total_follows')))
    pull_age = data_health.get('last_pull_age_min')

    factors = []
    penalty = 0.0

    lpv_rate = (lpv / outbound) if outbound > 0 else 0.0
    lpv_penalty = 20.0 if outbound >= 50 and lpv_rate < 0.45 else (10.0 if outbound >= 20 and lpv_rate < 0.6 else 0.0)
    penalty += lpv_penalty
    factors.append({'factor': 'LPV coverage of outbound clicks', 'value': round(lpv_rate * 100, 1), 'penalty': round(lpv_penalty, 1)})

    event_penalty = 0.0 if follows > 0 else 18.0
    penalty += event_penalty
    factors.append({'factor': 'Direct follow events', 'value': follows, 'penalty': round(event_penalty, 1)})

    stale_penalty = 0.0
    if pull_age is not None and pull_age > 180:
        stale_penalty = 20.0
    elif pull_age is not None and pull_age > 90:
        stale_penalty = 10.0
    penalty += stale_penalty
    factors.append({'factor': 'Data freshness (minutes)', 'value': pull_age, 'penalty': round(stale_penalty, 1)})

    sample_penalty = 10.0 if outbound < 25 else (5.0 if outbound < 60 else 0.0)
    penalty += sample_penalty
    factors.append({'factor': 'Attribution sample size (outbound clicks)', 'value': outbound, 'penalty': round(sample_penalty, 1)})

    score = max(0.0, min(100.0, 100.0 - penalty))
    if score >= 80:
        band = 'high'
    elif score >= 60:
        band = 'med'
    else:
        band = 'low'

    return {
        'score': round(score, 1),
        'band': band,
        'factors': factors,
        'penalty_total': round(penalty, 1),
        'summary': f'Attribution confidence {band.upper()} ({round(score,1)}/100).',
    }


def build_anomaly_diagnostics(campaign_daily_rows, limit=12):
    by_campaign = {}
    for r in campaign_daily_rows:
        cid = (r.get('campaign_id') or r.get('campaign_name') or '').strip()
        if not cid:
            continue
        by_campaign.setdefault(cid, {'name': r.get('campaign_name') or cid, 'rows': []})['rows'].append(r)

    anomalies = []
    for info in by_campaign.values():
        rows = sorted(info['rows'], key=lambda x: x.get('date') or '')
        if len(rows) < 8:
            continue
        baseline = rows[:-2]
        recent = rows[-2:]

        def stats(metric):
            vals = [num(x.get(metric)) for x in baseline if num(x.get('impressions')) >= 600 and num(x.get('spend')) >= 4]
            if len(vals) < 4:
                return None, None
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / max(1, (len(vals) - 1))
            std = var ** 0.5
            return mean, std

        for metric in ('ctr', 'cpc', 'spend'):
            mean, std = stats(metric)
            if mean is None:
                continue
            for rec in recent:
                if num(rec.get('impressions')) < 800 and metric != 'spend':
                    continue
                val = num(rec.get(metric))
                if metric == 'cpc' and rec.get('cpc') is None:
                    continue
                if std <= 1e-6:
                    continue
                z = (val - mean) / std
                if abs(z) < 2.4:
                    continue
                direction = 'up' if z > 0 else 'down'
                severity = 'high' if abs(z) >= 3.2 else 'med'
                anomalies.append({
                    'campaign': info['name'],
                    'date': rec.get('date'),
                    'metric': metric,
                    'value': round(val, 3),
                    'baseline_mean': round(mean, 3),
                    'z_score': round(z, 2),
                    'direction': direction,
                    'severity': severity,
                    'confidence': _bucket_confidence(len(rows), num(rec.get('spend')), num(rec.get('impressions'))),
                })

    anomalies.sort(key=lambda x: abs(x.get('z_score') or 0), reverse=True)
    return {
        'count': len(anomalies),
        'items': anomalies[:limit],
        'noise_guards': {
            'min_impressions': 800,
            'min_spend': 4,
            'min_baseline_points': 4,
            'z_threshold': 2.4,
        }
    }


def build_action_recommendations(campaigns, creative_fatigue=None, anomalies=None, time_efficiency=None, attribution=None):
    fatigue_by_campaign = {}
    for x in (creative_fatigue or {}).get('campaigns', []):
        fatigue_by_campaign[str(x.get('campaign_id') or x.get('campaign') or '')] = x

    anomaly_by_campaign = {}
    for a in (anomalies or {}).get('items', []):
        anomaly_by_campaign.setdefault(str(a.get('campaign') or ''), []).append(a)

    out = []
    for c in campaigns:
        freq = c.get('frequency_avg')
        cpc = c.get('cpc')
        ctr = c.get('ctr') or 0
        spend = c.get('spend') or 0
        cid = str(c.get('campaign_id') or c.get('campaign') or '')

        fatigue = fatigue_by_campaign.get(cid) or fatigue_by_campaign.get(str(c.get('campaign') or ''))
        fatigue_score = num((fatigue or {}).get('fatigue_score'))
        related_anoms = anomaly_by_campaign.get(str(c.get('campaign') or ''), [])

        tag = 'Hold'
        reason = 'Baseline monitoring'
        confidence = 'low'
        priority = 'normal'
        diagnostic_tags = []
        expected_impact = 'low'

        if spend >= 15 and cpc is not None and ctr >= 2.5 and cpc <= 0.35 and (freq is None or freq < 1.8):
            tag = 'Scale'; reason = 'Strong CTR + low CPC with acceptable frequency'; confidence = 'high'; expected_impact = 'high'; diagnostic_tags.append('efficiency')
        elif spend >= 10 and (cpc is None or cpc > 0.8 or ctr < 1.0):
            tag = 'Cut'; reason = 'Weak efficiency after spend'; confidence = 'high'; priority = 'high'; diagnostic_tags.append('inefficiency')
        elif spend >= 8 and freq is not None and freq >= 1.8:
            tag = 'Retest'; reason = 'Fatigue risk; rotate creatives/audiences'; confidence = 'med'; diagnostic_tags.append('frequency')
        elif spend < 8:
            tag = 'Hold'; reason = 'Insufficient spend for confident action'; confidence = 'low'

        if fatigue_score >= 65:
            tag = 'Retest'
            reason = f"Creative fatigue score {fatigue_score:.1f} is elevated (refresh hooks and broaden audience)."
            priority = 'high' if fatigue_score >= 78 else priority
            diagnostic_tags.append('fatigue')

        if related_anoms:
            worst = sorted(related_anoms, key=lambda x: abs(num(x.get('z_score'))), reverse=True)[0]
            diagnostic_tags.append(f"anomaly_{worst.get('metric')}")
            if worst.get('metric') == 'ctr' and worst.get('direction') == 'down':
                tag = 'Retest'
                reason = f"CTR anomaly ({worst.get('z_score')}σ) detected; test new creative angle immediately."
                priority = 'high'
            elif worst.get('metric') == 'cpc' and worst.get('direction') == 'up':
                tag = 'Cut'
                reason = f"CPC spike anomaly ({worst.get('z_score')}σ); cap budget until efficiency normalizes."
                priority = 'high'

        if (attribution or {}).get('band') == 'low':
            confidence = 'low'
            diagnostic_tags.append('low_attribution_confidence')

        out.append({
            'campaign': c.get('campaign'),
            'campaign_id': c.get('campaign_id'),
            'tag': tag,
            'reason': reason,
            'confidence': confidence,
            'priority': priority,
            'diagnostic_tags': diagnostic_tags,
            'expected_impact': expected_impact,
            'fatigue_score': None if fatigue is None else fatigue.get('fatigue_score'),
        })

    rank = {'Scale': 0, 'Retest': 1, 'Hold': 2, 'Cut': 3}
    pr = {'high': 0, 'normal': 1, 'low': 2}
    out.sort(key=lambda x: (pr.get(x.get('priority') or 'normal', 9), rank.get(x['tag'], 9)))
    return out


def build_pacing(spend_series, target_daily=60.0):
    if not spend_series:
        return {'target_daily': target_daily, 'today_spend': 0.0, 'status': 'unknown'}
    today_spend = num(spend_series[-1].get('spend'))
    now_pt = datetime.now(ZoneInfo('America/Los_Angeles'))
    elapsed = max(1/24, (now_pt.hour + now_pt.minute / 60) / 24)
    expected = target_daily * elapsed
    ratio = (today_spend / expected) if expected > 0 else 0
    if ratio > 1.2:
        status = 'too_fast'
    elif ratio < 0.7:
        status = 'too_slow'
    else:
        status = 'on_track'
    return {
        'target_daily': target_daily,
        'today_spend': round(today_spend, 2),
        'expected_by_now': round(expected, 2),
        'pace_ratio': round(ratio, 3),
        'status': status,
    }


def build_data_health(summary, campaigns, rows):
    status = 'ok'
    reason = 'Fresh pull and non-empty campaign data'
    pulled_at = summary.get('pulled_at')
    age_min = None
    if pulled_at:
        try:
            dt = datetime.fromisoformat(str(pulled_at).replace('Z', '+00:00'))
            age_min = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60
        except Exception:
            pass

    if not rows or not campaigns:
        status = 'degraded'; reason = 'No insights rows or no campaign rollup'
    elif age_min is not None and age_min > 180:
        status = 'stale'; reason = f'Last successful pull is {int(age_min)} min old'

    attribution_confidence = 'direct'
    if int(sum(num(c.get('landing_page_views')) for c in campaigns)) == 0:
        attribution_confidence = 'estimated'
    if status in ('stale', 'degraded'):
        attribution_confidence = 'stale'

    return {
        'status': status,
        'reason': reason,
        'last_pull_age_min': None if age_min is None else round(age_min, 1),
        'attribution_confidence': attribution_confidence,
    }


def read_follower_city_rows(limit=200):
    p = ADS_DIR / 'follower_demographics_city_latest.json'
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text())
        rows = d.get('rows') or []

        # Baseline = yesterday close (latest snapshot from previous PT day)
        baseline_map = {}
        hist_dir = ADS_DIR / 'follower_demographics_city_history'
        if hist_dir.exists():
            now_pt = datetime.now(ZoneInfo('America/Los_Angeles'))
            today_pt = now_pt.date().isoformat()
            candidates = sorted(hist_dir.glob('follower_demographics_city_*.json'))
            prev_day_files = []
            for f in candidates:
                try:
                    js = json.loads(f.read_text())
                    ts = js.get('updated_at', '')
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(ZoneInfo('America/Los_Angeles'))
                    if dt.date().isoformat() < today_pt:
                        prev_day_files.append((dt, js))
                except Exception:
                    continue
            if prev_day_files:
                prev_day_files.sort(key=lambda x: x[0])
                yclose_js = prev_day_files[-1][1]
                for r in yclose_js.get('rows') or []:
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
            gained = cur - base
            out.append({'city': city, 'followers': cur, 'gained_today': gained})
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


def build_geo_efficiency(region_breakdown, limit=12):
    out = []
    for r in region_breakdown[:limit]:
        ctr = num(r.get('ctr'))
        cpc = num(r.get('cpc')) if r.get('cpc') is not None else 1.5
        spend = num(r.get('spend'))
        score = (ctr * 12) - (cpc * 20) - (spend * 0.08)
        out.append({
            'region': r.get('label'),
            'spend': spend,
            'ctr': ctr,
            'cpc': r.get('cpc'),
            'score': round(score, 3),
        })
    out.sort(key=lambda x: x['score'], reverse=True)
    return out


def build_optimization(rows, campaigns, summary, followers_daily):
    now_pt = datetime.now(ZoneInfo('America/Los_Angeles'))

    total_clicks = sum(num(r.get('clicks')) for r in rows)
    total_outbound = sum(num(r.get('outbound_clicks')) for r in rows)
    total_impr = sum(num(r.get('impressions')) for r in rows)
    total_reach = sum(num(r.get('reach')) for r in rows)
    avg_freq = (total_impr / total_reach) if total_reach > 0 else None
    first_time_ratio = (total_reach / total_impr) if total_impr > 0 else None

    # Pacing vs $60/day target
    day_target = 60.0
    today_spend = 0.0
    for r in rows:
        if (r.get('date_start') or '').strip() == now_pt.date().isoformat():
            today_spend += num(r.get('spend'))
    hour_progress = max(1e-6, (now_pt.hour + now_pt.minute / 60) / 24)
    expected_spend_now = day_target * hour_progress
    pace_ratio = (today_spend / expected_spend_now) if expected_spend_now > 0 else None

    # Geo efficiency score (region-level)
    region_rows = read_csv_rows('insights_region_latest.csv')
    geo = {}
    for r in region_rows:
        region = (r.get('region') or 'Unknown').strip() or 'Unknown'
        g = geo.setdefault(region, {'spend': 0.0, 'clicks': 0.0, 'impressions': 0.0})
        g['spend'] += num(r.get('spend'))
        g['clicks'] += num(r.get('clicks'))
        g['impressions'] += num(r.get('impressions'))

    geo_scores = []
    for region, g in geo.items():
        clicks = g['clicks']
        impr = g['impressions']
        spend = g['spend']
        ctr = (clicks / impr) * 100 if impr > 0 else 0
        cpc = (spend / clicks) if clicks > 0 else None
        # higher is better
        score = ((ctr * 4) - ((cpc or 1.5) * 3))
        geo_scores.append({'region': region, 'score': round(score, 3), 'ctr': round(ctr, 3), 'cpc': None if cpc is None else round(cpc, 3), 'spend': round(spend, 2)})
    geo_scores.sort(key=lambda x: x['score'], reverse=True)

    # Action engine from campaign performance
    actions = []
    for c in campaigns:
        cpc = c.get('cpc')
        ctr = c.get('ctr') or 0
        spend = c.get('spend') or 0
        tag = 'hold'
        why = 'stable'
        if cpc is not None and spend >= 5:
            if cpc < 0.35 and ctr >= 1.2:
                tag, why = 'scale', 'strong efficiency'
            elif cpc > 0.85 or ctr < 0.7:
                tag, why = 'cut', 'inefficient traffic'
            elif 0.35 <= cpc <= 0.6:
                tag, why = 'retest', 'middle efficiency'
        actions.append({'campaign': c.get('campaign'), 'tag': tag, 'why': why, 'cpc': cpc, 'ctr': ctr, 'spend': spend})

    # Attribution confidence
    follows = num(summary.get('total_follows'))
    attribution = {
        'status': 'direct' if follows > 0 else 'estimated',
        'reason': 'Meta follow events available' if follows > 0 else 'Using blended model (spend + follower deltas)'
    }

    # Token/data health
    pulled_at = summary.get('pulled_at')
    stale_minutes = None
    if pulled_at:
        try:
            dt = datetime.fromisoformat(str(pulled_at).replace('Z', '+00:00'))
            stale_minutes = int((datetime.now(timezone.utc) - dt).total_seconds() / 60)
        except Exception:
            pass
    data_health = {
        'stale_minutes': stale_minutes,
        'status': 'stale' if stale_minutes is not None and stale_minutes > 120 else 'ok',
    }

    # Creative leaderboard (ad-level hook proxy)
    leaderboard = []
    for c in campaigns:
        for s in c.get('adsets', []):
            for a in s.get('ads', []):
                ctr = a.get('ctr') or 0
                cpc = a.get('cpc') if a.get('cpc') is not None else 9.99
                spend = a.get('spend') or 0
                hook_score = (ctr * 10) - cpc
                leaderboard.append({'ad': a.get('ad'), 'campaign': c.get('campaign'), 'ctr': ctr, 'cpc': a.get('cpc'), 'spend': spend, 'hook_score': round(hook_score, 3)})
    leaderboard.sort(key=lambda x: x['hook_score'], reverse=True)

    return {
        'frequency': {'avg_frequency': None if avg_freq is None else round(avg_freq, 3), 'first_time_impression_ratio': None if first_time_ratio is None else round(first_time_ratio, 3)},
        'traffic_quality': {'outbound_clicks': int(total_outbound), 'clicks': int(total_clicks), 'outbound_click_rate': round((total_outbound / total_clicks), 3) if total_clicks > 0 else None},
        'pacing': {'day_target': day_target, 'today_spend': round(today_spend, 2), 'expected_spend_now': round(expected_spend_now, 2), 'pace_ratio': None if pace_ratio is None else round(pace_ratio, 3)},
        'geo_efficiency': geo_scores[:12],
        'actions': actions[:20],
        'attribution': attribution,
        'data_health': data_health,
        'creative_leaderboard': leaderboard[:20],
    }


def main():
    summary = read_summary()
    meta = read_meta_config()
    rows = read_insights_rows()
    campaigns = aggregate_hierarchy(rows)
    manual_spend_override = read_manual_intraday_spend_override()
    if manual_spend_override:
        today_pt = datetime.now(ZoneInfo('America/Los_Angeles')).date().isoformat()
        until = manual_spend_override.get('apply_until_local_date')
        if until and today_pt > str(until):
            manual_spend_override = None

    if manual_spend_override:
        cmap = manual_spend_override.get('campaign_spend', {})
        for c in campaigns:
            n = c.get('campaign')
            if n in cmap:
                c['spend'] = round(num(cmap[n]), 2)
                clicks = num(c.get('clicks'))
                impr = num(c.get('impressions'))
                c['cpc'] = round(c['spend'] / clicks, 3) if clicks > 0 else None
                c['cpm'] = round((c['spend'] / impr) * 1000, 3) if impr > 0 else c.get('cpm')
    followers = read_followers_series()
    spend_series = build_spend_series(rows)
    campaign_daily_rows = build_campaign_daily(rows)
    live_followers = read_live_followers_stats()

    placement_rows = read_csv_rows('insights_placement_latest.csv')
    age_gender_rows = read_csv_rows('insights_age_gender_latest.csv')
    device_rows = read_csv_rows('insights_device_latest.csv')
    region_rows = read_csv_rows('insights_region_latest.csv')
    follower_city_rows = read_follower_city_rows()

    # Keep full follower history for trend continuity/backfill; ad insights can
    # have a different coverage window.
    followers_daily = follower_daily_series(followers)
    pacing = build_pacing(spend_series, target_daily=60.0)

    total_outbound_clicks = int(sum(num(c.get('outbound_clicks')) for c in campaigns))
    total_landing_page_views = int(sum(num(c.get('landing_page_views')) for c in campaigns))
    total_link_clicks = int(sum(num(c.get('link_clicks')) for c in campaigns))
    total_spend = num(summary.get('total_spend'))
    today_pt = datetime.now(ZoneInfo('America/Los_Angeles')).date().isoformat()

    # Intraday fallback: if today's spend row is missing, estimate from current campaign rollup.
    found_today = any(r.get('date') == today_pt for r in spend_series)
    intraday_estimated = False
    if not found_today:
        # Estimate strictly from today's raw rows (avoid multi-day campaign rollup inflation)
        est_today = round(sum(num(r.get('spend')) for r in rows if (r.get('date_start') or '') == today_pt), 2)
        if est_today > 0:
            spend_series.append({'date': today_pt, 'spend': est_today})
            spend_series.sort(key=lambda x: x.get('date', ''))
            intraday_estimated = True

    if manual_spend_override and manual_spend_override.get('total_spend') is not None:
        total_spend = round(num(manual_spend_override.get('total_spend')), 2)
        # Keep UI 'Daily Spend' aligned by overriding today's spend_series point.
        found = False
        for r in spend_series:
            if r.get('date') == today_pt:
                r['spend'] = total_spend
                found = True
                break
        if not found:
            spend_series.append({'date': today_pt, 'spend': total_spend})
            spend_series.sort(key=lambda x: x.get('date', ''))

    breakdown_placement = top_breakdown(placement_rows, ['publisher_platform', 'platform_position'])
    breakdown_age_gender = top_breakdown(age_gender_rows, ['age', 'gender'])
    breakdown_device = top_breakdown(device_rows, ['device_platform'])
    breakdown_region = top_breakdown(region_rows, ['region'])
    geo_efficiency = build_geo_efficiency(breakdown_region)
    data_health = build_data_health(summary, campaigns, rows)

    creative_fatigue = build_creative_fatigue_diagnostics(rows)
    time_efficiency = build_time_efficiency_diagnostics(rows)
    attribution_confidence = build_attribution_confidence(summary, data_health, campaigns)
    anomalies = build_anomaly_diagnostics(campaign_daily_rows)
    action_recommendations = build_action_recommendations(
        campaigns,
        creative_fatigue=creative_fatigue,
        anomalies=anomalies,
        time_efficiency=time_efficiency,
        attribution=attribution_confidence,
    )

    payload = {
        'updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'summary': {
            'ad_account_id': meta.get('ad_account_id'),
            'total_spend': total_spend,
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'total_follows': summary.get('total_follows'),
            'blended_cost_per_follow': summary.get('blended_cost_per_follow'),
            'since': summary.get('since'),
            'until': summary.get('until'),
            'current_followers_live': live_followers.get('current_followers_live'),
            'daily_gain_live': live_followers.get('daily_gain_live'),
            'baseline_followers': live_followers.get('baseline_followers'),
            'total_link_clicks': total_link_clicks,
            'total_outbound_clicks': total_outbound_clicks,
            'total_landing_page_views': total_landing_page_views,
            'cost_per_landing_page_view': round(total_spend / total_landing_page_views, 3) if total_landing_page_views > 0 else None,
            'lpv_per_outbound_click_rate': round((total_landing_page_views / total_outbound_clicks) * 100, 3) if total_outbound_clicks > 0 else None,
            'manual_intraday_spend_override_note': ('intraday spend manually overridden from data/manual_intraday_spend.json' if manual_spend_override else None),
            'manual_intraday_spend_as_of_local': (manual_spend_override.get('as_of_local') if manual_spend_override else None),
            'intraday_spend_estimated': intraday_estimated,
        },
        'campaigns': campaigns,
        'top_campaigns': campaigns[:10],
        'campaign_daily': campaign_daily_rows,
        'followers_series': followers,
        'followers_daily_series': followers_daily,
        'spend_series': spend_series,
        'insights': build_insights(summary, campaigns, followers_daily, spend_series, action_recommendations, data_health),
        'recommendations': action_recommendations,
        'pacing': pacing,
        'geo_efficiency': geo_efficiency,
        'data_health': data_health,
        'diagnostics': {
            'creative_fatigue': creative_fatigue,
            'time_efficiency': time_efficiency,
            'attribution_confidence': attribution_confidence,
            'anomalies': anomalies,
            'recommendation_context': {
                'top_fatigue_campaigns': creative_fatigue.get('campaigns', [])[:3],
                'anomaly_count': anomalies.get('count', 0),
                'time_efficiency_source': time_efficiency.get('source'),
                'attribution_band': attribution_confidence.get('band'),
            },
        },
        'breakdowns': {
            'placement': breakdown_placement,
            'age_gender': breakdown_age_gender,
            'device': breakdown_device,
            'region': breakdown_region,
        },
        'follower_demographics': {
            'city': follower_city_rows,
        },
        'optimization': build_optimization(rows, campaigns, summary, followers_daily),
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
