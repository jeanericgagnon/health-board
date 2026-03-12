#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/Users/ericsysclaw/.openclaw/workspace/health-board')
KPI_PATH = ROOT / 'data' / 'kpi_latest.json'
OUT_JSON = ROOT / 'data' / 'analysis_latest.json'
OUT_BRIEF = ROOT / 'data' / 'analysis_brief.txt'
OUT_HISTORY = ROOT / 'data' / 'analysis_history.jsonl'


def num(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def decision_confidence(data_health, action_coverage):
    status = (data_health or {}).get('status', 'unknown')
    cov = {x.get('action_type'): num(x.get('coverage_pct')) for x in (action_coverage or [])}
    core = max(
        cov.get('offsite_conversion.fb_pixel_purchase', 0.0),
        cov.get('purchase', 0.0),
        cov.get('offsite_conversion.fb_pixel_lead', 0.0),
        cov.get('lead', 0.0),
        cov.get('landing_page_view', 0.0),
        cov.get('omni_landing_page_view', 0.0),
    )
    if status == 'ok' and core >= 30:
        return 'HIGH', core
    if status == 'ok' and core >= 5:
        return 'MEDIUM', core
    return 'LOW', core


def summarize_recommendations(recos):
    scale = [r for r in recos if (r.get('tag') or '').lower() == 'scale']
    cut = [r for r in recos if (r.get('tag') or '').lower() == 'cut']
    retest = [r for r in recos if (r.get('tag') or '').lower() == 'retest']
    hold = [r for r in recos if (r.get('tag') or '').lower() == 'hold']
    return {
        'scale': scale[:3],
        'cut': cut[:3],
        'retest': retest[:3],
        'hold': hold[:3],
    }


def immediate_actions(summary, pacing, recos, confidence):
    actions = []
    pstatus = (pacing or {}).get('status')
    if pstatus == 'too_fast':
        actions.append('Pacing too fast: reduce total daily spend velocity by ~10-20% today.')
    elif pstatus == 'too_slow':
        actions.append('Pacing too slow: increase delivery on top efficient campaigns by ~10-15%.')

    for r in recos.get('scale', [])[:2]:
        actions.append(f"Scale: {r.get('campaign')} ({r.get('reason','')})")
    for r in recos.get('cut', [])[:1]:
        actions.append(f"Cut/Reduce: {r.get('campaign')} ({r.get('reason','')})")

    if confidence == 'LOW':
        actions.append('Confidence LOW: prioritize tracking/event reliability before major budget shifts.')

    # cap to top 3 for low-noise ops
    return actions[:3]


def compute_alert(prev, cur):
    reasons = []
    if not prev:
        reasons.append('initial_run')
    else:
        if (prev.get('decision_confidence') != cur.get('decision_confidence')):
            reasons.append('confidence_changed')
        if ((prev.get('pacing') or {}).get('status') != (cur.get('pacing') or {}).get('status')):
            reasons.append('pacing_status_changed')
        if ((prev.get('data_health') or {}).get('status') != (cur.get('data_health') or {}).get('status')):
            reasons.append('data_health_changed')
        prev_scale = [x.get('campaign') for x in ((prev.get('recommendations') or {}).get('scale') or [])[:2]]
        cur_scale = [x.get('campaign') for x in ((cur.get('recommendations') or {}).get('scale') or [])[:2]]
        prev_cut = [x.get('campaign') for x in ((prev.get('recommendations') or {}).get('cut') or [])[:2]]
        cur_cut = [x.get('campaign') for x in ((cur.get('recommendations') or {}).get('cut') or [])[:2]]
        if prev_scale != cur_scale:
            reasons.append('scale_candidates_changed')
        if prev_cut != cur_cut:
            reasons.append('cut_candidates_changed')

    should_alert = any(r in reasons for r in ['confidence_changed', 'pacing_status_changed', 'data_health_changed', 'scale_candidates_changed', 'cut_candidates_changed'])
    return {'should_alert': should_alert, 'reasons': reasons}


def main():
    if not KPI_PATH.exists():
        raise SystemExit(f'Missing KPI source: {KPI_PATH}')

    d = json.loads(KPI_PATH.read_text())
    summary = d.get('summary', {})
    pacing = d.get('pacing', {})
    data_health = d.get('data_health', {})
    action_coverage = d.get('action_coverage', [])
    recommendations = d.get('recommendations', [])

    conf, core_cov = decision_confidence(data_health, action_coverage)
    recos = summarize_recommendations(recommendations)
    actions = immediate_actions(summary, pacing, recos, conf)

    prev = None
    if OUT_JSON.exists():
        try:
            prev = json.loads(OUT_JSON.read_text())
        except Exception:
            prev = None

    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'source_updated_at': d.get('updated_at'),
        'decision_confidence': conf,
        'core_signal_coverage_pct': round(core_cov, 2),
        'pacing': pacing,
        'data_health': data_health,
        'summary': {
            'total_spend': summary.get('total_spend'),
            'total_clicks': summary.get('total_clicks'),
            'total_impressions': summary.get('total_impressions'),
            'daily_gain_live': summary.get('daily_gain_live'),
            'current_followers_live': summary.get('current_followers_live'),
        },
        'recommendations': recos,
        'immediate_actions': actions,
    }
    payload['alert'] = compute_alert(prev, payload)

    brief = []
    brief.append(f"Decision confidence: {conf} (core coverage {payload['core_signal_coverage_pct']}%)")
    brief.append(f"Pacing: {(pacing or {}).get('status','unknown')} | spend today {(pacing or {}).get('today_spend','N/A')} vs expected {(pacing or {}).get('expected_by_now','N/A')}")
    if recos['scale']:
        brief.append('Scale: ' + ', '.join([x.get('campaign','?') for x in recos['scale']]))
    if recos['cut']:
        brief.append('Cut: ' + ', '.join([x.get('campaign','?') for x in recos['cut']]))
    if actions:
        brief.append('Do now: ' + ' | '.join(actions))
    brief.append(f"Alert: {'YES' if payload.get('alert',{}).get('should_alert') else 'NO'} ({', '.join(payload.get('alert',{}).get('reasons',[])) or 'no_change'})")

    OUT_JSON.write_text(json.dumps(payload, indent=2))
    OUT_BRIEF.write_text('\n'.join(brief) + '\n')
    with OUT_HISTORY.open('a', encoding='utf-8') as f:
        f.write(json.dumps(payload) + '\n')

    print(f'wrote {OUT_JSON}')
    print(f'wrote {OUT_BRIEF}')
    print(f'appended {OUT_HISTORY}')


if __name__ == '__main__':
    main()
