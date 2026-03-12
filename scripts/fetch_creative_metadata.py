#!/usr/bin/env python3
import json
import re
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path('/Users/ericsysclaw/.openclaw/workspace')
HB = ROOT / 'health-board'
KPI = HB / 'data' / 'kpi_latest.json'
OUT = HB / 'data' / 'creative_metadata_latest.json'
ENV = ROOT / 'ads-ops' / '.env.meta'


def read_env(key):
    if not ENV.exists():
        return None
    m = re.search(rf'^{key}=(.*)$', ENV.read_text(), re.M)
    if not m:
        return None
    v = m.group(1).strip()
    return v.strip('"')


def main():
    token = read_env('META_ACCESS_TOKEN')
    ver = read_env('META_API_VERSION') or 'v25.0'
    if not token or not KPI.exists():
        print('skip: missing token or kpi file')
        return

    d = json.loads(KPI.read_text())
    ad_ids = []
    for c in d.get('campaigns', []):
        for s in c.get('adsets', []):
            for a in s.get('ads', []):
                aid = (a.get('ad_id') or '').strip()
                if aid:
                    ad_ids.append(aid)
    ad_ids = sorted(set(ad_ids))[:150]

    out = {'updated_at': d.get('updated_at'), 'rows': []}
    for aid in ad_ids:
        url = f'https://graph.facebook.com/{ver}/{aid}'
        fields = 'id,name,creative{id,name,title,body,call_to_action_type,object_type,object_story_spec}'
        try:
            q = urlencode({'access_token': token, 'fields': fields})
            with urlopen(f"{url}?{q}", timeout=30) as resp:
                raw = resp.read().decode('utf-8')
            j = json.loads(raw)
            cr = j.get('creative') or {}
            row = {
                'ad_id': j.get('id'),
                'ad_name': j.get('name'),
                'creative_id': cr.get('id'),
                'creative_name': cr.get('name'),
                'title': cr.get('title'),
                'body': cr.get('body'),
                'cta': cr.get('call_to_action_type'),
                'object_type': cr.get('object_type'),
            }
            out['rows'].append(row)
        except Exception:
            continue

    OUT.write_text(json.dumps(out, indent=2))
    print(f'wrote {OUT} ({len(out["rows"])} rows)')


if __name__ == '__main__':
    main()
