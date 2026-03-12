#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from pathlib import Path

OUT = Path('/Users/ericsysclaw/.openclaw/workspace/health-board/data/manual_intraday_spend.json')


def parse_item(s):
    # format: "campaign=amount"
    if '=' not in s:
        raise ValueError(f'Invalid item: {s}')
    k, v = s.split('=', 1)
    return k.strip(), float(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--as-of-local', required=True, help='ISO local timestamp, e.g. 2026-03-12T22:30:00-07:00')
    ap.add_argument('--apply-until-local-date', required=True, help='YYYY-MM-DD')
    ap.add_argument('--campaign', action='append', default=[], help='campaign=amount (repeat)')
    ap.add_argument('--total', type=float, default=None)
    ap.add_argument('--source', default='manual_user_input')
    args = ap.parse_args()

    # validate timestamp/date
    datetime.fromisoformat(args.as_of_local)
    datetime.fromisoformat(args.apply_until_local_date + 'T00:00:00')

    campaign_spend = {}
    for c in args.campaign:
        k, v = parse_item(c)
        campaign_spend[k] = round(v, 2)

    total = round(args.total if args.total is not None else sum(campaign_spend.values()), 2)

    payload = {
        'as_of_local': args.as_of_local,
        'apply_until_local_date': args.apply_until_local_date,
        'source': args.source,
        'campaign_spend': campaign_spend,
        'total_spend': total,
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
