#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / 'data' / 'follower_demographics_city_latest.json'


def main():
    now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    payload = {
        'updated_at': now,
        'source': 'not_configured',
        'status': 'unavailable',
        'reason': 'No follower geo source is currently configured for this system. Total follower count is wired, but city-level follower demographics are not being fetched from any provider yet.',
        'rows': []
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': True, 'status': 'unavailable', 'out': str(OUT)}, indent=2))


if __name__ == '__main__':
    main()
