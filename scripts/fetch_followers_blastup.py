#!/usr/bin/env python3
import csv
import json
import re
from datetime import date
from pathlib import Path
from http.cookiejar import CookieJar
from urllib.request import Request, build_opener, HTTPCookieProcessor

USERNAME = "thesocial.study"
PAGE_URL = f"https://blastup.com/instagram-follower-count?{USERNAME}"
POST_URL = "https://blastup.com/instagram-follower-count"
WORKSPACE = Path('/Users/ericsysclaw/.openclaw/workspace')
FOLLOWERS_CSV = WORKSPACE / 'exports' / 'meta-ads' / 'followers_daily.csv'


def http_get(opener, url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with opener.open(req, timeout=30) as r:
        return r.read().decode('utf-8', errors='ignore')


def http_post_json(opener, url: str, payload: dict) -> dict:
    body = json.dumps(payload).encode('utf-8')
    req = Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://blastup.com",
            "Referer": PAGE_URL,
        },
        method='POST'
    )
    with opener.open(req, timeout=30) as r:
        return json.loads(r.read().decode('utf-8', errors='ignore'))


def parse_token(html: str) -> str:
    # token appears in window.__config or hidden input
    patterns = [
        r'window\.__config\s*=\s*\{[^}]*token\s*:\s*"([^"]+)"',
        r'name="_token"\s+value="([^"]+)"',
        r'"_token"\s*:\s*"([^"]+)"',
    ]
    for p in patterns:
        m = re.search(p, html, re.S)
        if m:
            return m.group(1)
    raise RuntimeError('Could not locate blastup token on page')


def upsert_followers(total: int):
    FOLLOWERS_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    if FOLLOWERS_CSV.exists():
        with open(FOLLOWERS_CSV, newline='', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))

    today = date.today().isoformat()
    updated = False
    for r in rows:
        if r.get('date') == today:
            r['followers_total'] = str(total)
            updated = True
            break

    if not updated:
        rows.append({'date': today, 'followers_total': str(total)})

    with open(FOLLOWERS_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['date', 'followers_total'])
        w.writeheader()
        for r in sorted(rows, key=lambda x: x['date']):
            w.writerow(r)


def main():
    cj = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cj))

    html = http_get(opener, PAGE_URL)
    token = parse_token(html)
    data = http_post_json(opener, POST_URL, {'_token': token, 'username': USERNAME})

    if not data.get('success'):
        raise RuntimeError(f"Blastup response unsuccessful: {data}")

    followers_raw = str(data.get('followers', '')).replace(',', '').strip()
    followers = int(float(followers_raw))
    upsert_followers(followers)
    print(json.dumps({'ok': True, 'username': USERNAME, 'followers': followers, 'csv': str(FOLLOWERS_CSV)}, indent=2))


if __name__ == '__main__':
    main()
