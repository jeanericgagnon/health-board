#!/usr/bin/env python3
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from http.cookiejar import CookieJar
from urllib.request import Request, build_opener, HTTPCookieProcessor

REPO = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO / "data" / "competitor_handles.json"
LATEST_PATH = REPO / "data" / "competitor_followers_latest.json"
HISTORY_PATH = REPO / "data" / "competitor_followers_history.jsonl"

BASE_URL = "https://blastup.com/instagram-follower-count"


def http_get(opener, url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with opener.open(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="ignore")


def http_post_json(opener, url: str, payload: dict, referer: str) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://blastup.com",
            "Referer": referer,
        },
        method="POST",
    )
    with opener.open(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))


def parse_token(html: str) -> str:
    patterns = [
        r'window\.__config\s*=\s*\{[^}]*token\s*:\s*"([^"]+)"',
        r'name="_token"\s+value="([^"]+)"',
        r'"_token"\s*:\s*"([^"]+)"',
    ]
    for p in patterns:
        m = re.search(p, html, re.S)
        if m:
            return m.group(1)
    raise RuntimeError("Could not locate blastup token on page")


def load_history() -> list:
    if not HISTORY_PATH.exists():
        return []
    rows = []
    for line in HISTORY_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows


def baseline_by_days(history_rows: list, username: str, now_utc: datetime, days: int):
    cutoff = now_utc - timedelta(days=days)
    candidates = [
        r for r in history_rows
        if (r.get("username") == username and r.get("pulled_at_utc"))
    ]
    best = None
    for r in candidates:
        try:
            ts = datetime.fromisoformat(str(r["pulled_at_utc"]).replace("Z", "+00:00"))
        except Exception:
            continue
        if ts <= cutoff and (best is None or ts > best[0]):
            best = (ts, r)
    return best[1] if best else None


def main():
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Missing config: {CONFIG_PATH}")

    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    handles = config.get("handles") or []
    if not handles:
        raise RuntimeError("competitor_handles.json has no handles")

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat().replace("+00:00", "Z")

    cj = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cj))

    history = load_history()
    out_rows = []

    for h in handles:
        username = (h.get("username") or "").strip().lstrip("@")
        if not username:
            continue

        page_url = f"{BASE_URL}?{username}"
        try:
            html = http_get(opener, page_url)
            token = parse_token(html)
            data = http_post_json(opener, BASE_URL, {"_token": token, "username": username}, referer=page_url)
            if not data.get("success"):
                raise RuntimeError(f"unsuccessful response: {data}")
            followers = int(float(str(data.get("followers", "0")).replace(",", "")))
            err = None
        except Exception as e:
            followers = None
            err = str(e)

        windows = [1, 3, 7, 30]
        baselines = {}
        deltas = {}
        for w in windows:
            base = baseline_by_days(history, username, now_utc, w)
            base_val = int(base["followers"]) if base and base.get("followers") is not None else None
            baselines[str(w)] = base_val
            deltas[str(w)] = (followers - base_val) if (followers is not None and base_val is not None) else None

        row = {
            "label": h.get("label") or username,
            "username": username,
            "url": h.get("url") or f"https://www.instagram.com/{username}",
            "market": h.get("market"),
            "followers": followers,
            "baseline_24h": baselines.get("1"),
            "delta_24h": deltas.get("1"),
            "baselines": baselines,
            "deltas": deltas,
            "pulled_at_utc": now_iso,
            "ok": err is None,
            "error": err,
        }
        out_rows.append(row)

    # append history for successful pulls only
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        for r in out_rows:
            if r["followers"] is None:
                continue
            f.write(json.dumps({"pulled_at_utc": now_iso, "username": r["username"], "followers": r["followers"]}) + "\n")

    payload = {
        "updated_at": now_iso,
        "rows": sorted(out_rows, key=lambda x: (x["followers"] is None, -(x["followers"] or 0))),
    }
    LATEST_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "updated_at": now_iso, "count": len(out_rows), "out": str(LATEST_PATH)}, indent=2))


if __name__ == "__main__":
    main()
