# Health Dashboard Idea Backlog

_Last updated: 2026-03-05_

## Core Direction
- Build a personal health dashboard site on `gaginonricky.com` (likely `health.gaginonricky.com`).
- Use local/advisor flow with Huberman + Attia board insights.
- Keep data always available post-workout (hybrid sync: automatic + quick manual fallback).

## Data Sources (planned)
- Apple Health bridge (shortcut/app bridge)
- WHOOP API (OAuth app)
- Manual daily entries (weight + optional subjective metrics)

## Metrics to Track
- HRV
- Resting HR
- Sleep duration/quality
- Strain/training load
- Swim distance, pace, duration
- Weight
- Daily energy score (1-10)

## Visualization Ideas
- Catalina -> Long Beach distance progress overlay
- 6-month cumulative "Pacific crossing" style distance overlay
- Weekly streak + monthly distance heatmap
- Pace trend + HRV overlay
- Daily readiness/advisory card

## Product UX Ideas
- "Today" card with top action from health advisors
- Last sync timestamp + data freshness indicator
- Fail-safe status if automation misses

## Infra/Hosting Notes
- Domain: Namecheap-managed `gaginonricky.com`
- Candidate hosting: Vercel + subdomain CNAME (`health` -> `cname.vercel-dns.com`)

## Advisory System Notes
- Health board should be consensus-first (not hyper-adversarial).
- Local corpus citations abbreviated in responses.
- Mark unsupported claims as `[uncertain]`.

## Next Steps (when resuming)
1. Finalize ingestion architecture (shortcut-first vs app bridge).
2. Stand up MVP schema + API endpoint(s).
3. Build v1 dashboard (distance overlays + key trends).
4. Wire daily advisory generation on top of metrics.
