# Phase 1 — Autopilot Foundation (Local Only)

Status: Drafted and approved for execution on branch `local-tune-safe`.
Constraint: **No live push/deploy during Phase 1 build + validation.**

## Goal
Build a reliable hourly analysis loop that produces clear, high-confidence optimization decisions without auto-editing ads yet.

## Success Criteria (Definition of Done)
1. Hourly analyzer runs locally (daytime schedule) and writes:
   - `health-board/data/analysis_latest.json`
   - `health-board/data/analysis_brief.txt`
   - `health-board/data/analysis_history.jsonl` (append-only)
2. Analyzer outputs are decision-useful:
   - Top scale / hold / cut / retest candidates
   - Confidence per recommendation
   - Pacing status vs $60/day target
   - Data health + signal coverage diagnostics
3. Safety/quality gates in place:
   - Mark recommendations as limited when conversion signal is missing
   - No destructive/write actions to ad platform in Phase 1
4. Dry-run validation completed:
   - Run analyzer manually at least 3 times
   - Confirm deterministic output schema and stable recommendations

## Scope (Phase 1)
### A) Analysis Engine
Create `health-board/scripts/analyze_kpis.py` to:
- Read `health-board/data/kpi_latest.json`
- Compute recommendation set with confidence + reasons
- Score pacing risk (too_fast / on_track / too_slow)
- Compute decision confidence using data health + signal coverage
- Emit concise action list (max 3 immediate actions)

### B) Outputs
- `analysis_latest.json`: structured machine-readable output
- `analysis_brief.txt`: concise human summary
- `analysis_history.jsonl`: timestamped run log (append)

### C) Scheduling (Local/Internal)
- Add local hourly schedule (6am–10pm PT), offset after data pull.
- Initial mode: dry-run only (no external edit actions).

### D) Alert Rules (Low-noise)
Only surface alert/update when one of the following is true:
- Recommendation state changed materially (e.g., Scale→Cut)
- Pacing moved to too_fast/too_slow
- Data health degraded/stale
- Confidence tier changed

## Non-Goals (Phase 1)
- No automatic ad edits
- No automatic creative publishing
- No live-site UX overhaul

## Phase 1 Validation Checklist
- [ ] `analyze_kpis.py` exists and runs cleanly
- [ ] Output files generated with expected schema
- [ ] Recommendation confidence downgrade works when signals missing
- [ ] Pacing alert logic behaves correctly at different times of day
- [ ] History log appends without corruption
- [ ] No live push/deploy occurred

## Next (Phase 2 Preview)
- Creative metadata join (ad -> creative fields)
- Event-gap diagnostics expansion
- Semi-automatic execution queue with approval gating

