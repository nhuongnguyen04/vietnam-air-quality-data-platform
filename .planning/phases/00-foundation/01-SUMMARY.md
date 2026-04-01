---
phase: "00"
plan: "01"
subsystem: foundation
tags: [audit, openaq, clickhouse, dbt, airflow]
duration: ~15 min
completed: 2026-04-01
---

## Summary

Baseline codebase audit completed. Documented all OpenAQ-specific hardcoded names, ClickHouse table schemas, and DAG task dependencies needed for Phase 1 planning.

## What Was Built

- **`.planning/codebase/AUDIT.md`** — comprehensive audit document (9.9KB) covering:
  - 7 ClickHouse tables with engines, ORDER BY keys, dedup strategy
  - 55 OpenAQ references in dbt models (staging + intermediate layers)
  - OpenAQ Python job references (table names, API endpoints, config)
  - 4 Airflow DAG dependency diagrams
  - DB name consistency check across all files
- **Fixed** `dag_ingest_hourly.py`: DB fallback `airquality` → `air_quality`

## Key Decisions

- DB is consistently `air_quality` — no rename needed (D-09/D-10)
- OpenAQ Python-side dedup is a known risk (CONCERN #7) — fix deferred to Phase 2 Plan 2.4
- Historical backfill has concurrency limit (CONCERN #14) — fix deferred to Phase 2

## Issues Encountered

- `dag_transform.py` still has `'airquality'` fallback bug — documented in AUDIT.md, fix in Phase 2 Plan 2.4

## Files Modified

- `.planning/codebase/AUDIT.md` (created)
- `airflow/dags/dag_ingest_hourly.py` (DB name fallback fix)

## Commits

- `ea7c4a1` — feat(00-audit): add codebase AUDIT.md and fix DB name fallback
