---
phase: "00"
plan: "04"
subsystem: operations
tags: [stability, baseline, monitoring, aqicn]
duration: 7 days (monitoring phase)
completed: 2026-04-01 (DAG changes committed; monitoring runs 2026-04-02 onwards)
---

## Summary

OpenAQ tasks disabled and AQICN-only stability baseline initiated. `dag_ingest_hourly` now runs AQICN + forecast only.

## What Was Built

- **`airflow/dags/dag_ingest_hourly.py`** — OpenAQ measurement task commented out:
  - `run_openaq_measurements_ingestion()` task definition commented
  - `openaq` task instantiation commented
  - Dependency chain changed to `[aqicn, forecast] >> completion`
- **`.planning/codebase/BASELINE-METRICS.md`** — monitoring template with 7-day table

## Monitoring Instructions

1. Start Docker Compose: `docker compose up -d`
2. Restart Airflow: `docker compose restart airflow-scheduler airflow-dag-processor`
3. Wait for first DAG run (hourly at minute 0)
4. Check: `docker compose logs airflow-scheduler --since 1h | grep -i error`
5. Fill in `.planning/codebase/BASELINE-METRICS.md` daily (same time each day)
6. After 7 days: run Plan 04 task `baseline-results`

## Re-enabling OpenAQ

After baseline complete and Phase 1 begins:
1. Uncomment `run_openaq_measurements_ingestion()` definition
2. Uncomment `openaq = run_openaq_measurements_ingestion()`
3. Restore: `check_clickhouse >> metadata >> [openaq, aqicn, forecast] >> completion`
4. Restart Airflow

## Files Modified

- `airflow/dags/dag_ingest_hourly.py` (OpenAQ disabled)

## Commits

- `e5deec8` — feat(00-baseline): disable OpenAQ task for AQICN-only stability baseline
- `3d20512` — feat(00-baseline): add BASELINE-METRICS.md template

## Next

Phase 1 — Multi-Source Ingestion (begins after 7-day baseline)
