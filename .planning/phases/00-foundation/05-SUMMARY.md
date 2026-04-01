---
phase: "00"
plan: "05"
subsystem: monitoring
tags: [ingestion-control, clickhouse, airflow, observability]
duration: ~20 min
completed: 2026-04-01
---

## Summary

Ingestion control table created and wired into all DAGs. Every ingestion run now records its metadata to `ingestion_control`, enabling Grafana freshness dashboards (Phase 3.4) and alerting (Phase 5.2).

## What Was Built

- **`scripts/init-clickhouse.sql`** — new `ingestion_control` table:
  - Schema: `source`, `last_run`, `last_success`, `records_ingested`, `lag_seconds`, `error_message`, `updated_at`
  - Engine: `ReplacingMergeTree(updated_at)` — latest row per source returned
  - ORDER BY `source` — one row per source
- **`python_jobs/common/ingestion_control.py`** — Python module:
  - `update_control(source, records_ingested, success, error_message)` function
  - Uses `clickhouse_connect` (already in requirements.txt)
  - Thread-safe inserts
- **`airflow/dags/dag_ingest_hourly.py`** — control task wiring:
  - `update_aqicn_control()` fires after AQICN measurements
  - `update_forecast_control()` fires after AQICN forecast
  - `[aqicn, forecast] >> update_aqicn_control >> update_forecast_control >> completion`
- **`airflow/dags/dag_transform.py`** — control task wiring:
  - `update_transform_control()` fires after dbt transformation
  - `... >> stats >> update_transform_control >> completion`

## Key Design Decisions

- `source` as ORDER BY key — one row per source (aqicn, aqicn_forecast, dbt_transform)
- `ReplacingMergeTree(updated_at)` — dedups on source automatically
- `records_ingested=0` placeholder — Phase 1 will wire actual counts from Python jobs
- OpenAQ `update_openaq_control()` — added in Phase 1 Plan 1.1 when OpenAQ re-enabled

## Files Modified

- `scripts/init-clickhouse.sql` (added ingestion_control table)
- `python_jobs/common/ingestion_control.py` (created)
- `airflow/dags/dag_ingest_hourly.py` (added control tasks)
- `airflow/dags/dag_transform.py` (added control task)

## Commits

- `136347e` — feat(00-control): add ingestion control table and DAG wiring
