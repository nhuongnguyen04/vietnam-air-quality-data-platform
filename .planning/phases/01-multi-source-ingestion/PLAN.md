---
gsd_plan_version: 1.0
phase: 01
slug: multi-source-ingestion
status: draft
wave: 0
depends_on: []
files_modified:
  - .planning/phases/01-multi-source-ingestion/PLAN.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-1-01.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-1-02.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-1-03.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-1-04.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-1-05.md
  - .planning/phases/01-multi-source-ingestion/plans/PLAN-0-00.md
autonomous: true
created: 2026-04-01
must_haves:
  - pytest test stubs exist for all 5 sources + decommission + rate_limiter
  - conftest.py with ClickHouse and rate_limiter fixtures
  - pytest.ini with correct testpaths
  - Wave 0 complete before any Wave 1 tasks are executed
---

# Phase 1 — Multi-Source Ingestion: Master Plan

**Phase goal:** Replace OpenAQ with OpenWeather Air Pollution API, WAQI/World Air Quality Index, and Sensors.Community. AQICN stays as primary. All ingestion additive with zero risk to existing pipeline.

**4 sources at end of Phase 1:** AQICN (existing) + OpenWeather + WAQI + Sensors.Community.

---

## Scope (locked — do not revisit)

- OpenWeather Air Pollution API → Plan 1.01
- WAQI / World Air Quality Index → Plan 1.02
- Sensors.Community → Plan 1.03
- OpenAQ Decommission → Plan 1.04
- Rate Limiter + Orchestration → Plan 1.05
- Wave 0: test infrastructure → all plans depend on this

**Out of scope:** MONRE (no public API), AirNow (US/Canada-only).

---

## Decisions (locked — do not revisit)

| ID | Decision |
|----|----------|
| D-01 | All new raw tables: `ReplacingMergeTree(ingest_time)`, ORDER BY `(station_id, timestamp_utc, parameter)` |
| D-02 | No Python-side dedup for new sources — ClickHouse handles server-side |
| D-03 | OpenWeather: city-centroid polling (Hanoi 21.0°N/105.8°E, HCMC 10.8°N/106.7°E, Da Nang 16.1°N/108.2°E) |
| D-04 | WAQI: one bounding-box call `/feed/geo:8.4;102.1;23.4;109.5/` |
| D-05 | Sensors.Community: insert ALL data; `quality_flag` = valid/implausible/outlier |
| D-06 | All 3 new sources run in parallel in `dag_ingest_hourly` (not sequentially) |
| D-07 | OpenAQ tables renamed to `raw_openaq_*_archived` (not dropped) |
| D-28 | `OPENWEATHER_API_TOKEN`, `WAQI_API_TOKEN` added to `.env` and docker-compose env blocks |
| D-31 | tenacity retry with exponential backoff (base=2, max=5, max_wait=5min) added to `APIClient` |

---

## Wave Map

```
Wave 0 (foundation) ──────────────────────────────────────────────────────┐
  PLAN-0-00: Test infrastructure stubs                                       │
                                                                             ▼
Wave 1 (ingestion clients) ─────────────────────────────────────────────┐  │
  PLAN-1-01: OpenWeather client                                         │  │
  PLAN-1-02: WAQI client                                                 │  │
  PLAN-1-03: Sensors.Community client                                    │  │
                                                                             │
  (all Wave 1 plans are independent and run in parallel)                     │
                                                                             ▼
Wave 2 (decommission) ──────────────────────────────────────────────────┐  │
  PLAN-1-04: OpenAQ decommission + metadata DAG fix (D-07, D-32-D-35)  │  │
                                                                             │
  (depends on all Wave 1 plans complete)                                      │
                                                                             ▼
Wave 3 (optimization) ─────────────────────────────────────────────────┐  │
  PLAN-1-05: Rate limiter + parallel orchestration + dag_ingest_hourly │  │
                                                                             │
  (depends on Wave 2 complete)                                                │
                                                                             ▼
  Phase 1 done
```

---

## Files to Create

### Wave 0 — Test Infrastructure
- `tests/conftest.py` — shared fixtures
- `tests/test_openweather.py` — stubs
- `tests/test_waqi.py` — stubs
- `tests/test_sensorscm.py` — stubs
- `tests/test_decommission.py` — stubs
- `tests/test_rate_limiter.py` — stubs
- `pytest.ini`

### Wave 1 — Ingestion Clients
- `python_jobs/jobs/openweather/__init__.py`
- `python_jobs/jobs/openweather/ingest_measurements.py`
- `python_jobs/models/openweather_models.py`
- `scripts/init-clickhouse.sql` — ADD: `raw_openweather_measurements` table
- `tests/test_openweather_int.py` — integration stubs

- `python_jobs/jobs/waqi/__init__.py`
- `python_jobs/jobs/waqi/ingest_measurements.py`
- `python_jobs/models/waqi_models.py`
- `scripts/init-clickhouse.sql` — ADD: `raw_waqi_measurements` table
- `tests/test_waqi_int.py` — integration stubs

- `python_jobs/jobs/sensorscm/__init__.py`
- `python_jobs/jobs/sensorscm/ingest_measurements.py`
- `python_jobs/models/sensorscm_models.py`
- `scripts/init-clickhouse.sql` — ADD: `raw_sensorscm_measurements`, `raw_sensorscm_sensors` tables
- `tests/test_sensorscm_int.py` — integration stubs

### Wave 2 — OpenAQ Decommission
- `airflow/dags/dag_ingest_hourly.py` — updated (no OpenAQ, 3 new sources)
- `airflow/dags/dag_metadata_update.py` — updated (no OpenAQ refs, DB name fix)
- `scripts/init-clickhouse.sql` — ADD: ALTER TABLE for OpenAQ rename

### Wave 3 — Optimization
- `python_jobs/common/rate_limiter.py` — ADD: `create_openweather_limiter()`, `create_waqi_limiter()`, `create_sensorscm_limiter()`
- `python_jobs/common/api_client.py` — ADD: tenacity retry (base=2, max=5, max_wait=5min)
- `airflow/dags/dag_ingest_hourly.py` — ADD: parallel task pattern for 3 new sources
- `airflow/dags/dag_sensorscm_poll.py` — NEW: `*/10 * * * *` schedule
- `.env` — ADD: `OPENWEATHER_API_TOKEN`, `WAQI_API_TOKEN`
- `docker-compose.yml` — ADD: env vars to all Airflow services

---

## Verification Criteria

After all plans complete:

1. `raw_openweather_measurements` has >0 rows
2. `raw_waqi_measurements` has >0 rows
3. `raw_sensorscm_measurements` has >0 rows
4. `dag_ingest_hourly` runs with 0 OpenAQ tasks
5. `python_jobs/jobs/openaq/` directory removed
6. OpenAQ tables renamed to `raw_openaq_*_archived`
7. All 3 new sources run in parallel in `dag_ingest_hourly`
8. `dag_sensorscm_poll` runs every 10 minutes
9. `ingestion.control` updated for all 6 sources (aqicn, aqicn_forecast, openweather, waqi, sensorscm, dbt_transform)
10. Zero HTTP 429 errors in logs

---

*Generated: 2026-04-01*
