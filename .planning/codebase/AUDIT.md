# Phase 0 — Codebase Audit

**Phase:** 00-foundation
**Plan:** 01-Audit
**Created:** 2026-04-01
**Source:** Phase 0 Plan 0.1 — Baseline Codebase Audit

Inventory of OpenAQ-specific assumptions, ClickHouse schema details, and DAG task dependencies needed for Phase 1 planning.

---

## ClickHouse Tables

### Schema Summary

| # | Table | Engine | ORDER BY | PARTITION | ingest_time in Engine? | Phase 1 Impact |
|---|-------|--------|----------|-----------|------------------------|----------------|
| 1 | `raw_openaq_measurements` | `MergeTree()` | `(location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc)` | `toYYYYMM(ingest_date)` | **No** | Python-side dedup only — see CONCERN #7 |
| 2 | `raw_openaq_locations` | `ReplacingMergeTree(ingest_time)` | `(location_id)` | `toYYYYMM(ingest_date)` | Yes | Phase 1 adds new source location tables |
| 3 | `raw_openaq_parameters` | `ReplacingMergeTree(ingest_time)` | `(parameter_id)` | `toYYYYMM(ingest_date)` | Yes | Phase 1 adds new source parameter tables |
| 4 | `raw_openaq_sensors` | `ReplacingMergeTree(ingest_time)` | `(sensor_id)` | `toYYYYMM(ingest_date)` | Yes | Phase 1 adds new source sensor tables |
| 5 | `raw_aqicn_measurements` | `MergeTree()` | `(station_id, time_v, pollutant, ingest_time)` | `toYYYYMM(ingest_date)` | No | Append-only — no changes needed |
| 6 | `raw_aqicn_forecast` | `ReplacingMergeTree(ingest_time)` | `(station_id, measurement_time_v, forecast_type, pollutant, day, ingest_time)` | `toYYYYMM(ingest_date)` | Yes | Append-only forecast table — no changes needed |
| 7 | `raw_aqicn_stations` | `ReplacingMergeTree(ingest_time)` | `(station_id, ingest_date, ingest_time)` | `toYYYYMM(ingest_date)` | Yes | Phase 1 may add new government station tables |

### Dedup Strategy

#### `raw_openaq_measurements` — MergeTree (Python-side dedup only)

> **CONCERN #7: Python-Side Dedup Only**
>
> Table: `raw_openaq_measurements`
> Engine: `MergeTree()` — **No server-side deduplication**
> Dedup strategy: Python checks ClickHouse before insert (see `python_jobs/jobs/openaq/ingest_measurements.py`)
> Risk: If dedup check races or fails, duplicates accumulate in MergeTree
> Fix: Migrate to `ReplacingMergeTree(version)` in Phase 2 Plan 2.4

The dedup is done in Python (`ingest_measurements.py`) by querying `SELECT max(ingest_time) FROM raw_openaq_measurements` before inserting new data. This is a race-prone pattern — two concurrent runs could both pass the check and insert duplicates.

#### `raw_aqicn_measurements` — MergeTree (append-only)

Table uses `MergeTree()` with ORDER BY `(station_id, time_v, pollutant, ingest_time)`. This is truly append-only — no dedup strategy, all historical rows kept.

---

## OpenAQ References in dbt Models

**Total references:** 55 lines across staging, intermediate, and schema files.

### Source Definitions (`dbt/dbt_tranform/models/staging/openaq/_openaq__sources.yml`)

Four dbt sources are defined:

| Source Name | Table | Description |
|-------------|-------|-------------|
| `openaq.raw_openaq_locations` | `raw_openaq_locations` | OpenAQ location metadata |
| `openaq.raw_openaq_parameters` | `raw_openaq_parameters` | OpenAQ parameter reference |
| `openaq.raw_openaq_sensors` | `raw_openaq_sensors` | OpenAQ sensor metadata |
| `openaq.raw_openaq_measurements` | `raw_openaq_measurements` | OpenAQ measurement data |

### Staging Models

| File | Purpose | OpenAQ-specific? |
|------|---------|-----------------|
| `stg_openaq__measurements.sql` | Selects from `source('openaq', 'raw_openaq_measurements')` with Vietnam filter | Yes — OpenAQ-specific |
| `stg_openaq__locations.sql` | Selects from `source('openaq', 'raw_openaq_locations')` with Vietnam filter | Yes — OpenAQ-specific |
| `stg_openaq__parameters.sql` | Selects from `source('openaq', 'raw_openaq_parameters')` | Yes — OpenAQ-specific |
| `stg_openaq__sensors.sql` | Joins `stg_openaq__locations` with `source('openaq', 'raw_openaq_sensors')` | Yes — OpenAQ-specific |

### Intermediate Models

| File | CTE Name | `'openaq'` Literal | Replaceable in Phase 1? |
|------|----------|--------------------|------------------------|
| `int_unified__measurements.sql` | `openaq_measurements` | `'openaq' as source_system` (line 28) | Yes — add AirNow/EPA CTE |
| `int_unified__measurements.sql` | — | `inner join {{ ref('stg_openaq__measurements') }}` | Yes — add AirNow CTE |
| `int_unified__measurements.sql` | — | `inner join {{ ref('stg_openaq__parameters') }}` | Yes |
| `int_unified__measurements.sql` | — | `inner join {{ ref('stg_openaq__locations') }}` | Yes |
| `int_unified__stations.sql` | `openaq_locations` | `'openaq' as source_system` (line 22) | Yes |
| `int_data_quality.sql` | `openaq_quality` | `'openaq' as source_system` (line 23) | Yes |

### Phase 1 Action Items for dbt OpenAQ References

1. **Keep all `{{ source('openaq', ...) }}` calls** — these are dbt source refs and must be kept
2. **Rename `openaq` source** in Phase 2 (not Phase 1) — Phase 1 adds new sources alongside existing ones
3. **Rename `'openaq'` literal strings** in intermediate CTEs — Phase 2 Plan 2.3 handles source name normalization
4. **OpenAQ staging models stay as-is in Phase 1** — new sources get their own staging directory

---

## OpenAQ References in Python Jobs

**Total references:** Module imports, docstrings, table names, logging, and config.

| File | Type | Details |
|------|------|---------|
| `jobs/openaq/ingest_measurements.py` | Table name | `table="raw_openaq_measurements"`, `source="openaq"` |
| `jobs/openaq/ingest_measurements.py` | API endpoint | `/v3/measurements` via `create_openaq_client()` |
| `jobs/openaq/ingest_measurements.py` | API header | `X-API-KEY` (OpenAQ token) |
| `jobs/openaq/ingest_measurements.py` | Config | `job_config.yaml` rate limits for OpenAQ |
| `jobs/openaq/ingest_locations.py` | Table name | `raw_openaq_locations` |
| `jobs/openaq/ingest_sensors.py` | Table name | `raw_openaq_sensors`, `raw_openaq_locations` |
| `jobs/openaq/ingest_parameters.py` | Table name | `raw_openaq_parameters` |
| `jobs/openaq/` | All files | Rate limiter: `create_openaq_limiter()` (~48 req/min) |
| `config/job_config.yaml` | Config | `openaq_token`, `rate_limit_openaq: 0.8`, country code |

### Phase 1 Action Items for Python OpenAQ References

- **Keep all OpenAQ Python jobs as-is in Phase 1** — Phase 1 adds AirNow ingestion alongside OpenAQ
- **AirNow job** gets new `jobs/airnow/` directory, not mixed into `jobs/openaq/`
- **`create_openaq_client()`** in `common/api_client.py` stays OpenAQ-specific

---

## Airflow DAGs

### `dag_ingest_hourly` — Hourly Ingestion

**DAG ID:** `dag_ingest_hourly`
**Schedule:** `0 * * * *` (hourly at minute 0)
**Tasks:**

```
check_clickhouse_connection
        ↓
ensure_metadata
        ↓
[run_openaq_measurements_ingestion, run_aqicn_measurements_ingestion, run_aqicn_forecast_ingestion]
        ↓
log_completion
```

**Env var capture:** `get_job_env_vars()` is a **function** (not module-level dict) — correct pattern for Airflow 3.
**DB fallback fixed:** `'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality')` (was `'airquality'`, fixed in Plan 0.1 task `audit-db-name`).

### `dag_transform` — Hourly dbt Transformation

**DAG ID:** `dag_transform`
**Schedule:** `30 * * * *` (hourly at minute 30)
**Tasks:**

```
check_clickhouse_connection → check_dbt_ready → dbt_deps → dbt_seed
        → dbt_run_staging → dbt_run_intermediate → dbt_run_marts
        → dbt_test → log_dbt_stats → log_completion
```

**Env var:** `DBT_ENV_VARS` is a module-level dict (not env-captured at execution time). Uses `'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'airquality')` — same `'airquality'` fallback bug as `dag_ingest_hourly` (fixed in Plan 0.1).

### `dag_metadata_update` — Daily Metadata Refresh

**DAG ID:** `dag_metadata_update`
**Schedule:** `0 1 * * *` (daily at 01:00)
**Tasks:** `refresh_openaq_parameters`, `refresh_openaq_locations`, `refresh_openaq_sensors` (OpenAQ-only)

### `dag_ingest_historical` — Manual Historical Backfill

**DAG ID:** `dag_ingest_historical`
**Schedule:** Manual only (`max_active_runs=1`, no schedule)
**Tasks:** `ingest_openaq_parameters`, `ingest_openaq_locations`, `run_openaq_measurements_ingestion`, `run_aqicn_measurements_ingestion`
**Note:** `max_active_runs=1` — CONCERN #14: may need higher concurrency for large historical backfills.

---

## DB Name Consistency

| File | Line | Value | Notes |
|------|------|-------|-------|
| `.env` | 4 | `air_quality` | Production value |
| `docker-compose.yml` | 20 | `${CLICKHOUSE_DB}` → `air_quality` | Resolves to .env |
| `scripts/init-clickhouse.sql` | 2 | `${CLICKHOUSE_DB}` → `air_quality` | Resolves to .env |
| `dbt/dbt_tranform/profiles.yml` dev | 13 | `air_quality` | Default matches .env |
| `dbt/dbt_tranform/profiles.yml` prod | 27 | `air_quality` | Default matches .env |
| `airflow/dags/dag_ingest_hourly.py` | 44 | `'air_quality'` | **Fixed** (was `'airquality'`) |
| `airflow/dags/dag_transform.py` | 37 | `'airquality'` | **Still has bug** — to fix in Phase 2 Plan 2.4 |

**Decision D-10:** No rename needed. DB is consistently named `air_quality` in production. The `airquality` fallback in `dag_transform.py` is never used in practice but should be fixed in Phase 2.

---

## Key Decisions Already Resolved

| Decision | Status | Notes |
|----------|--------|-------|
| D-09/D-10 DB Name | ✅ Fixed | `dag_ingest_hourly.py` fallback now `'air_quality'` |
| D-20 airflow-webserver Limits | ✅ Correct | No `mem_limit` added per decision |
| CONCERN #7 MergeTree Dedup | ⚠️ Known Risk | Phase 2 Plan 2.4 addresses |
| CONCERN #14 Historical Concurrency | ⚠️ Known Risk | Documented, fix deferred to Phase 2 |

---

*Generated by: Phase 0 Plan 0.1 — Baseline Codebase Audit*
