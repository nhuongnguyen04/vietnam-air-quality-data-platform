# Architecture

Vietnam Air Quality Data Platform — end-to-end data engineering pipeline.

## System Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCES                               │
│                                                                              │
│  OpenAQ API          AQICN API                                              │
│  api.openaq.org      api.waqi.info                                          │
│  (48 req/min)        (60 req/min)                                          │
└──────────┬──────────────────────┬───────────────────────────────────────────┘
           │                      │
           ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER (Python)                            │
│                                                                              │
│  python_jobs/                                                              │
│  ├── api_client.py        — APIClient (retry, timeout)                       │
│  │                        — PaginatedAPIClient (cursor/page)                 │
│  ├── clickhouse_writer.py — bulk insert with dedup                           │
│  ├── config.py            — YAML job config loader                           │
│  ├── logging_config.py    — JSON structured logging                          │
│  └── rate_limiter.py      — TokenBucketRateLimiter                          │
│                                                                              │
│  Jobs per source:                                                           │
│  openaq/:  ingest_parameters, ingest_locations, ingest_sensors,             │
│            ingest_measurements (--mode incremental|rewrite)                  │
│  aqicn/:   ingest_measurements, ingest_forecast                              │
└──────────┬──────────────────────┬───────────────────────────────────────────┘
           │                      │
           ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          STORAGE LAYER (ClickHouse 25.12)                    │
│                                                                              │
│  airquality database                                                        │
│  ├── RAW (MergeTree, ReplacingMergeTree)                                   │
│  │   ├── raw_openaq_locations, raw_openaq_parameters, raw_openaq_sensors    │
│  │   ├── raw_openaq_measurements (append-only, dedup in Python)             │
│  │   └── raw_aqicn_stations, raw_aqicn_measurements, raw_aqicn_forecast     │
│  └── dbt output                                                              │
│      ├── STAGING (views) — stg_openaq__*, stg_aqicn__*                     │
│      ├── INTERMEDIATE (views) — int_aqi_calculations, int_data_quality,     │
│      │                         int_forecast_accuracy, int_unified__*        │
│      └── MARTS (tables) — fct_*, mart_air_quality__*, mart_kpis__*,        │
│                            mart_analytics__*                                 │
└──────────┬──────────────────────┬───────────────────────────────────────────┘
           │                      │
           ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER (Apache Airflow 3.1)                  │
│                                                                              │
│  4 services (all in docker-compose):                                         │
│  ├── airflow-webserver    → api-server command (port 8090→8080)             │
│  ├── airflow-scheduler    → scheduler command                              │
│  ├── airflow-dag-processor → dag-processor command                          │
│  └── airflow-triggerer    → triggerer command                               │
│                                                                              │
│  Executor: LocalExecutor                                                    │
│  Metadata DB: PostgreSQL 15 (airflow/airflow/airflow)                        │
│  Auth: basic_auth with JWT for API                                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. OpenAQ Data Path
```
OpenAQ API → ingest_parameters.py → raw_openaq_parameters
                       ↓
          ingest_locations.py → raw_openaq_locations
                       ↓
            ingest_sensors.py → raw_openaq_sensors
                       ↓
        ingest_measurements.py → raw_openaq_measurements
```

### 2. AQICN Data Path
```
AQICN API → ingest_measurements.py → raw_aqicn_measurements
                    ↓
          ingest_forecast.py → raw_aqicn_forecast
                    ↓
          (stations populated via feed API crawl.html)
```

### 3. dbt Transformation Path
```
staging → intermediate → marts
 (views)    (views)     (tables)

stg_openaq__*        ──┐
stg_aqicn__*         ──┼──→ int_aqi_calculations
                       ├──→ int_data_quality
                       ├──→ int_forecast_accuracy
                       └──→ int_unified__measurements
                                                 │
                                                 ▼
                              mart_air_quality__hourly
                              mart_air_quality__daily_summary
                              mart_air_quality__stations
                              mart_kpis__pollutant_concentrations
                              mart_kpis__data_coverage
                              mart_kpis__air_quality_index
                              mart_analytics__trends
                              mart_analytics__geographic
                              mart_analytics__forecast_accuracy
```

## Airflow DAGs

### dag_ingest_hourly (schedule: `0 * * * *`)
```
check_clickhouse
       ↓
ensure_metadata  (conditional: run metadata ingest if missing)
       ↓
[run_openaq_measurements, run_aqicn_measurements, run_aqicn_forecast]  (parallel)
       ↓
log_completion
```
- Uses Airflow 3 TaskFlow API (`@dag`, `@task` decorators)
- Env vars captured at execution time (not parse time) via `get_job_env_vars()`
- All env vars injected into subprocess via `env.copy()`

### dag_transform (schedule: `30 * * * *`)
```
check_clickhouse → check_dbt_ready
       ↓
dbt_deps → dbt_seed → dbt_run_staging → dbt_run_intermediate
                                         ↓
                                  dbt_run_marts → dbt_test
                                         ↓
                                  log_dbt_stats → log_completion
```

### dag_metadata_update (schedule: daily at 01:00)
- Runs openaq: ingest_parameters, ingest_locations, ingest_sensors
- Ensures metadata is fresh before next hourly run

### dag_ingest_historical
- Manual trigger only (no schedule)
- Config: `start_date`/`end_date` or `days_back` parameters
- Backfills historical measurements from both sources

## Key Architecture Decisions

### Dedup Strategy
- ClickHouse MergeTree: Python jobs check for duplicates before insert
- Unique key: `(location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc)`
- `raw_payload` column stores full JSON for audit/debugging

### Materialization Strategy
- **staging**: `view` — fast to refresh, always current
- **intermediate**: `view` — layered business logic
- **marts**: `table` — pre-computed for query performance

### Airflow 3 Changes
- `dag-processor` runs separately from `scheduler`
- `triggerer` enables deferrable operators
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` set for cross-service communication
- DAG env vars must be captured inside `@task` functions (not module-level dicts)

## Infrastructure

### Docker Compose Services
| Service | Port | Purpose |
|---|---|---|
| clickhouse | 8123, 9000, 9440 | Analytical database |
| airflow-webserver | 8090 | Airflow UI + API |
| airflow-scheduler | — | DAG scheduling |
| airflow-dag-processor | — | DAG file parsing |
| airflow-triggerer | — | Deferred task execution |
| postgres | 5432 | Airflow metadata |

### Network
- All services on `air-quality-network`
- Services reference each other by container name
