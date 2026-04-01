# Directory Structure

```
vietnam-air-quality-data-platform/
├── .env                          # Production secrets (API tokens, ClickHouse, Airflow keys)
├── .env.dev                      # Development environment overrides
├── .gitignore
├── .vscode/settings.json
├── README.md                     # Vietnamese overview
├── docker-compose.yml             # Full stack orchestration
├── build_job_and_dag.md           # Developer guide for jobs + DAGs

├── airflow/
│   ├── Dockerfile                 # apache/airflow:3.1.7 + Python deps
│   ├── config/
│   │   ├── airflow.cfg            # Airflow configuration
│   │   ├── entrypoint.sh           # Container entrypoint
│   │   └── setup_connections.py   # Airflow connection setup script
│   └── dags/
│       ├── __init__.py
│       ├── README.md
│       ├── dag_ingest_hourly.py   # Hourly measurement ingestion (TaskFlow API)
│       ├── dag_ingest_historical.py # Manual historical backfill
│       ├── dag_metadata_update.py  # Daily metadata refresh
│       └── dag_transform.py        # dbt run/test pipeline

├── python_jobs/
│   ├── config/
│   │   └── job_config.yaml         # Shared job configuration
│   ├── api_client.py              # APIClient + PaginatedAPIClient (retry, rate limit)
│   ├── clickhouse_writer.py       # ClickHouse bulk writer with dedup
│   ├── config.py                  # YAML config loader
│   ├── logging_config.py          # JSON structured logging
│   ├── rate_limiter.py            # TokenBucketRateLimiter
│   └── jobs/
│       ├── openaq/
│       │   ├── ingest_parameters.py    # Fetch pollutant/measurement types
│       │   ├── ingest_locations.py      # Fetch Vietnam monitoring locations
│       │   ├── ingest_sensors.py       # Fetch sensors per location
│       │   └── ingest_measurements.py  # Fetch measurements (incremental/rewrite)
│       └── aqicn/
│           ├── ingest_measurements.py  # Fetch AQICN station measurements
│           └── ingest_forecast.py      # Fetch AQICN forecast data

├── dbt/
│   └── dbt_tranform/              # dbt project (name: dbt_tranform)
│       ├── dbt_project.yml        # staging=view, intermediate=view, marts=table
│       ├── profiles.yml            # ClickHouse profiles (production + dev targets)
│       ├── README.md
│       ├── .user.yml
│       ├── macros/
│       │   ├── calculate_aqi.sql       # AQI calculation macro
│       │   ├── filter_vietnam.sql      # Vietnam location filter
│       │   ├── parse_timestamp.sql     # Timestamp parsing
│       │   └── standardize_pollutant.sql # Pollutant name normalization
│       └── models/
│           ├── staging/
│           │   ├── openaq/
│           │   │   ├── stg_openaq__locations.sql
│           │   │   ├── stg_openaq__parameters.sql
│           │   │   ├── stg_openaq__sensors.sql
│           │   │   ├── stg_openaq__measurements.sql
│           │   │   ├── _staging_openaq_schema.yml
│           │   │   └── _openaq__sources.yml
│           │   └── aqicn/
│           │       ├── stg_aqicn__measurements.sql
│           │       ├── stg_aqicn__stations.sql
│           │       ├── stg_aqicn__forecast.sql
│           │       ├── _staging_aqicn_schema.yml
│           │       └── _aqicn__sources.yml
│           ├── intermediate/
│           │   ├── int_aqi_calculations.sql
│           │   ├── int_data_quality.sql
│           │   ├── int_forecast_accuracy.sql
│           │   ├── int_unified__measurements.sql
│           │   └── _intermediate_schema.yml
│           └── marts/
│               ├── core/
│               │   ├── mart_air_quality__hourly.sql
│               │   ├── mart_air_quality__daily_summary.sql
│               │   └── mart_air_quality__stations.sql
│               ├── kpis/
│               │   ├── mart_kpis__pollutant_concentrations.sql
│               │   ├── mart_kpis__data_coverage.sql
│               │   ├── mart_kpis__air_quality_index.sql
│               │   └── _marts_schema.yml
│               └── analytics/
│                   ├── mart_analytics__trends.sql
│                   ├── mart_analytics__geographic.sql
│                   └── mart_analytics__forecast_accuracy.sql

├── scripts/
│   └── init-clickhouse.sql        # ClickHouse schema init (runs in container)
│                                  # Creates airquality database + raw tables

├── monitoring/                    # Grafana dashboards (referenced in README,
│                                  # not in docker-compose.yml)

├── clickhouse-data/              # ClickHouse persistent data volume
├── airflow/
│   ├── logs/                     # Airflow task logs
│   └── data/postgres/             # PostgreSQL persistent data

└── venv/                         # Local Python virtualenv (excluded from docker)
```

## Key Files

### docker-compose.yml
- Defines 6 services: clickhouse, airflow-webserver, airflow-scheduler, airflow-dag-processor, airflow-triggerer, postgres
- Shared environment variables via `.env`
- All Airflow services share volumes: dags, python_jobs, dbt_tranform

### python_jobs/config/job_config.yaml
- Centralized YAML configuration for all jobs
- Shared between all Python ingestion scripts

### dbt/dbt_tranform/profiles.yml
- `production` target: uses env vars for ClickHouse connection
- `dev` target: localhost configuration
- dbt-clickhouse adapter handles ClickHouse SQL dialect

### airflow/Dockerfile
- Base: `apache/airflow:3.1.7`
- Installs: requirements.txt packages, apache-airflow-providers-http/sqlite/postgres
- Mounts: dbt project, python_jobs, airflow config

### scripts/init-clickhouse.sql
- Creates `airquality` database
- Creates all raw tables with MergeTree/ReplacingMergeTree engines
- Documents deduplication strategy (Python-side dedup, unique key on period)
