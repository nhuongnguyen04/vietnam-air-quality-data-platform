# Technology Stack

Vietnam Air Quality Data Platform — complete technology stack (venv/ excluded).

---

## Programming Languages

| Language | Version | Role |
|----------|---------|------|
| Python | 3.10 (via Airflow base image) | All ingestion jobs, DAGs, dbt execution |
| SQL | ClickHouse dialect | dbt transformation models |

---

## Python Frameworks & Libraries

### Core Dependencies (`requirements.txt`)

| Package | Version | Purpose |
|---------|---------|---------|
| `apache-airflow` | 3.1.7 | Orchestration & scheduling |
| `apache-airflow-providers-http` | (bundled with Airflow 3.x) | HTTP operator for API calls |
| `apache-airflow-providers-postgres` | (bundled with Airflow 3.x) | PostgreSQL Airflow backend |
| `apache-airflow-providers-sqlite` | (bundled with Airflow 3.x) | SQLite support |
| `dbt-core` | 1.10.13 | Data transformation framework |
| `dbt-clickhouse` | 1.9.5 | ClickHouse adapter for dbt |
| `sqlfluff` | 3.5.0 | SQL linting |
| `sqlfluff-templater-dbt` | 3.5.0 | dbt-aware SQL linting |
| `clickhouse-connect` | 0.9.2 | ClickHouse Python client |
| `psycopg2-binary` | 2.9.11 | PostgreSQL adapter (Airflow metadata DB) |
| `requests` | (pinned via Airflow deps) | HTTP client for API ingestion |
| `python-json-logger` | 2.0.7 | JSON-structured logging for Python jobs |
| `pydantic` | (latest) | Data validation & config modeling |

### Airflow Dockerfile (`airflow/Dockerfile`)

- **Base image**: `apache/airflow:3.1.7`
- Installs all packages from `requirements.txt`
- Explicitly adds `apache-airflow-providers-http`, `apache-airflow-providers-sqlite`, `apache-airflow-providers-postgres` at build time
- Copies `dbt/dbt_tranform` into `/opt/dbt/dbt_tranform`

### Python Jobs (`python_jobs/`)

The `python_jobs/` package is a Python library consumed by Airflow DAG tasks. It is mounted read-only into Airflow containers at `/opt/python/jobs`.

| Module | File | Purpose |
|--------|------|---------|
| `common` | `api_client.py` | Generic `APIClient` with retry/backoff; `PaginatedAPIClient` for OpenAQ; factory functions `create_openaq_client()` and `create_aqicn_client()` |
| `common` | `clickhouse_writer.py` | `ClickHouseWriter` — batch insert writer via HTTP interface; `create_clickhouse_writer()` factory |
| `common` | `config.py` | Dataclass-based config: `ClickHouseConfig`, `APIConfig`, `JobConfig`, `IngestionConfig`; reads from env vars and YAML |
| `common` | `logging_config.py` | JSON-structured logging via `pythonjsonlogger`; `StructuredLogFormatter`, `JobLogger`, `JobContextFilter` |
| `common` | `rate_limiter.py` | `TokenBucketRateLimiter` (thread-safe, sliding window); `AdaptiveRateLimiter`; `create_openaq_limiter()` (~48 req/min), `create_aqicn_limiter()` (~60 req/min) |
| `jobs/openaq` | `ingest_parameters.py` | Ingest OpenAQ parameter definitions |
| `jobs/openaq` | `ingest_locations.py` | Ingest OpenAQ location metadata for Vietnam |
| `jobs/openaq` | `ingest_sensors.py` | Ingest OpenAQ sensor metadata |
| `jobs/openaq` | `ingest_measurements.py` | Ingest OpenAQ measurement data (incremental + historical modes) |
| `jobs/aqicn` | `ingest_stations.py` | Ingest AQICN station metadata |
| `jobs/aqicn` | `ingest_measurements.py` | Ingest AQICN measurement data (incremental + historical modes) |
| `jobs/aqicn` | `ingest_forecast.py` | Ingest AQICN forecast data |

### Airflow DAGs (`airflow/dags/`)

| DAG | File | Schedule | Purpose |
|-----|------|----------|---------|
| `dag_ingest_hourly` | `dag_ingest_hourly.py` | `0 * * * *` (hourly at minute 0) | Run OpenAQ + AQICN measurements + AQICN forecast ingestion every hour |
| `dag_ingest_historical` | `dag_ingest_historical.py` | Manual trigger only | One-time backfill of historical data |
| `dag_metadata_update` | `dag_metadata_update.py` | `0 1 * * *` (daily at 01:00) | Daily refresh of OpenAQ parameters, locations, sensors, and AQICN stations |
| `dag_transform` | `dag_transform.py` | `30 * * * *` (hourly at minute 30) | Run `dbt deps` → `dbt seed` → `dbt run` (staging → intermediate → marts) → `dbt test` |

All DAGs use the **Airflow 3.x TaskFlow API** (`@dag`, `@task` decorators). DAGs pass env vars at task execution time (not parse time) via `get_job_env_vars()` functions.

---

## Infrastructure Tools

### Databases

| Product | Version | Role |
|---------|---------|------|
| **ClickHouse** | **25.12** | Primary analytical database; stores all raw measurements, forecasts, metadata, and dbt-transformed marts |
| **PostgreSQL** | **15** | Airflow metadata database (scheduler, DAG state, connections, XCom) |

#### ClickHouse Tables (defined in `scripts/init-clickhouse.sql`)

Raw (ingestion) tables:
- `raw_openaq_measurements` — MergeTree, append-only
- `raw_openaq_locations` — ReplacingMergeTree, deduped on `location_id`
- `raw_openaq_parameters` — ReplacingMergeTree, deduped on `parameter_id`
- `raw_openaq_sensors` — ReplacingMergeTree, deduped on `sensor_id`
- `raw_aqicn_measurements` — MergeTree, append-only
- `raw_aqicn_forecast` — ReplacingMergeTree
- `raw_aqicn_stations` — ReplacingMergeTree, deduped on `station_id`

All tables include standard metadata columns: `source`, `ingest_time`, `ingest_batch_id`, `ingest_date`, and a `raw_payload` JSON column for full audit.

### Message Queue / Scheduling

- **Apache Airflow 3.1.7** — orchestration and scheduling
  - Services deployed: `airflow-webserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`
  - Executor: `LocalExecutor`
  - Auth: `airflow.api.auth.backend.basic_auth`
  - Execution API server enabled at `/execution/`
- **Docker Compose** — all services containerized; no separate message queue needed (Airflow handles scheduling internally via PostgreSQL)

### dbt

| Component | Value |
|-----------|-------|
| `dbt-core` | 1.10.13 |
| `dbt-clickhouse` adapter | 1.9.5 |
| dbt project name | `dbt_tranform` |
| Project directory (host) | `dbt/dbt_tranform/` |
| Mounted in container at | `/opt/dbt/dbt_tranform` |
| Profiles directory | `/opt/dbt/dbt_tranform` |
| Profiles target | `production` (also `dev` available) |
| ClickHouse connection | HTTP on port 8123; user/password via env vars; LZ4 compression; 4 threads; 300s timeout; 3 retries |

**dbt model layers** (configured in `dbt_project.yml`):
- `staging` — materialized as `view` (raw → staged)
- `intermediate` — materialized as `view` (staged → intermediate)
- `marts` — materialized as `table` (intermediate → analytics-ready fact tables)

**dbt packages** (`dbt_packages/`): directory is present; no packages were found installed (directory exists but `dbt_packages/` was empty/contained no files at time of scanning).

### Deployment

| Tool | Role |
|------|------|
| Docker Compose (`docker-compose.yml`) | Defines all 7 services; version 3.8 |
| `airflow/Dockerfile` | Builds custom Airflow image from `apache/airflow:3.1.7` |
| `airflow/config/entrypoint.sh` | Custom entrypoint: runs `airflow db migrate/init`, creates log directories, handles Airflow 3.x command mapping (`webserver` → `api-server`) |
| `airflow/config/setup_connections.py` | Python script to create Airflow connections programmatically |

---

## Environment & Configuration

All services share configuration via Docker environment variables (defined in `.env`, consumed in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | `admin` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `admin123456` | ClickHouse password |
| `CLICKHOUSE_DB` | `air_quality` | ClickHouse database name |
| `OPENAQ_API_TOKEN` | (from `.env`) | OpenAQ API key (`X-API-KEY` header) |
| `AQICN_API_TOKEN` | (from `.env`) | AQICN API token (passed as query param `?token=`) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | PostgreSQL Airflow backend |
| `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` | auto-assembled | Airflow connection string for ClickHouse sensor |
| `DBT_PROFILES_DIR` | `/opt/dbt/dbt_tranform` | dbt profiles directory |
| `PYTHON_JOBS_DIR` | `/opt/python/jobs/` | Python jobs mount path |
| `AIRFLOW_API_SECRET_KEY` | (from `.env`) | Airflow API secret |
| `AIRFLOW_API_AUTH_JWT_SECRET` | (from `.env`) | JWT signing secret |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | (from `.env`) | Webserver secret key |

> **Note**: `.env.dev` contains alternative development values for `OPENAQ_API_KEY` and `AQICN_TOKEN`, and adds `NIFI_USERNAME`/`NIFI_PASSWORD` (NiFi credentials — NiFi is referenced but not deployed in the current `docker-compose.yml`).
