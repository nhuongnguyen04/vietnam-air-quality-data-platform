<!-- GSD:project-start source:PROJECT.md -->
## Project

**Vietnam Air Quality Data Platform — Refactor & Upgrade**

A comprehensive data engineering platform that ingests, transforms, and visualizes air quality data for Vietnam from multiple external sources (AQICN, Sensors.Community, OpenWeather, and government/MONRE data), stores it in ClickHouse, and exposes it through Superset dashboards, Grafana monitoring, and automated reports with alerting.

**This is a brownfield project.** The existing codebase already has OpenAQ ingestion, ClickHouse storage, dbt transformations, and Airflow orchestration. This refactor replaces the data source layer, modernizes the entire pipeline, and adds visualization + metadata management.

**Core Value:** Reliable, near-real-time air quality monitoring for Vietnam — trusted data from multiple sources, cleaned and unified, available to analysts and the public via dashboards and alerts.

### Constraints

- **Tech stack**: Python, ClickHouse, dbt, Airflow, Docker Compose — existing, no wholesale replacement
- **New additions**: Superset, Grafana, OpenMetadata — to be containerized alongside existing services
- **Near-real-time**: Target <15 min ingestion latency if sources and API rate limits permit; fall back to hourly if unstable
- **Vietnam focus**: All sources must have measurable Vietnam data coverage
- **API costs**: Prefer free-tier APIs; AQICN token already exists
- **Stability over speed**: Get it working reliably before optimizing for speed
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Programming Languages
| Language | Version | Role |
|----------|---------|------|
| Python | 3.10 (via Airflow base image) | All ingestion jobs, DAGs, dbt execution |
| SQL | ClickHouse dialect | dbt transformation models |
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
| Module | File | Purpose |
|--------|------|---------|
| `common` | `api_client.py` | Generic `APIClient` with retry/backoff; factory function `create_aqicn_client()` (api.waqi.info); `create_openaq_client()` (deprecated, OpenAQ removed) |
| `common` | `clickhouse_writer.py` | `ClickHouseWriter` — batch insert writer via HTTP interface; `create_clickhouse_writer()` factory |
| `common` | `config.py` | Dataclass-based config: `ClickHouseConfig`, `APIConfig`, `JobConfig`, `IngestionConfig`; reads from env vars and YAML |
| `common` | `logging_config.py` | JSON-structured logging via `pythonjsonlogger`; `StructuredLogFormatter`, `JobLogger`, `JobContextFilter` |
| `common` | `rate_limiter.py` | `TokenBucketRateLimiter` (thread-safe, sliding window); `AdaptiveRateLimiter`; `create_aqicn_limiter()` (~60 req/min), `create_openweather_limiter()` (~60 req/min) |
| `jobs/aqicn` | `ingest_stations.py` | Ingest AQICN station metadata |
| `jobs/aqicn` | `ingest_measurements.py` | Ingest AQICN measurement data (incremental + historical modes) |
| `jobs/aqicn` | `ingest_forecast.py` | Ingest AQICN forecast data |
| `jobs/sensorscm` | `ingest_sensors.py` | Ingest Sensors.Community sensor metadata |
| `jobs/sensorscm` | `ingest_measurements.py` | Ingest Sensors.Community measurement data (incremental + historical modes) |
| `jobs/openweather` | `ingest_measurements.py` | Ingest OpenWeather Air Pollution measurements (incremental mode) |
### Airflow DAGs (`airflow/dags/`)
| DAG | File | Schedule | Purpose |
|-----|------|----------|---------|
| `dag_ingest_hourly` | `dag_ingest_hourly.py` | `0 * * * *` (hourly at minute 0) | Run AQICN + Sensors.Community + OpenWeather measurements + AQICN forecast ingestion every hour (4 parallel sources) |
| `dag_ingest_historical` | `dag_ingest_historical.py` | Manual trigger only | One-time backfill of historical data |
| `dag_metadata_update` | `dag_metadata_update.py` | `0 1 * * *` (daily at 01:00) | Daily refresh of Sensors.Community sensors and AQICN stations metadata |
| `dag_transform` | `dag_transform.py` | `30 * * * *` (hourly at minute 30) | Run `dbt deps` → `dbt seed` → `dbt run` (staging → intermediate → marts) → `dbt test` |
## Infrastructure Tools
### Databases
| Product | Version | Role |
|---------|---------|------|
| **ClickHouse** | **25.12** | Primary analytical database; stores all raw measurements, forecasts, metadata, and dbt-transformed marts |
| **PostgreSQL** | **15** | Airflow metadata database (scheduler, DAG state, connections, XCom) |
#### ClickHouse Tables (defined in `scripts/init-clickhouse.sql`)
- `raw_aqicn_measurements` — MergeTree, append-only
- `raw_aqicn_forecast` — ReplacingMergeTree
- `raw_aqicn_stations` — ReplacingMergeTree, deduped on `station_id`
- `raw_sensorscm_measurements` — ReplacingMergeTree, deduped on `sensor_id + timestamp`
- `raw_sensorscm_sensors` — ReplacingMergeTree, deduped on `sensor_id`
- `raw_openweather_measurements` — MergeTree, append-only
### Message Queue / Scheduling
- **Apache Airflow 3.1.7** — orchestration and scheduling
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
- `staging` — materialized as `view` (raw → staged)
- `intermediate` — materialized as `view` (staged → intermediate)
- `marts` — materialized as `table` (intermediate → analytics-ready fact tables)
### Deployment
| Tool | Role |
|------|------|
| Docker Compose (`docker-compose.yml`) | Defines all 7 services; version 3.8 |
| `airflow/Dockerfile` | Builds custom Airflow image from `apache/airflow:3.1.7` |
| `airflow/config/entrypoint.sh` | Custom entrypoint: runs `airflow db migrate/init`, creates log directories, handles Airflow 3.x command mapping (`webserver` → `api-server`) |
| `airflow/config/setup_connections.py` | Python script to create Airflow connections programmatically |
## Environment & Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | `admin` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `admin123456` | ClickHouse password |
| `CLICKHOUSE_DB` | `air_quality` | ClickHouse database name |
| `OPENAQ_API_TOKEN` | (from `.env`) | OpenAQ API key (deprecated; OpenAQ ingestion removed; `X-API-KEY` header) |
| `AQICN_API_TOKEN` | (from `.env`) | AQICN API token (passed as query param `?token=`) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | PostgreSQL Airflow backend |
| `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` | auto-assembled | Airflow connection string for ClickHouse sensor |
| `DBT_PROFILES_DIR` | `/opt/dbt/dbt_tranform` | dbt profiles directory |
| `PYTHON_JOBS_DIR` | `/opt/python/jobs/` | Python jobs mount path |
| `AIRFLOW_API_SECRET_KEY` | (from `.env`) | Airflow API secret |
| `AIRFLOW_API_AUTH_JWT_SECRET` | (from `.env`) | JWT signing secret |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | (from `.env`) | Webserver secret key |
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## 1. Python Style
### Tooling & Configuration
- `requirements.txt` at the repo root is the single source of truth for Python dependencies.
- No per-DAG or per-module type-checking config files exist.
### Code Style Rules
- **PEP 8** compliance is expected for all Python code.
- **Docstrings**: Every module and class should have a docstring. Use triple-quoted strings.
- **Imports**: Standard library first, then third-party, then local.
- **Type hints**: Use type hints for function parameters and return types where practical.
- **Logging**: Use `logging.getLogger(__name__)` — do not use `print()` for application logging except in lightweight Airflow task wrappers.
- **Variable naming**: `snake_case` for variables and functions, `PascalCase` for classes.
- **Constants**: `UPPERCASE_WITH_UNDERSCORES` at module level.
### Python Package Structure
- Python ingestion jobs live in `/python_jobs/jobs/<source>/` (e.g., `python_jobs/jobs/aqicn/`, `python_jobs/jobs/sensorscm/`, `python_jobs/jobs/openweather/`).
- Shared utilities live in `python_jobs/common/` (e.g., `api_client.py`, `rate_limiter.py`, `clickhouse_writer.py`, `config.py`, `logging_config.py`).
- Models live in `python_jobs/models/` (e.g., `aqicn_models.py`, `sensorscm_models.py`, `openweather_models.py`).
- Every package directory must contain an `__init__.py` file.
### CLI Arguments
- `--mode`: `incremental` (default for hourly runs), `historical` (for backfills), or `rewrite` (for metadata refresh).
- `--start-date`, `--end-date`: Date range for historical ingestion.
- `--days-back`: Number of days to backfill for AQICN.
## 2. SQL Naming Conventions (dbt)
### Project Setup
- **Project name**: `dbt_tranform`
- **Project file**: `dbt/dbt_tranform/dbt_project.yml`
- **Profiles file**: `dbt/dbt_tranform/profiles.yml`
- **dbt version**: `dbt-core==1.10.13`, `dbt-clickhouse==1.9.5`
- **SQL linter**: `sqlfluff==3.5.0` with `sqlfluff-templater-dbt==3.5.0`
### dbt Model Naming Prefixes
| Layer | Prefix | Materialization | Description |
|---|---|---|---|
| Staging | `stg_<source>__<entity>` | `view` | Raw source cleaning, type casting, Vietnam filtering |
| Intermediate | `int_<concept>__<entity>` | `view` | Cross-source unions, AQI calculations, quality metrics |
| Marts | `mart_<domain>__<entity>` | `table` | Analytics-ready tables |
### dbt Source Naming
### dbt Model Config
- Staging and intermediate models: `materialized='view'`
- Marts models: `materialized='table'`
### SQL Style
- Use CTEs (`with source as (...)`) for readability.
- Use `toFloat64OrNull()`, `toInt32OrNull()` for safe type casting (ClickHouse dialect).
- Use explicit schema prefixes: `{{ source('...') }}` and `{{ ref('...') }}`.
- Use Jinja macros for repeated operations (e.g., `{{ parse_unix_timestamp('col') }}`).
- dbt package install path: `dbt_packages` (configurable via `DBT_PACKAGES_INSTALL_PATH` env var).
## 3. YAML Schema Conventions (dbt `schema.yml`)
### Schema File Naming
### Schema.yml Structure
### Tests Used
- `not_null` — primary keys and required fields
- `unique` — identifier columns (e.g., `station_id`, `location_id`)
- `accepted_values` — enum-like fields
### Documentation
## 4. Airflow DAG Conventions
### Framework & Version
- **Airflow version**: `apache-airflow==3.1.7`
- **API**: Airflow 3 TaskFlow API (`@dag`, `@task` decorators from `airflow.decorators`)
- **Executor**: `LocalExecutor`
- **DAG files**: `airflow/dags/<dag_name>.py`
### DAG Naming
- File name: `dag_<domain>_<schedule>.py` (e.g., `dag_ingest_hourly.py`, `dag_ingest_historical.py`, `dag_transform.py`, `dag_metadata_update.py`)
- DAG ID: matches the function name (e.g., `dag_ingest_hourly`)
### Default Args
### Task Naming
- Use `snake_case` task names (function names decorated with `@task`).
- Tasks follow a `verb_noun` pattern: `check_clickhouse_connection`, `run_aqicn_measurements_ingestion`, `dbt_run_staging`.
- In `dag_transform.py`, dbt tasks are explicitly named: `dbt_deps`, `dbt_seed`, `dbt_run_staging`, `dbt_run_intermediate`, `dbt_run_marts`, `dbt_test`.
### DAG Schedule & Concurrency
### Environment Variables in DAGs
### Task Grouping & Dependencies
- Use `>>` and `<<` operators for linear chains.
- Use `[task_a, task_b] >> task_c` for parallel fan-in.
- Example from `dag_ingest_hourly`:
### Docker Services Used by Airflow
- **ClickHouse**: `clickhouse:8123` (HTTP), `clickhouse:9000` (Native TCP)
- **PostgreSQL**: `postgres:5432` (Airflow metadata DB)
- **dbt project**: mounted at `/opt/dbt/dbt_tranform` inside containers
- **Python jobs**: mounted at `/opt/python/jobs` inside containers
- **Airflow connections**: configured via `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` env var in `docker-compose.yml`
### DAG-Level Retries
- Default: 2 retries with 5-minute delay (`timedelta(minutes=5)`).
- Historical backfill DAG: 10-minute retry delay.
- No SLA is explicitly defined in DAG code.
## 5. Docker Conventions
### Docker Compose (`docker-compose.yml`)
- **Version**: `3.8`
- **Network**: Named `air-quality-network` (default network).
- **Services**:
### Healthchecks
| Service | Check | Interval | Timeout | Retries |
|---|---|---|---|---|
| clickhouse | `wget --spider -q localhost:8123/ping` | 10s | 5s | 5 |
| postgres | `pg_isready -U airflow` | 10s | 5s | 5 |
| airflow-webserver | `curl --fail http://localhost:8080/api/v2/monitor/health` | 30s | 10s | 5 |
### Logging
### Airflow Dockerfile (`airflow/Dockerfile`)
- **Base image**: `apache/airflow:3.1.7`
- **User**: Defaults to `airflow` user (non-root).
- **System deps**: `curl` installed via `apt-get`.
- **Python deps**: Installed from `requirements.txt` via `pip install --no-cache-dir -r requirements.txt`.
- **Additional providers**: `apache-airflow-providers-http`, `apache-airflow-providers-sqlite`, `apache-airflow-providers-postgres`.
- **dbt project**: Copied to `/opt/dbt/dbt_tranform` with `chown airflow:airflow`.
- **Entrypoint**: `airflow/config/entrypoint.sh` (handles `db migrate`, creates log directories).
### Airflow Entrypoint Script
- Runs `airflow db migrate` on startup, falls back to `airflow db init` if migration fails.
- Defaults to `api-server` command (Airflow 3.x change from `webserver`).
- Creates required log directories: `dag_processor`, `dag_processor_manager`, `scheduler`, `triggerer`.
### Docker Socket Mount
## 6. Environment Variable Naming
### Pattern
- **Uppercase**, words separated by underscores.
- Namespaced by component where needed.
### Core Variables
### API Tokens
### Airflow Variables
### dbt Variables
### Python Jobs Variable
### `.env` vs `.env.dev`
- `.env` — production-style defaults checked into source control (with placeholder tokens).
- `.env.dev` — local development overrides (also checked in, but contains dev-specific values).
- **Never commit actual secrets** to `.env`; use secrets management in deployed environments.
## 7. Git Conventions
### Commit Message Rules
- **KHÔNG sử dụng `Co-Authored-By`** trong commit message. Mọi commit đều là của developer, không ghi nhận AI.
- Sử dụng tiếng Việt cho commit message.
- Theo format **Conventional Commits**: `<type>: <mô tả ngắn gọn>`
  - `feat:` — tính năng mới
  - `fix:` — sửa lỗi
  - `refactor:` — tái cấu trúc code
  - `docs:` — cập nhật tài liệu
  - `chore:` — công việc bảo trì (deps, config, CI)
  - `test:` — thêm hoặc sửa test
  - `style:` — thay đổi format, không ảnh hưởng logic
- Commit message ngắn gọn, rõ ràng, tập trung vào **what** và **why**.
- Mỗi commit nên là một đơn vị thay đổi logic (atomic commit).
### Branch Naming
- `feature/<tên-tính-năng>` — nhánh phát triển tính năng
- `fix/<tên-lỗi>` — nhánh sửa lỗi
- `chore/<mô-tả>` — nhánh bảo trì
## 8. `.gitignore` Patterns
### Key Exclusion Rules
- All `venv/` directories are excluded recursively.
- All `__pycache__/` directories are excluded.
- All `*.pyc` files are excluded.
- `airflow/logs/` and `logs/` are excluded.
- `.env` and `*.env.*` are excluded (secrets).
- `clickhouse-data/` and `data/` are excluded (persistent data).
- `nifi/state/` and `nifi/*.log` are excluded.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## System Overview
```
```
## Data Flow
### 1. Sensors.Community Data Path
```
```
### 2. AQICN Data Path
```
```
### 3. dbt Transformation Path
```
```
## Airflow DAGs
### dag_ingest_hourly (schedule: `0 * * * *`)
```
```
- Uses Airflow 3 TaskFlow API (`@dag`, `@task` decorators)
- Env vars captured at execution time (not parse time) via `get_job_env_vars()`
- All env vars injected into subprocess via `env.copy()`
### dag_transform (schedule: `30 * * * *`)
```
```
### dag_metadata_update (schedule: daily at 01:00)
- Runs Sensors.Community sensors and AQICN stations metadata refresh
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
<!-- GSD:architecture-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## Developer Preferences

- **Ngôn ngữ giao tiếp**: Tiếng Việt cho tất cả các cuộc trò chuyện với developer. Mọi phản hồi, commit message, tài liệu, và giao tiếp đều sử dụng tiếng Việt.

## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd:quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd:debug` for investigation and bug fixing
- `/gsd:execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd:profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
