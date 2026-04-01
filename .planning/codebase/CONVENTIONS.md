# Coding Conventions — Vietnam Air Quality Data Platform

This document records the established conventions and standards used across the Vietnam Air Quality Data Platform repository. All contributors are expected to follow these guidelines.

---

## 1. Python Style

### Tooling & Configuration
The project does **not** use a project-level `.flake8`, `setup.cfg`, or `pyproject.toml` for linting rules. Python style is enforced informally via conventions below.

**Key references** from the codebase:
- `requirements.txt` at the repo root is the single source of truth for Python dependencies.
- No per-DAG or per-module type-checking config files exist.

### Code Style Rules
- **PEP 8** compliance is expected for all Python code.
- **Docstrings**: Every module and class should have a docstring. Use triple-quoted strings.
  ```python
  """
  Module name — brief description.

  Author: Air Quality Data Platform
  """
  ```
- **Imports**: Standard library first, then third-party, then local.
  ```python
  import time
  import json
  import logging
  from typing import Optional, Dict, Any, Callable
  import requests
  from requests.adapters import HTTPAdapter

  from python_jobs.common.api_client import APIClient
  ```
- **Type hints**: Use type hints for function parameters and return types where practical.
- **Logging**: Use `logging.getLogger(__name__)` — do not use `print()` for application logging except in lightweight Airflow task wrappers.
  ```python
  logger = logging.getLogger(__name__)
  logger.info("Processing completed")
  ```
- **Variable naming**: `snake_case` for variables and functions, `PascalCase` for classes.
- **Constants**: `UPPERCASE_WITH_UNDERSCORES` at module level.

### Python Package Structure
- Python ingestion jobs live in `/python_jobs/jobs/<source>/` (e.g., `python_jobs/jobs/openaq/`, `python_jobs/jobs/aqicn/`).
- Shared utilities live in `python_jobs/common/` (e.g., `api_client.py`, `rate_limiter.py`, `clickhouse_writer.py`, `config.py`, `logging_config.py`).
- Models live in `python_jobs/models/` (e.g., `openaq_models.py`, `aqicn_models.py`).
- Every package directory must contain an `__init__.py` file.

### CLI Arguments
Ingestion scripts use **argparse** for CLI argument parsing. Standard arguments include:
- `--mode`: `incremental` (default for hourly runs), `historical` (for backfills), or `rewrite` (for metadata refresh).
- `--start-date`, `--end-date`: Date range for historical ingestion.
- `--days-back`: Number of days to backfill for AQICN.

---

## 2. SQL Naming Conventions (dbt)

### Project Setup
- **Project name**: `dbt_tranform`
- **Project file**: `dbt/dbt_tranform/dbt_project.yml`
- **Profiles file**: `dbt/dbt_tranform/profiles.yml`
- **dbt version**: `dbt-core==1.10.13`, `dbt-clickhouse==1.9.5`
- **SQL linter**: `sqlfluff==3.5.0` with `sqlfluff-templater-dbt==3.5.0`

### dbt Model Naming Prefixes
Models are organized into three layers, each with a mandatory prefix:

| Layer | Prefix | Materialization | Description |
|---|---|---|---|
| Staging | `stg_<source>__<entity>` | `view` | Raw source cleaning, type casting, Vietnam filtering |
| Intermediate | `int_<concept>__<entity>` | `view` | Cross-source unions, AQI calculations, quality metrics |
| Marts | `mart_<domain>__<entity>` | `table` | Analytics-ready tables |

**Examples:**
```
stg_aqicn__measurements
stg_aqicn__stations
stg_aqicn__forecast
stg_openaq__measurements
stg_openaq__locations
stg_openaq__sensors
stg_openaq__parameters

int_unified__measurements
int_unified__stations
int_aqi_calculations
int_data_quality
int_forecast_accuracy

mart_air_quality__daily_summary
mart_air_quality__hourly
mart_air_quality__stations
mart_analytics__trends
mart_analytics__geographic
mart_analytics__forecast_accuracy
mart_kpis__air_quality_index
mart_kpis__pollutant_concentrations
mart_kpis__data_coverage
```

### dbt Source Naming
Sources are named after the data provider and live in `models/staging/<source>/_sources.yml`:
```
{{ source('aqicn', 'raw_aqicn_measurements') }}
{{ source('openaq', 'raw_openaq_locations') }}
```

### dbt Model Config
Each SQL model file starts with a Jinja config block:
```sql
{{ config(materialized='view') }}
```
- Staging and intermediate models: `materialized='view'`
- Marts models: `materialized='table'`

### SQL Style
- Use CTEs (`with source as (...)`) for readability.
- Use `toFloat64OrNull()`, `toInt32OrNull()` for safe type casting (ClickHouse dialect).
- Use explicit schema prefixes: `{{ source('...') }}` and `{{ ref('...') }}`.
- Use Jinja macros for repeated operations (e.g., `{{ parse_unix_timestamp('col') }}`).
- dbt package install path: `dbt_packages` (configurable via `DBT_PACKAGES_INSTALL_PATH` env var).

---

## 3. YAML Schema Conventions (dbt `schema.yml`)

Each model directory contains a `schema.yml` file with the naming convention `_<model_group>_<descriptor>.yml` (prefixed with `_` to indicate it is a partial/schema-only file).

### Schema File Naming
```
models/
  staging/
    aqicn/
      _aqicn__sources.yml          ← dbt sources
      _staging_aqicn_schema.yml    ← model column tests & docs
    openaq/
      _openaq__sources.yml
      _staging_openaq_schema.yml
  intermediate/
    _intermediate_schema.yml
  marts/
    _marts_schema.yml
```

### Schema.yml Structure
All schema.yml files use **version 2** format:

```yaml
version: 2

models:
  - name: stg_aqicn__measurements
    description: "Staged AQICN measurements data - filtered for Vietnam..."
    columns:
      - name: station_id
        description: "AQICN station ID"
        tests:
          - not_null
      - name: pollutant
        description: "Standardized pollutant name"
        tests:
          - not_null
      - name: value
        description: "Pollutant concentration value (converted to Float64)"
```

### Tests Used
- `not_null` — primary keys and required fields
- `unique` — identifier columns (e.g., `station_id`, `location_id`)
- `accepted_values` — enum-like fields
  ```yaml
  tests:
    - accepted_values:
        values: ['aqicn', 'openaq']
  ```

### Documentation
Every column **must** have a `description`. Descriptions should be concise and specify any type conversions applied.

---

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
All DAGs define `default_args`:
```python
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,   # historical & transform DAGs
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### Task Naming
- Use `snake_case` task names (function names decorated with `@task`).
- Tasks follow a `verb_noun` pattern: `check_clickhouse_connection`, `run_openaq_measurements_ingestion`, `dbt_run_staging`.
- In `dag_transform.py`, dbt tasks are explicitly named: `dbt_deps`, `dbt_seed`, `dbt_run_staging`, `dbt_run_intermediate`, `dbt_run_marts`, `dbt_test`.

### DAG Schedule & Concurrency
```python
@dag(
    default_args=default_args,
    description='...',
    schedule='0 * * * *',  # or None for manual-only
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', 'hourly', 'air-quality'],
)
```

### Environment Variables in DAGs
**Critical rule**: Environment variables for task execution must be captured in a **function** (not a module-level dict). This is because Airflow 3 parses DAGs in the `dag-processor` process but executes tasks in a separate task runner process — module-level dicts capture env vars at parse time, which may differ from the execution environment.

```python
def get_job_env_vars() -> dict:
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'airquality'),
        'OPENAQ_API_TOKEN': os.environ.get('OPENAQ_API_TOKEN', ''),
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
    }
```

### Task Grouping & Dependencies
- Use `>>` and `<<` operators for linear chains.
- Use `[task_a, task_b] >> task_c` for parallel fan-in.
- Example from `dag_ingest_hourly`:
  ```python
  check_clickhouse >> metadata >> [openaq, aqicn, forecast] >> completion
  ```

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

---

## 5. Docker Conventions

### Docker Compose (`docker-compose.yml`)
- **Version**: `3.8`
- **Network**: Named `air-quality-network` (default network).
- **Services**:
  - `clickhouse` — ClickHouse server 25.12
  - `postgres` — PostgreSQL 15 (Airflow metadata)
  - `airflow-webserver` (Airflow API server, port 8090→8080)
  - `airflow-scheduler`
  - `airflow-dag-processor`
  - `airflow-triggerer`
  - `airflow-permissions` (init container for directory permissions)

### Healthchecks
All services define healthchecks:

| Service | Check | Interval | Timeout | Retries |
|---|---|---|---|---|
| clickhouse | `wget --spider -q localhost:8123/ping` | 10s | 5s | 5 |
| postgres | `pg_isready -U airflow` | 10s | 5s | 5 |
| airflow-webserver | `curl --fail http://localhost:8080/api/v2/monitor/health` | 30s | 10s | 5 |

### Logging
All services use `json-file` logging driver with `max-size: 10m, max-file: 3`.

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
`- /var/run/docker.sock:/var/run/docker.sock` is mounted in all Airflow containers to enable Docker-based operators if needed.

---

## 6. Environment Variable Naming

### Pattern
- **Uppercase**, words separated by underscores.
- Namespaced by component where needed.

### Core Variables
```
CLICKHOUSE_HOST       # default: localhost / clickhouse
CLICKHOUSE_PORT       # default: 8123
CLICKHOUSE_USER       # default: admin
CLICKHOUSE_PASSWORD   # no default (secrets)
CLICKHOUSE_DB         # default: air_quality
```

### API Tokens
```
OPENAQ_API_TOKEN
AQICN_API_TOKEN
```

### Airflow Variables
```
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__API__SECRET_KEY
AIRFLOW__API_AUTH__JWT_SECRET
AIRFLOW__WEBSERVER__SECRET_KEY
AIRFLOW__CORE__FERNET_KEY=           # Empty (LocalExecutor only)
AIRFLOW_CONN_CLICKHOUSE_DEFAULT     # Pre-built connection URI
```

### dbt Variables
```
DBT_PROFILES_DIR       # default: /opt/dbt/dbt_tranform
DBT_PROJECT_DIR        # default: /opt/dbt/dbt_tranform
DBT_TARGET             # default: production
DBT_LOG_PATH
DBT_TARGET_PATH
DBT_PACKAGES_INSTALL_PATH
```

### Python Jobs Variable
```
PYTHON_JOBS_DIR        # default: /opt/python/jobs
```

### `.env` vs `.env.dev`
- `.env` — production-style defaults checked into source control (with placeholder tokens).
- `.env.dev` — local development overrides (also checked in, but contains dev-specific values).
- **Never commit actual secrets** to `.env`; use secrets management in deployed environments.

---

## 7. `.gitignore` Patterns

The repository root `.gitignore` defines the following patterns (excluding `venv/`):

```
# General
venv/
logs/

# Python / dbt
__pycache__/
*.pyc
dbt/target/
dbt/logs/
dbt/.dbt/

# Airflow
airflow/logs/
airflow/*.pid

# Env & secrets
.env
*.env.*
credentials.json

# NiFi
nifi/*.log
nifi/state/

# Editor
.vscode/
.idea/

# Agent / planning
.agent/
memory/
plans/
knowledge/
runbooks/
specs/
.cursor/

# Data directories
data/
clickhouse-data/
*.cfg
```

**Additional `.gitignore`** in `dbt/dbt_tranform/`:
```
target/
dbt_packages/
logs/
```

### Key Exclusion Rules
- All `venv/` directories are excluded recursively.
- All `__pycache__/` directories are excluded.
- All `*.pyc` files are excluded.
- `airflow/logs/` and `logs/` are excluded.
- `.env` and `*.env.*` are excluded (secrets).
- `clickhouse-data/` and `data/` are excluded (persistent data).
- `nifi/state/` and `nifi/*.log` are excluded.
