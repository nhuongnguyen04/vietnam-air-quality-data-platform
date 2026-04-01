# Testing

## dbt Tests

### Schema Tests (YAML-defined)
Defined in `schema.yml` files alongside models:

**OpenAQ sources** (`_openaq__sources.yml`):
- `not_null` tests: location_id, sensor_id, parameter_id, ingest_time, latitude, longitude
- `unique` tests: location_id, parameter_id

**OpenAQ staging** (`_staging_openaq_schema.yml`):
- Column nullability and type tests

**AQICN staging** (`_staging_aqicn_schema.yml`):
- `not_null` on is_vietnam (measurements + stations)
- `accepted_values` on is_vietnam (True only)

**Intermediate** (`_intermediate_schema.yml`):
- AQI calculation model tests

**Marts** (`_marts_schema.yml`):
- Core air quality model tests

### Test Execution
```bash
# Via dag_transform DAG (automatic, hourly)
dbt test --profiles-dir /opt/dbt/dbt_tranform --target production

# Manual
cd dbt/dbt_tranform && dbt test
```

## Docker Healthchecks

**ClickHouse**: `wget --spider -q localhost:8123/ping` (interval: 10s, retries: 5)
**PostgreSQL**: `pg_isready -U airflow` (interval: 10s, retries: 5)
**Airflow webserver**: `curl --fail http://localhost:8080/api/v2/monitor/health` (interval: 30s, retries: 5)

## API-Level Validation

- `ensure_metadata()` DAG task checks `raw_openaq_locations` row count before proceeding
- `log_dbt_stats()` queries `system.tables` to verify stg_/int_/fct_ counts after each run
- ClickHouse ping checks before ingestion DAG runs

## Linting

**Python**: `sqlfluff` in requirements.txt — used for SQL linting
**SQL (dbt)**: sqlfluff-templater-dbt for dbt model linting

## No Unit/Integration Tests Found

- No `tests/` directory under `python_jobs/`
- No pytest/unittest configuration found
- DAGs rely on production execution for validation
- No CI/CD pipeline file found (no GitHub Actions, no `.github/`)
- No `pytest.ini`, `setup.cfg`, or `pyproject.toml` with test config

## Recommendations (Technical Debt)

- Add unit tests for `api_client.py`, `clickhouse_writer.py`, `rate_limiter.py`
- Add integration tests for Python jobs (mock API responses)
- Add `dbt source freshness` for raw tables
- Add GitHub Actions workflow for CI (lint → test → deploy)
