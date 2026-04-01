# External Integrations

Vietnam Air Quality Data Platform — all external APIs, database connections, and environment variables.

---

## External APIs

### OpenAQ API

| Property | Value |
|----------|-------|
| Base URL | `https://api.openaq.org` |
| Auth method | `X-API-KEY` request header |
| Token env var | `OPENAQ_API_TOKEN` |
| Rate limit | ~48 requests/minute (safety margin within 60/min limit; 2000/hour hard cap) |
| Python client | `PaginatedAPIClient` from `python_jobs/common/api_client.py` |
| Rate limiter | `create_openaq_limiter()` → `TokenBucketRateLimiter(rate_per_second=0.8, burst_size=4, requests_per_minute=48.0)` |
| Pagination | `PaginatedAPIClient.fetch_all()` iterates via `page`/`limit` params |

**OpenAQ endpoints used** (via Python jobs in `python_jobs/jobs/openaq/`):

| Job | Endpoint | Data ingested |
|-----|----------|---------------|
| `ingest_parameters.py` | `/v3/parameters` | Parameter definitions (pollutant names, units) |
| `ingest_locations.py` | `/v3/locations?country=VN` | Location metadata for Vietnam |
| `ingest_sensors.py` | `/v3/locations/{id}/sensors` | Sensor metadata per location |
| `ingest_measurements.py` | `/v3/measurements?location_id={id}` | Measurement values and coverage |

**Token sourcing**:
- In Docker Compose, passed as `OPENAQ_API_TOKEN=${OPENAQ_API_TOKEN}` to all Airflow services
- Read in Python jobs via `os.environ.get("OPENAQ_API_TOKEN")`
- In `python_jobs/common/config.py`, `APIConfig.openaq_token` reads from `OPENAQ_API_TOKEN`

---

### AQICN (World Air Quality Index) API

| Property | Value |
|----------|-------|
| Base URL | `https://api.waqi.info` |
| Auth method | `token` query parameter (e.g., `?token=...`) |
| Token env var | `AQICN_API_TOKEN` |
| Rate limit | ~60 requests/minute (conservative default; varies by subscription) |
| Python client | `APIClient` from `python_jobs/common/api_client.py` |
| Rate limiter | `create_aqicn_limiter()` → `TokenBucketRateLimiter(rate_per_second=1.0, burst_size=5, max_delay=60.0)` |
| Station list | Crawled from `https://aqicn.org/city/vietnam/` (historical station IDs) |

**AQICN endpoints used** (via Python jobs in `python_jobs/jobs/aqicn/`):

| Job | Endpoint | Data ingested |
|-----|----------|---------------|
| `ingest_stations.py` | `/feed/@{station_id}/?token=...` | Station metadata (name, geo, AQI) |
| `ingest_measurements.py` | `/feed/@{station_id}/?token=...` | Current measurement + iaqi pollutant values |
| `ingest_forecast.py` | `/feed/@{station_id}/?token=...` | `forecast.daily` array (pm10, pm25, uvi, etc.) |

Station IDs are discovered via `crawl.html` (a static file in the repo root) which mirrors `https://aqicn.org/city/vietnam/` showHistorical data.

**Token sourcing**:
- In Docker Compose, passed as `AQICN_API_TOKEN=${AQICN_API_TOKEN}` to all Airflow services
- Read in Python jobs via `os.environ.get("AQICN_API_TOKEN")`
- In `python_jobs/common/config.py`, `APIConfig.aqicn_token` reads from `AQICN_API_TOKEN`

---

## Airflow Connections

### `clickhouse_default`

| Property | Value |
|----------|-------|
| `conn_id` | `clickhouse_default` |
| `conn_type` | `http` |
| Host | `clickhouse` (Docker service name) |
| Port | `8123` (ClickHouse HTTP interface) |
| Schema/Database | `air_quality` (from `CLICKHOUSE_DB`) |
| Login | `admin` (from `CLICKHOUSE_USER`) |
| Password | `admin123456` (from `CLICKHOUSE_PASSWORD`) |

**How it's configured**:
1. **Via Docker environment variable** (all Airflow services):
   ```
   AIRFLOW_CONN_CLICKHOUSE_DEFAULT=http://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@clickhouse:8123/?database=${CLICKHOUSE_DB}
   ```
2. **Via Python script**: `airflow/config/setup_connections.py` creates the connection programmatically using the Airflow ORM (`settings.Session()`, `Connection` model). The script reads `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB` from environment variables.

---

## Database Connections

### ClickHouse

| Property | Value |
|----------|-------|
| Host | `clickhouse` (Docker service name); `localhost` (env default for local dev) |
| HTTP Port | `8123` |
| Native TCP Port | `9000` (available but not used by the application) |
| Database | `air_quality` |
| User | `admin` |
| Password | `admin123456` |
| Auth | `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` enabled |
| Tables | 7 raw tables + dbt staging/intermediate/marts |

**Connection used by**:
- Python ingestion jobs → `ClickHouseWriter` class (`python_jobs/common/clickhouse_writer.py`) uses HTTP `POST` to `http://user:password@host:port/?database=db`
- dbt → `profiles.yml` uses `host={{ env_var('CLICKHOUSE_HOST', 'clickhouse') }}` etc.; LZ4 compression enabled
- Airflow DAGs → direct HTTP health check via `requests.get(http://host:port/ping)` in task code

### PostgreSQL

| Property | Value |
|----------|-------|
| Image | `postgres:15` |
| Container | `airflow-postgres1` |
| Port | `5432` |
| Database | `airflow` |
| User | `airflow` |
| Password | `airflow` |
| Volume | `./airflow/data/postgres:/var/lib/postgresql/data` |
| Health check | `pg_isready -U airflow` |

**Connection string** (used by all Airflow services):
```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```
Configured in `airflow/config/airflow.cfg` under `[database]` and overridden via Docker environment variables in all Airflow service containers.

---

## Environment Variables

### ClickHouse (`docker-compose.yml` / `.env`)

| Variable | Example Value | Description |
|----------|---------------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | Host (overridden to `clickhouse` in Docker networking) |
| `CLICKHOUSE_PORT` | `8123` | HTTP port |
| `CLICKHOUSE_USER` | `admin` | Database user |
| `CLICKHOUSE_PASSWORD` | `admin123456` | Database password |
| `CLICKHOUSE_DB` | `air_quality` | Database name |

### API Tokens (`docker-compose.yml` / `.env`)

| Variable | Description |
|----------|-------------|
| `OPENAQ_API_TOKEN` | OpenAQ API key (`X-API-KEY` header) |
| `AQICN_API_TOKEN` | AQICN API token (query param `?token=`) |

### Airflow Secrets (`docker-compose.yml` / `.env`)

| Variable | Purpose |
|----------|---------|
| `AIRFLOW_API_SECRET_KEY` | Airflow API secret (shared across all Airflow services) |
| `AIRFLOW_API_AUTH_JWT_SECRET` | JWT signing secret (shared across all Airflow services) |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Webserver secret key (shared across all Airflow services) |

### dbt / Python Jobs (`docker-compose.yml`)

| Variable | Value | Description |
|----------|-------|-------------|
| `DBT_PROFILES_DIR` | `/opt/dbt/dbt_tranform` | dbt profiles directory (same as project dir) |
| `CLICKHOUSE_HOST` | `clickhouse` | Passed to dbt environment |
| `CLICKHOUSE_PORT` | `8123` | Passed to dbt environment |
| `CLICKHOUSE_USER` | `${CLICKHOUSE_USER}` | Passed to dbt environment |
| `CLICKHOUSE_PASSWORD` | `${CLICKHOUSE_PASSWORD}` | Passed to dbt environment |
| `CLICKHOUSE_DB` | `air_quality` | Passed to dbt environment |
| `PYTHON_JOBS_DIR` | `/opt/python/jobs/` | Mount path for Python jobs in containers |

### Airflow Internal (`docker-compose.yml`)

| Variable | Value | Description |
|----------|-------|-------------|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Task execution executor |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | PostgreSQL backend connection |
| `AIRFLOW__CORE__FERNET_KEY` | (empty) | Disables encryption of sensitive fields |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | New DAGs start paused |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Disable built-in example DAGs |
| `AIRFLOW__API__AUTH_BACKENDS` | `airflow.api.auth.backend.basic_auth` | Auth backend |
| `AIRFLOW__CORE__ENABLE_AIRFLOW_TASKS_STDOUT` | `true` | Task stdout logging |
| `AIRFLOW__CORE__STRICT_DATABASE_EXPRESSIONS` | `false` | Relaxed SQL expression checking |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-webserver:8080/execution/` | Airflow 3.x execution API |
| `AIRFLOW__API__SECRET_KEY` | `${AIRFLOW_API_SECRET_KEY}` | API authentication |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `${AIRFLOW_API_AUTH_JWT_SECRET}` | JWT auth |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | `${AIRFLOW_WEBSERVER_SECRET_KEY}` | Webserver security |

---

## API Tokens Summary

| Token | Env Var | Used By | Format |
|-------|---------|---------|--------|
| OpenAQ API key | `OPENAQ_API_TOKEN` | All Airflow services (env), Python jobs (via `config.py`), `api_client.py` (`X-API-KEY` header) | Hex string (64 chars) |
| AQICN API token | `AQICN_API_TOKEN` | All Airflow services (env), Python jobs (via `config.py`), `api_client.py` (query param `?token=`) | Hex string (40 chars) |

> **Warning**: Both tokens are stored in plain text in `.env`. Never commit `.env` to version control (it is gitignored). In production, use a secrets manager (e.g., HashiCorp Vault, AWS Secrets Manager) and inject values at container runtime.

> **`.env.dev` note**: This file contains a different OpenAQ key (`OPENAQ_API_KEY`) and AQICN token (`AQICN_TOKEN`) as well as NiFi credentials. NiFi is referenced in `.env.dev` but is not deployed in the current `docker-compose.yml`.
