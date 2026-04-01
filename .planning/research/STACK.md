# Stack Research — Vietnam Air Quality Data Platform (Refactor)

**Domain:** Multi-source air quality data engineering platform
**Researched:** 2026-04-01
**Confidence:** MEDIUM-HIGH (based on existing codebase + public API docs)

## Context

Existing stack is fixed — ClickHouse 25.12, dbt-core 1.10.13, dbt-clickhouse 1.9.5, Airflow 3.1.7, Docker Compose. This research covers the NEW components being added.

---

## What's Already Decided (Keep As-Is)

| Component | Version | Decision |
|-----------|---------|----------|
| ClickHouse | 25.12 | Database — no change |
| dbt-core | 1.10.13 | Transformation — refactor models, keep version |
| dbt-clickhouse | 1.9.5 | dbt adapter — keep version |
| Airflow | 3.1.7 | Orchestration — optimize DAGs, keep version |
| requests | (via Airflow) | HTTP client — existing |
| tenacity | (in requirements.txt) | Retry/backoff — existing, extend for new sources |
| python-json-logger | 2.0.7 | Structured logging — existing |
| clickhouse-connect | 0.9.2 | ClickHouse Python client — existing |

---

## New Components

### 1. EPA AirNow API Client

No official Python SDK. Build on top of existing `api_client.py` + `requests`.

| Item | Value | Confidence |
|------|-------|------------|
| **API base** | `https://airnowapi.org` | HIGH |
| **Auth** | Free API key from `docs.airnowtech.org` | HIGH |
| **Format** | REST JSON (preferred over SOAP) | HIGH |
| **Rate limit** | Free tier: 1 req/sec | HIGH |
| **Key endpoints** | `/v1/observationByCity`, `/v1/observationByLatLong`, `/v1/forecastByCity` | HIGH |
| **Vietnam coverage** | Uses lat/lon queries; Bangkok API gateway closest to VN | MEDIUM |
| **Output table** | `raw_airnow_measurements`, `raw_airnow_sites` | HIGH |

**Implementation:**
- Extend `python_jobs/api_client.py` with `AirNowClient` class
- Reuse existing `TokenBucketRateLimiter` — set to 1 req/sec for AirNow
- Date format: `YYYYMMDDHH` (e.g. `2026040112`) — different from other sources
- Response parsing: parse JSON, map to canonical schema, write to ClickHouse
- AQI comes pre-calculated in AirNow response — store as-is alongside raw AQI for comparison

**What NOT to do:**
- Don't use SOAP endpoints (deprecated)
- Don't assume AQI formula matches EPA standard (AirNow pre-calculates; others may not)
- Don't hardcode city names — use lat/lon bounding box queries for Vietnam

---

### 2. Sensors.Community / Luftdaten API Client

| Item | Value | Confidence |
|------|-------|------------|
| **API base** | `https://api.luftdaten.info/v1/` | HIGH |
| **Auth** | None required | HIGH |
| **Rate limit** | None (request polite usage) | HIGH |
| **Key endpoints** | `GET /v1/sensors` (with bbox filter), `GET /v1/push-v2/sensors-data` | HIGH |
| **Hardware** | SDS011 (PM2.5, PM10), DHT22 (temp/humidity), BMP180 (pressure) | HIGH |
| **Sensor accuracy** | Community-grade, NOT EPA-equivalent | HIGH |
| **Vietnam coverage** | Mostly peri-urban/rural — complements regulatory stations | MEDIUM |
| **Output table** | `raw_sensorscm_measurements` | HIGH |

**Implementation:**
- New module: `python_jobs/jobs/sensorscm/ingest_measurements.py`
- Filter by Vietnam bounding box: `lat_min=8.4&lat_max=23.4&lon_min=102.1&lon_max=109.5`
- No auth needed — public API
- Add `sensor_quality_tier = 'community'` to all records
- Flag implausible values: PM2.5 outside 0–500 µg/m³, stations outside Vietnam bbox
- Run on `*/10 * * * *` schedule (high-frequency source, near-real-time target)

**What NOT to do:**
- Don't treat Sensors.Community data as equivalent to EPA AirNow
- Don't ingest without quality flagging — community sensors produce noise
- Don't use for regulatory/trust-critical AQI reporting without disclaimer

---

### 3. Apache Superset (New Deployment)

| Item | Value | Confidence |
|------|-------|------------|
| **Image** | `apache/superset:4.x` | HIGH |
| **ClickHouse driver** | Built-in SQLAlchemy + `clickhouse-connect` (already in requirements) | HIGH |
| **Connection string** | `clickhouse+native://clickhouse:9000/airquality` or `clickhouse+http://clickhouse:8123/airquality` | HIGH |
| **Auth** | Public read-only; admin user on first run | MEDIUM |
| **Port** | 8088 (override via `SUPERSET_WEBSERVER_PORT`) | HIGH |

**Installation:**
```yaml
# Add to docker-compose.yml
superset:
  image: apache/superset:4.x
  container_name: superset
  env_file: .env
  environment:
    SUPERSET_CONFIG_PATH: /app/superset_config.py
    DATABASE_CONNECTION: clickhouse+native://clickhouse:9000/airquality
  volumes:
    - ./superset:/app
  ports:
    - "8088:8088"
  depends_on:
    - clickhouse
```

**Configuration:**
- `superset_config.py`: database connection, feature flags, secret key
- Bootstrap: `superset-init.sh` to create admin user and import ClickHouse connection
- Cache: configure `CACHE_TIME` per dataset (15 min TTL for near-real-time dashboards)
- Query timeout: set `SQLALCHEMY_ENGINE_OPTIONS.connect_args.connect_timeout = 30`

**What NOT to do:**
- Don't let Superset query raw tables — only query mart tables
- Don't use `clickhouse-city` driver (deprecated)
- Don't set unlimited query timeouts — ClickHouse queries against large tables can hang

---

### 4. Grafana (New Deployment)

| Item | Value | Confidence |
|------|-------|------------|
| **Image** | `grafana/grafana:11.x` | HIGH |
| **ClickHouse plugin** | `grafana-clickhouse-datasource` (community) or Altinity plugin | HIGH |
| **Airflow metadata** | Connect directly to `postgres:5432` (Airflow metadata DB) | HIGH |
| **Port** | 3000 (override via `GF_SERVER_HTTP_PORT`) | HIGH |

**Installation:**
```yaml
# Add to docker-compose.yml
grafana:
  image: grafana/grafana:11.x
  container_name: grafana
  environment:
    GF_SECURITY_ADMIN_USER: admin
    GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
    GF_SERVER_ROOT_URL: http://localhost:3000
  volumes:
    - ./grafana/provisioning:/etc/grafana/provisioning
    - ./grafana/dashboards:/var/lib/grafana/dashboards
  ports:
    - "3000:3000"
  depends_on:
    - postgres
    - clickhouse
```

**Dashboards to build:**
1. **Pipeline Health**: DAG success/failure rate, task duration trend, last successful run per DAG
2. **Data Freshness**: `max(ingest_time)` per source, rows ingested per hour, null rate in key columns
3. **Ingestion Latency**: time between data source timestamp and ClickHouse write
4. **System Metrics**: ClickHouse/Airflow/PostgreSQL CPU, memory, disk

**What NOT to do:**
- Don't build alerting rules on raw tables — use source health control table
- Don't alert on every threshold breach immediately — configure deduplication windows to avoid alert fatigue

---

### 5. OpenMetadata (New Deployment)

| Item | Value | Confidence |
|------|-------|------------|
| **Image** | `openmetadata/server:1.1.x` | HIGH |
| **Backend** | Bundled MySQL + Elasticsearch (via Docker Compose) | HIGH |
| **ClickHouse connector** | Native OpenMetadata ClickHouse connector | HIGH |
| **dbt integration** | `openmetadata-ingestion` connector reads `target/manifest.json` | HIGH |
| **RAM requirement** | Minimum 4GB; recommend 8GB | HIGH |
| **Port** | 8585 (web UI) | HIGH |

**Installation:**
```yaml
# Add to docker-compose.yml — use official OpenMetadata docker-compose as reference
# openmetadata/docker — includes server + MySQL + Elasticsearch
openmetadata:
  image: openmetadata/server:1.1.x
  container_name: openmetadata
  environment:
    DB_HOST: openmetadata-mysql
    DB_PORT: 3306
    # ... (see openmetadata/docker-compose for full config)
  ports:
    - "8585:8585"
  depends_on:
    - openmetadata-mysql
    - openmetadata-elasticsearch
```

**ClickHouse Connector Config:**
```json
{
  "type": "clickhouse",
  "host": "${CLICKHOUSE_HOST}",
  "port": 8123,
  "username": "${CLICKHOUSE_USER}",
  "password": "${CLICKHOUSE_PASSWORD}",
  "database": "airquality",
  "includeDatabases": "airquality",
  "markDeletedTables": true
}
```

**What NOT to do:**
- Don't use JDBC generic connector — use OpenMetadata's native ClickHouse connector
- Don't install OpenMetadata without enough RAM — it will crash
- Don't skip the dbt integration — lineage is OpenMetadata's main value

---

### 6. Near-Real-Time Ingestion Patterns

**Achievable latency targets:**

| Source | Ingestion frequency | Target latency |
|--------|-------------------|----------------|
| Sensors.Community | `*/10 * * * *` | 10–15 min |
| AQICN | hourly | 30–45 min |
| EPA AirNow | hourly | 30–60 min |
| MONRE | daily | 1–24 hr |

**Implementation approach:**
- Reuse existing Airflow TaskFlow API — no new infrastructure
- Add parallel task execution: all sources in same DAG use `@task` (parallel) not sequential `>>`
- Source health control table in ClickHouse: `ingestion_control(source, last_run, last_success, records_count, lag_seconds)`
- Update control table after each successful ingestion run (Airflow XCom or direct ClickHouse insert)
- Separate `dag_ingest_hourly` (IQAir, AirNow) from `dag_sensorscm_poll` (`*/10 * * * *`)

**What NOT to do:**
- Don't use `while True: sleep()` loops — no Airflow retry semantics, no observability
- Don't run all sources in the same task sequentially — parallelize or they compound latency
- Don't assume API stability — all sources need retry + fallback logic

---

## Docker Compose Changes Summary

| Service | Image | Port | Notes |
|---------|-------|------|-------|
| superset | `apache/superset:4.x` | 8088 | New — needs `superset_config.py` + init script |
| grafana | `grafana/grafana:11.x` | 3000 | New — needs provisioning/dashboards dirs |
| openmetadata | `openmetadata/server:1.1.x` | 8585 | New — needs MySQL + ES backends |
| openmetadata-mysql | (from OM docker-compose) | 3306 | New |
| openmetadata-elasticsearch | (from OM docker-compose) | 9200 | New |

**Port conflict check:**
- ClickHouse: 8123, 9000
- Airflow webserver: 8090→8080
- PostgreSQL: 5432
- Superset: 8088 (ensure no conflict with Airflow's 8080)
- OpenMetadata: 8585
- Grafana: 3000

---

## Resource Planning

| Service | Min RAM | Recommended RAM | CPU |
|---------|---------|-----------------|-----|
| ClickHouse | 2GB | 4GB+ | 2 cores |
| Airflow (all services) | 2GB | 4GB | 2 cores |
| PostgreSQL | 512MB | 1GB | 1 core |
| Superset | 1GB | 2GB | 1 core |
| Grafana | 200MB | 512MB | 1 core |
| OpenMetadata | 4GB | 8GB | 2 cores |
| **Total** | **~10GB** | **~20GB** | **9 cores** |

Add resource limits to all services in docker-compose.yml from day one.

---

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Kafka / Flink / Pulsar | Out of scope; batch architecture sufficient | Airflow DAGs + shorter intervals |
| NiFi | Referenced in .env but not deployed | Python ingestion jobs |
| Commercial BI (Tableau, Power BI) | Already chose Superset | Superset |
| `clickhouse-city` driver | Deprecated | `clickhouse-connect` (already in requirements) |
| ODBC for Grafana | Complex setup | Grafana ClickHouse community plugin |
| Airflow CEL executor | Requires Redis; LocalExecutor sufficient | LocalExecutor |
| Spark / EMR | Overkill for this scale | dbt + ClickHouse aggregation |
| JDBC generic for OpenMetadata | Doesn't work well with ClickHouse | Native OpenMetadata ClickHouse connector |
| AirNow SOAP API | Deprecated | AirNow REST JSON API |

---

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|------------------------|
| AirNow REST API | AirNow SOAP | Never — SOAP is deprecated |
| Grafana community ClickHouse plugin | Grafana Infinity | Infinity is more general but less optimized; plugin preferred |
| OpenMetadata native dbt ingestion | Manual catalog curation | Manual only for small tables; dbt ingestion scales |
| Sensors.Community | PurpleAir API | PurpleAir has auth + rate limits; Sensors.Community is free |

---

*Stack research for: Vietnam Air Quality Data Platform refactor*
*Researched: 2026-04-01*
