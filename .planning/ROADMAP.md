# Roadmap — Vietnam Air Quality Data Platform Refactor

**Milestone:** Refactor & Upgrade
**Phases:** 6 | **Plans:** 30 | **Granularity:** Standard
**Created:** 2026-04-01

---

## Overview

| # | Phase | Goal | Plans | Success Criteria |
|---|-------|------|-------|-----------------|
| 0 | Foundation & Stabilization | Stabilize brownfield baseline before adding new components | 5 | 5 |
| 1 | Multi-Source Ingestion | Replace OpenAQ; add AirNow, Sensors.Community, MONRE | 6 | 6 |
| 2 | dbt Refactor | Multi-source canonical mart layer | 5 | 5 |
| 3 | Visualization & Monitoring | Superset + Grafana deployment | 5 | 5 |
| 4 | OpenMetadata Integration | Data catalog + lineage + data quality | 4 | 4 |
| 5 | Alerting & Reporting | End-to-end alerting + automated reports | 5 | 4 |

**Critical path:** Phase 0 → Phase 1 → Phase 2 → Phase 3
**Parallelization:** Phase 3 (Superset + Grafana run in parallel), Phase 5 depends on Phase 2 + 3

**MONRE policy:** Phase 1 ships complete with AQICN + AirNow + Sensors.Community if MONRE is inaccessible. MONRE is never a blocker.

---

## Phase 0 — Foundation & Stabilization

**Goal:** Stabilize brownfield baseline before adding new components. Audit existing codebase to surface hardcoded assumptions and resource gaps.

### Plan 0.1 — Baseline Codebase Audit

**Owner:** data engineering
**Outputs:** Audit report listing OpenAQ schema assumptions, ClickHouse table inventory, dbt model inventory

**Tasks:**
- Inventory all ClickHouse raw tables: list schemas, dedup strategy, audit column coverage
- Inventory all dbt models: identify hardcoded OpenAQ column names, parameter assumptions, source filter conditions
- Inventory all Airflow DAGs: task dependencies, env var usage, failure handling
- Document findings in `.planning/codebase/AUDIT.md`

**Success criteria:**
1. Complete list of OpenAQ-specific column names hardcoded in dbt WHERE clauses or Python jobs
2. All ClickHouse raw table schemas documented with dedup key and MergeTree engine type
3. All Airflow DAG task dependencies mapped in a single diagram

---

### Plan 0.2 — Docker Compose Resource Hardening

**Owner:** data engineering
**Outputs:** Resource limits on all services, health checks configured

**Tasks:**
- Add `mem_limit` and `cpu_limit` to all Docker Compose services (ClickHouse: 4GB, Airflow: 2GB, PostgreSQL: 1GB)
- Add health checks to all services (`healthcheck:` in docker-compose.yml)
- Verify existing services start cleanly: `docker compose up -d && docker compose ps`
- Document minimum hardware requirements: 16GB RAM, 4 CPU cores

**Success criteria:**
1. `docker compose ps` shows all services healthy within 60s
2. `docker stats` shows no OOM events during startup
3. README updated with hardware requirements

---

### Plan 0.3 — CI Pipeline Bootstrap

**Owner:** data engineering
**Outputs:** GitHub Actions CI pipeline

**Tasks:**
- Create `.github/workflows/ci.yml`: lint → dbt compile → dbt test
- Create `docker-compose.test.yml`: minimal stack for CI (ClickHouse + test runner, no Superset/Grafana)
- Add `sqlfluff lint dbt/` and `ruff check python_jobs/` to lint step
- Block merge to main on CI failure

**Success criteria:**
1. `git push` with breaking lint triggers CI failure with clear error message
2. `dbt compile` runs successfully in CI against minimal ClickHouse
3. CI completes in <5 minutes for lint step, <10 minutes for full pipeline

---

### Plan 0.4 — AQICN-Only Stability Run

**Owner:** data engineering
**Outputs:** 7-day baseline run proving AQICN-only pipeline stability

**Tasks:**
- Disable OpenAQ ingestion (comment out OpenAQ tasks from `dag_ingest_hourly`)
- Run AQICN-only for 7 days: monitor DAG success rate, row counts, ClickHouse storage growth
- Verify no regression in AQICN data quality during this period
- Document baseline metrics: rows/day, storage growth rate, API call count

**Success criteria:**
1. 100% DAG success rate over 7 consecutive days
2. AQICN row count grows linearly with no super-linear spikes (no duplicates)
3. No alerts or OOM events in Grafana during baseline period

---

### Plan 0.5 — Ingestion Control Table

**Owner:** data engineering
**Outputs:** `ingestion.control` ClickHouse table

**Tasks:**
- Create `ingestion.control` table in ClickHouse:
  ```sql
  CREATE TABLE airquality.ingestion_control (
    source String,
    last_run DateTime,
    last_success DateTime,
    records_ingested UInt64,
    lag_seconds Int64,
    error_message String,
    updated_at DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(updated_at)
  ORDER BY source;
  ```
- Update control table after every ingestion DAG run via Airflow PythonOperator
- Expose `ingestion.control` as a Grafana data source for pipeline health dashboard

**Success criteria:**
1. `ingestion.control` row exists for each active source after first DAG run
2. `last_success` updates within 5 minutes of DAG completion
3. `lag_seconds` is within expected range (hourly sources: <3600s; Sensors.Community: <600s)

---

## Phase 1 — Multi-Source Ingestion

**Goal:** Replace OpenAQ with OpenWeather Air Pollution API, WAQI/World Air Quality Index, and Sensors.Community. AQICN stays as primary. All ingestion additive with zero risk to existing pipeline.

> **Note (2026-04-01):** EPA AirNow is US/Canada-only — no Vietnam coverage. MONRE has no public API. Both replaced by OpenWeather + WAQI per user decision.
> **Status (2026-04-01):** ✅ Phase 1 complete — all 5 plans executed.

**Depends on:** Phase 0

### Plan 0.00 — Test Infrastructure Stubs ✅

**Owner:** data engineering
**Executed:** 2026-04-01
**Outputs:** `tests/conftest.py`, `tests/test_openweather.py`, `tests/test_waqi.py`, `tests/test_sensorscm.py`, `tests/test_decommission.py`, `tests/test_rate_limiter.py`, `pytest.ini`

**Tasks:**
- Create `tests/conftest.py` with shared fixtures: `mock_clickhouse_client`, `mock_clickhouse_writer`, `mock_rate_limiter`, `env_vars`, `sample_openweather_response`, `sample_waqi_response`, `sample_sensorscm_response`
- Create test stubs for OpenWeather (5), WAQI (5), Sensors.Community (7), OpenAQ decommission (5), rate limiter/orchestration (7)
- Add `pytest.ini` at repo root with `testpaths = tests`
- Add `pytest` to `requirements.txt`

**Success criteria:**
1. ✅ `pytest tests/ --collect-only` produces zero errors — 29 tests collected
2. ✅ `pytest tests/ -q --tb=short` runs to completion — all 29 pass (placeholders expected)
3. ✅ All fixtures use real `python_jobs.common.*` classes where modules exist

---

### Plan 1.1 — OpenWeather Air Pollution Client

**Owner:** data engineering
**Outputs:** `python_jobs/jobs/openweather/ingest_measurements.py`, `raw_openweather_measurements`

**Tasks:**
- Create `python_jobs/jobs/openweather/` module
- Build `OpenWeatherClient` extending existing `api_client.py`: API base `https://api.openweathermap.org/data/2.5`
- Create ClickHouse raw table: `raw_openweather_measurements` (`ReplacingMergeTree(ingest_time)`, ORDER BY `(station_id, timestamp_utc, parameter)`)
- Poll city centroids: Hanoi (21.0°N/105.8°E), HCMC (10.8°N/106.7°E), Da Nang (16.1°N/108.2°E) — expand to more cities as needed
- Fetch current (`/air_pollution`), forecast (`/air_pollution/forecast`), and historical (`/air_pollution/history`) endpoints
- Add as parallel task in `dag_ingest_hourly`: `run_openweather_measurements`
- Store OpenWeather's reported AQI in `aqi_reported` column; canonical EPA AQI computed in Phase 2 dbt
- Update `ingestion.control` with source `openweather` on each run

**Success criteria:**
1. `raw_openweather_measurements` populated with >0 rows after first DAG run
2. All records have `source = 'openweather'` and valid `timestamp_utc`
3. DAG task completes in <60s per city (rate limit: 60 req/min)
4. No HTTP 429 errors in Airflow logs

---

### Plan 1.2 — WAQI / World Air Quality Index Client

**Owner:** data engineering
**Outputs:** `python_jobs/jobs/waqi/ingest_measurements.py`, `raw_waqi_measurements`

**Tasks:**
- Create `python_jobs/jobs/waqi/` module
- Build `WAQIClient` extending existing `api_client.py`: API base `https://api.waqi.info/feed/`
- Create ClickHouse raw table: `raw_waqi_measurements` (`ReplacingMergeTree(ingest_time)`, ORDER BY `(station_id, timestamp_utc, parameter)`)
- Vietnam bounding-box query: `/feed/geo:8.4;102.1;23.4;109.5/` — returns all Vietnam stations in one call
- Parse per-station data from response: PM2.5, PM10, O₃, NO₂, SO₂, CO, aqicn-reported AQI
- Add as parallel task in `dag_ingest_hourly`: `run_waqi_measurements`
- Update `ingestion.control` with source `waqi` on each run

**Success criteria:**
1. `raw_waqi_measurements` populated with >0 rows after first DAG run
2. All records have `source = 'waqi'` and valid `timestamp_utc`
3. DAG task completes in <30s (one bounding-box API call per run)
4. No HTTP 429 errors in Airflow logs

---

### Plan 1.3 — Sensors.Community Client

**Owner:** data engineering
**Outputs:** `python_jobs/jobs/sensorscm/ingest_measurements.py`, `raw_sensorscm_measurements`, `dag_sensorscm_poll`

**Tasks:**
- Create `python_jobs/jobs/sensorscm/` module
- Build `SensorsCMClient` for Luftdaten API (`https://api.luftdaten.info/v1/`), no auth required
- Create ClickHouse raw table: `raw_sensorscm_measurements` (`ReplacingMergeTree(ingest_time)`, ORDER BY `(sensor_id, timestamp_utc, parameter)`)
- Filter Vietnam bounding box: `lat_min=8.4&lat_max=23.4&lon_min=102.1&lon_max=109.5`
- Assign `sensor_quality_tier = 'community'` to all records
- Flag implausible values: PM2.5 outside 0–500 µg/m³ → `quality_flag = 'implausible'`; stations outside Vietnam bbox → `quality_flag = 'outlier'`
- Create new DAG `dag_sensorscm_poll` with `*/10 * * * *` schedule
- Update `ingestion.control` for Sensors.Community on each run

**Success criteria:**
1. `raw_sensorscm_measurements` populated with >0 rows after first `dag_sensorscm_poll` run
2. All records have `sensor_quality_tier = 'community'` and valid `timestamp_utc`
3. DAG runs every 10 minutes with <15 min end-to-end latency from data generation to ClickHouse
4. `ingestion.control.sensorscm` lag_seconds < 900 (15 min threshold)

---

### Plan 1.4 — OpenAQ Decommission

**Owner:** data engineering
**Outputs:** Removed code and tables

**Tasks:**
- Remove OpenAQ tasks from `dag_ingest_hourly`
- Remove `python_jobs/jobs/openaq/` directory (keep `api_client.py` and common modules)
- Archive (do not drop) OpenAQ ClickHouse raw tables: `raw_openaq_*` (keep data for rollback)
- Update `dag_metadata_update` to remove OpenAQ metadata ingestion
- Verify AQICN-only baseline run continues successfully without OpenAQ tasks

**Success criteria:**
1. `dag_ingest_hourly` runs without any OpenAQ tasks
2. `python_jobs/jobs/openaq/` directory removed
3. OpenAQ raw tables renamed to `raw_openaq_*_archived` (not dropped — rollback safety)
4. DAG run succeeds with 0 OpenAQ-related tasks

---

### Plan 1.5 — Rate Limiter + Orchestration Optimization ✅

**Owner:** data engineering
**Executed:** 2026-04-01
**Outputs:** `create_sensorscm_limiter()`, `dag_sensorscm_poll`, 5-source parallel fan-in, backoff_factor=2.0

**Tasks:**
- Implement global rate limit coordination: one `TokenBucketRateLimiter` per API key, shared across all tasks using same key
- Add tenacity-based retry with exponential backoff (base=2, max=5 retries, max_wait=5min) to `APIClient`
- Refactor `dag_ingest_hourly`: all source ingestion tasks run in parallel using `@task` decorator (not sequential `>>`)
- Add `ingestion.control` update as final task in all ingestion DAGs
- Separate `dag_ingest_historical` from real-time ingestion (run at non-overlapping times)
- Cache AirNow station metadata in ClickHouse; refresh daily via `dag_metadata_update`

**Success criteria:**
1. ✅ All source ingestion tasks in `dag_ingest_hourly` run in parallel (verified: `[aqicn, forecast, sensorscm, openweather, waqi]`)
2. ✅ urllib3 Retry with `status_forcelist={429}` + `backoff_factor=2.0` for 429 backoff
3. ✅ `create_sensorscm_limiter()` added (1.0 req/s, burst=5, max_delay=300s)
4. ✅ `dag_sensorscm_poll` created with `*/10 * * * *` schedule
5. ✅ `ingestion.control` updated for all 5 sources (aqicn, aqicn_forecast, openweather, waqi, sensorscm)
6. ✅ `pytest tests/test_rate_limiter.py -q` — 7/7 pass

---

## Phase 2 — dbt Refactor

**Goal:** Multi-source canonical mart layer. Existing OpenAQ dbt models refactored for AirNow, Sensors.Community, and MONRE.

**Depends on:** Phase 1 (raw tables stable)

### Plan 2.1 — Source-Specific Staging Models

**Owner:** data engineering
**Outputs:** `stg_airnow__measurements`, `stg_sensorscm__measurements`, `stg_monre__measurements`

**Tasks:**
- Create `stg_airnow__measurements.sql`: map AirNow response columns → canonical schema
- Create `stg_sensorscm__measurements.sql`: map Sensors.Community columns → canonical schema
- Create `stg_monre__measurements.sql`: map MONRE response → canonical schema
- Canonical column set: `station_id`, `station_name`, `latitude`, `longitude`, `parameter`, `value`, `unit`, `aqi_reported`, `timestamp_utc`, `source`, `quality_flag`
- Add dbt singular tests per staging model:
  - Row count > 0
  - `timestamp_utc` not null
  - `value` within valid range per parameter
  - `source` = expected value
- Add `{{ doc() }}` descriptions to all columns for OpenMetadata

**Success criteria:**
1. All three staging models compile without errors (`dbt compile`)
2. All singular tests pass: row count > 0, null checks, range checks
3. Each staging model produces exactly the canonical column set
4. Staging models join correctly to `int_unified__measurements`

---

### Plan 2.2 — Unified Intermediate Layer

**Owner:** data engineering
**Outputs:** `int_unified__measurements`, `int_aqi_calculations`, `normalize_parameter_name()` macro

**Tasks:**
- Create `int_unified__measurements.sql`: `UNION ALL` of all source-specific staging models
- Create `normalize_parameter_name()` dbt macro: map all source-specific parameter names to standard vocabulary
  - e.g., `pm2_5` → `pm25`, `co_8hr` → `co`, `o3_8h` → `o3`
- Create `int_aqi_calculations.sql`: compute canonical EPA AQI for each measurement
  - One AQI formula applied consistently across all sources
  - Store `aqi_calculation_method = 'epa_canonical'` in output
- Parameter mapping seed file: `seeds/parameter_mapping.csv` (updatable without model changes)

**Success criteria:**
1. `int_unified__measurements` contains data from all active sources
2. `int_aqi_calculations` produces `normalized_aqi` values in range 0–500 for all records
3. `normalize_parameter_name()` maps all known parameter variants correctly
4. Parameter mapping seed is updatable via `dbt seed --select parameter_mapping` without model changes

---

### Plan 2.3 — Mart Layer

**Owner:** data engineering
**Outputs:** `fct_hourly_aqi`, `fct_daily_aqi_summary`, `dim_locations`, `mart_air_quality__dashboard`, `mart_air_quality__alerts`

**Tasks:**
- Create `fct_hourly_aqi.sql`: `GROUP BY station_id + hour`; include min/max/avg per parameter, dominant pollutant, source_count
- Create `fct_daily_aqi_summary.sql`: daily rollup per station with AQI min/max/avg
- Create `dim_locations.sql`: deduped, geocoded station master from all sources
- Create `mart_air_quality__dashboard.sql`: Vietnam bounding box (8°N–24°N, 102°E–110°E); pre-materialized for Superset
- Create `mart_air_quality__alerts.sql`: AQI threshold breach events (AQI > 150, AQI > 200)
- Materialize marts as ClickHouse `AggregatingMergeTree` tables for fast Superset queries
- Add dbt column descriptions via `{{ doc() }}` blocks for OpenMetadata

**Success criteria:**
1. `fct_hourly_aqi` row count matches expected hourly volume (verify with `ingestion.control`)
2. `mart_air_quality__dashboard` query time < 3 seconds for 30-day range in ClickHouse
3. `mart_air_quality__alerts` produces zero rows during normal AQI (no false positives)
4. All mart tables are documented in dbt YAML schema files

---

### Plan 2.4 — ClickHouse Schema Migration

**Owner:** data engineering
**Outputs:** ReplacingMergeTree with proper version column, materialized views

**Tasks:**
- Migrate `raw_openaq_measurements` from MergeTree to ReplacingMergeTree with `version` column:
  ```sql
  ALTER TABLE raw_openaq_measurements MODIFY ENGINE ReplacingMergeTree(version)
  ORDER BY (station_id, parameter, timestamp_utc, ingest_batch_id);
  ```
- Audit all other MergeTree raw tables for dedup needs
- Add ClickHouse materialized views for common dashboard queries:
  - `mv_hourly_station_aqi`: pre-aggregated hourly AQI by station
  - `mv_daily_station_summary`: pre-aggregated daily summary
- Document in STACK.md which tables use which engine and why

**Success criteria:**
1. `raw_openaq_measurements` row count stabilizes (no super-linear growth)
2. Re-running ingestion does not duplicate records (dedup working correctly)
3. Materialized views update automatically on insert
4. STACK.md updated with ClickHouse engine selection rationale

---

### Plan 2.5 — dbt Isolation & CI Integration

**Owner:** data engineering
**Outputs:** dbt groups, isolated runs, full CI pipeline

**Tasks:**
- Create dbt groups: `source_aqicn`, `source_airnow`, `source_sensorscm`, `source_monre`
- Run dbt with `--select source_airnow.staging source_airnow.intermediate` for isolated rollout
- Keep shared marts (`fct_hourly_aqi`, `mart_air_quality__dashboard`) as union of all sources
- Run full dbt test suite in CI: `dbt compile && dbt test`
- Add `dbt-validate` CI step: run `dbt run --target dev --select +mart_air_quality__dashboard` against pre-seeded test data

**Success criteria:**
1. `dbt run --select source_airnow.staging+` completes without modifying other source models
2. One broken staging model does not corrupt shared mart tables
3. CI pipeline blocks merge if `dbt test` fails
4. dbt test coverage: at least one test per staging model and per mart table

---

## Phase 3 — Visualization & Monitoring

**Goal:** Superset + Grafana deployment. First time end users interact with the platform.

**Depends on:** Phase 2 (mart tables must exist)

### Plan 3.1 — Superset Deployment

**Owner:** data engineering + analytics
**Outputs:** Superset running in Docker Compose, connected to ClickHouse

**Tasks:**
- Add Superset to `docker-compose.yml`: `apache/superset:4.x`, port 8088, `superset_config.py`
- Create `superset-init.sh`: create admin user, import ClickHouse connection string `clickhouse+native://clickhouse:9000/airquality`
- Mount `superset_config.py` and dashboard YAML exports
- Configure per-dataset query timeout: 30 seconds for large tables
- Configure `CACHE_TIME`: 15 minutes TTL for near-real-time datasets
- **Rule:** Superset queries marts only — never raw or staging tables
- Add `SUPERSET_` environment variables to `.env`

**Success criteria:**
1. `docker compose up superset` starts successfully in <3 minutes
2. Superset UI accessible at `localhost:8088` with admin login
3. ClickHouse database connection verified in Superset → Databases
4. `fct_hourly_aqi`, `mart_air_quality__dashboard`, `dim_locations` visible as datasets

---

### Plan 3.2 — Superset Dashboards

**Owner:** analytics
**Outputs:** 5 Superset dashboards ready for end users

**Tasks:**
- **Dashboard 1 — AQI Overview**: Current AQI by city/station, color-coded (green/yellow/orange/red/purple/maroon), Vietnam map
- **Dashboard 2 — Trends**: 24h and 7d AQI + pollutant (PM2.5, PM10, O₃, NO₂, SO₂, CO) trends per station
- **Dashboard 3 — Pollutant Breakdown**: Pie/bar charts showing pollutant contribution to overall AQI per city
- **Dashboard 4 — Source Comparison**: Side-by-side AQI from different sources for the same station — builds transparency and trust
- **Dashboard 5 — Forecast vs Actual**: AQICN forecast accuracy; compare forecast vs actual readings
- Export dashboard YAML configs to `superset/dashboards/` for version control
- Schedule dashboard cache refresh to align with ingestion frequency

**Success criteria:**
1. All 5 dashboards load in <10 seconds (verify no query timeout)
2. AQI color coding matches standard EPA breakpoints
3. Source comparison dashboard shows data from at least 2 sources
4. Dashboard YAML configs committed to git

---

### Plan 3.3 — Grafana Deployment

**Owner:** data engineering
**Outputs:** Grafana running in Docker Compose, connected to ClickHouse and Airflow metadata

**Tasks:**
- Add Grafana to `docker-compose.yml`: `grafana/grafana:11.x`, port 3000
- Provision Grafana with ClickHouse datasource (install `grafana-clickhouse-datasource` plugin)
- Provision Grafana with Airflow PostgreSQL datasource: connect to `postgres:5432/airflow`
- Create `grafana/provisioning/dashboards/` and `grafana/provisioning/datasources/` YAML configs
- Create **Pipeline Health Dashboard**:
  - DAG success/failure rate (7-day bar chart)
  - Task duration trend per DAG
  - Last successful run per DAG (traffic light: green > yellow > red)
  - Records ingested per source per hour
  - API error rate (4xx/5xx)

**Success criteria:**
1. Grafana accessible at `localhost:3000` with admin login
2. ClickHouse datasource returns data from `ingestion.control` table
3. Airflow PostgreSQL datasource returns DAG run history
4. Pipeline Health Dashboard shows data for all active DAGs

---

### Plan 3.4 — Grafana Freshness & Operational Alerts

**Owner:** data engineering
**Outputs:** Grafana freshness panels and alerting rules

**Tasks:**
- Create **Data Freshness Dashboard**:
  - `max(timestamp_utc)` per source (Stat panel with threshold coloring)
  - `ingestion.control` lag_seconds per source (time series)
  - Rows ingested per hour per source (bar chart, 48h window)
  - Null rate in key columns per source (time series)
- Create **Grafana Alert Rules**:
  - Freshness alert: `max(timestamp_utc) < now() - 3h` → PagerDuty/Slack `#data-ops`
  - DAG failure alert: any DAG failure → Slack `#data-ops`
  - Sensors.Community freshness: `max(timestamp_utc) < now() - 20min` → warning
  - Duplicate spike alert: row count > 2× rolling average → warning
- Configure alert deduplication: minimum 30-minute gap between repeat alerts
- Create **Data Source Coverage** panel: number of active stations per source in Vietnam

**Success criteria:**
1. Freshness panel shows `max(timestamp_utc)` within expected range for each source
2. Test alert fires correctly when `max(timestamp_utc)` exceeds threshold
3. No alert fatigue: alerts deduplicate correctly within 30-minute window
4. Alert notification received on Slack `#data-ops` channel within 5 minutes of threshold breach

---

### Plan 3.5 — Docker Compose Integration & Baseline

**Owner:** data engineering
**Outputs:** Full docker-compose.yml with all services, resource baseline test

**Tasks:**
- Verify all new services (Superset, Grafana) integrate cleanly into existing `docker-compose.yml`
- Run `docker compose up -d` with full stack: verify no port conflicts (Superset 8088, Grafana 3000, OpenMetadata 8585)
- Run `docker stats` during full ingestion cycle: verify no OOM kills
- Document full Docker Compose setup in README
- Verify named volumes persist across `docker compose down && docker compose up`
- Test Grafana + Superset querying simultaneously: verify no ClickHouse degradation

**Success criteria:**
1. `docker compose up -d` starts all services within 5 minutes with no errors
2. `docker stats` shows no `OOMKilled` events during full ingestion cycle
3. No port conflicts between Superset (8088), Grafana (3000), Airflow (8080), ClickHouse (8123/9000)
4. README documents full setup including hardware requirements (16GB RAM, 4 CPU cores)

---

## Phase 4 — OpenMetadata Integration

**Goal:** Data catalog + lineage + data quality. Layered last — requires stable schemas.

**Depends on:** Phase 1 + Phase 2 + Phase 3

### Plan 4.1 — OpenMetadata Deployment

**Owner:** data engineering + analytics
**Outputs:** OpenMetadata running in Docker Compose, connected to ClickHouse

**Tasks:**
- Add OpenMetadata to `docker-compose.yml`: `openmetadata/server:1.1.x`, port 8585, 4GB mem_limit
- Include MySQL and Elasticsearch backends from `openmetadata/docker` docker-compose reference
- Configure ClickHouse connector: port 8123, `airquality` database, `markDeletedTables: true`
- Configure `includeDatabases: airquality`; exclude `system.` and `_tmp`
- Create `openmetadata/dbt_ingestion.yml` for dbt manifest ingestion
- Test OpenMetadata crawler: manually trigger ingestion, verify tables appear with correct schemas
- Assign `raw_*` tables as "raw" tier; `mart_*` tables as "analytical" tier via tags

**Success criteria:**
1. OpenMetadata accessible at `localhost:8585` with admin login
2. `airquality` database tables visible in OpenMetadata catalog with correct column names
3. `system.` database excluded from catalog
4. `raw_*` and `mart_*` tables tagged correctly in OpenMetadata

---

### Plan 4.2 — dbt Lineage Ingestion

**Owner:** data engineering
**Outputs:** Full lineage graph: source API → raw → staging → intermediate → marts → Superset

**Tasks:**
- Configure Airflow `dag_transform` to trigger OpenMetadata dbt ingestion after `dbt run`:
  ```python
  # In dag_transform.py
  openmetadata_ingest_task = HttpOperator(
      task_id='openmetadata_dbt_ingestion',
      http_conn_id='openmetadata_default',
      endpoint='/api/v1/lineage/ingest',
      method='POST',
      headers={'Content-Type': 'application/json'},
      json={'entityType': 'dbtModel', 'manifestPath': '/openmetadata/manifest.json'}
  )
  ```
- Copy `target/manifest.json` to OpenMetadata volume after each `dbt run`
- Verify lineage graph shows: AirNow/Sensors.Community/MONRE → `raw_*` → `stg_*` → `int_*` → `fct_*` → `mart_*`
- Verify Superset chart lineage visible in OpenMetadata

**Success criteria:**
1. dbt model lineage visible in OpenMetadata after `dag_transform` completes
2. Lineage shows complete path: raw table → staging → intermediate → mart → Superset dashboard
3. OpenMetadata schema descriptions match dbt `{{ doc() }}` column descriptions
4. No stale lineage entries after dbt column rename (verified by renaming a column and re-running)

---

### Plan 4.3 — Catalog Curation

**Owner:** analytics + data engineering
**Outputs:** All mart tables documented, owned, tagged

**Tasks:**
- Assign owner to every mart table before dashboards use them
- Add descriptions to all tables and columns in OpenMetadata:
  - `{{ doc('fct_hourly_aqi') }}` — hourly AQI readings per station
  - `{{ doc('normalized_aqi') }}` — canonical EPA AQI (0–500 scale)
  - `{{ doc('sensor_quality_tier') }}` — research (EPA-grade) vs community (Sensors.Community)
- Create glossary terms in OpenMetadata: AQI, PM2.5, PM10, O₃, NO₂, SO₂, CO
- Add tags: `air_quality`, `vietnam`, `pii=false` to all mart tables
- Configure OpenMetadata profiler to run column-level statistics daily

**Success criteria:**
1. All mart tables have non-empty descriptions in OpenMetadata
2. All mart tables have at least one owner assigned
3. Glossary terms (AQI, PM2.5, etc.) linked to relevant columns
4. Column-level statistics available in OpenMetadata profiler (verify after first run)

---

### Plan 4.4 — Data Quality Tests & Governance Workflow

**Owner:** data engineering
**Outputs:** OpenMetadata SQL tests, governance process documented

**Tasks:**
- Define OpenMetadata SQL tests for every mart table:
  - `normalized_aqi` between 0 and 500
  - `timestamp_utc` not null and `timestamp_utc <= now()`
  - `latitude` between 8 and 24, `longitude` between 102 and 110
  - `row_count > 0` (freshness proxy)
- Configure test schedule: run after each `dag_transform` via OpenMetadata API
- Document governance workflow:
  1. New source added → create staging model → add to OpenMetadata
  2. dbt column renamed → `{{ doc() }}` updated → OpenMetadata re-ingests manifest
  3. Dashboard breaks → check OpenMetadata lineage → identify source of issue
- Add OpenMetadata registration step to Airflow DAG: after `dbt run`, trigger metadata refresh

**Success criteria:**
1. All mart tables have at least 3 OpenMetadata SQL tests
2. Test results visible in OpenMetadata incidents dashboard
3. Governance workflow documented in README
4. Test failure blocks dashboard access (enforce via Superset dataset settings)

---

## Phase 5 — Alerting & Reporting

**Goal:** End-to-end alerting + automated reports. Completes the user-facing platform.

**Depends on:** Phase 2 (mart tables) + Phase 3 (Grafana)

### Plan 5.1 — Mart-Based Alerting DAG

**Owner:** data engineering
**Outputs:** `mart_air_quality__alerts`, `dag_alerts`, email/Slack/webhook notifications

**Tasks:**
- Verify `mart_air_quality__alerts` dbt model (from Plan 2.3) produces threshold breach events:
  - AQI > 200 (Very Unhealthy): instant alert
  - AQI > 150 (Unhealthy for Sensitive Groups): warning alert
- Create `dag_alerts`: detect breach → HTTP POST to notification service
- Implement deduplication: max 1 alert per station per threshold per hour
- Configure notification channels:
  - Slack webhook: `#critical` for AQI > 200, `#air-quality-alerts` for others
  - SMTP email: for stakeholders without Slack access
  - Webhook: for future integration (custom systems)

**Success criteria:**
1. `mart_air_quality__alerts` produces zero rows during normal AQI (no false positives)
2. Injecting test AQI > 200 → notification delivered to Slack within 10 minutes
3. Deduplication: max 1 notification per station per hour for same threshold
4. Both Slack and email channels receive correct notifications

---

### Plan 5.2 — Grafana Infrastructure Alerts

**Owner:** data engineering
**Outputs:** Grafana-native alert rules for infrastructure + AQI thresholds

**Tasks:**
- Create Grafana alert rules:
  - **ClickHouse OOM/kill**: `clickhouse_up == 0` OR memory > 90% → Slack `#data-ops`
  - **Airflow down**: `airflow_scheduler_heartbeat == 0` → Slack `#data-ops` + PagerDuty
  - **Freshness breach**: `max(timestamp_utc) < now() - 3h` → Slack `#data-ops`
  - **AQI threshold** (Grafana-native): query `fct_hourly_aqi` where `normalized_aqi > 150` → Slack `#air-quality-alerts`
- Configure Slack `#air-quality-alerts` channel routing:
  - AQI > 200 → immediate notification
  - AQI > 150 → daily digest (batch at 09:00)
- Configure Grafana alert notification templates for AQI context (city name, AQI value, pollutant)

**Success criteria:**
1. ClickHouse OOM simulated → Grafana alert fires and Slack notification received within 5 minutes
2. AQI threshold Grafana alert fires when `normalized_aqi > 150` in ClickHouse query
3. Alert routing correct: critical alerts → immediate, non-critical → digest
4. No alert fatigue: alert deduplication working (verified by triggering same alert twice within 30 min)

---

### Plan 5.3 — Automated Weekly Reports

**Owner:** analytics
**Outputs:** Weekly AQ summary report delivered via email

**Tasks:**
- Create Grafana report or external Python script for weekly AQ summary:
  - HTML email: city averages, top-5 worst stations, trend vs prior week
  - PDF archive for compliance record-keeping
  - Charts: 7-day AQI trend per city, pollutant breakdown pie chart
- Schedule: every Monday at 09:00 local time (Vietnam: UTC+7)
- Delivery: email to stakeholder list via SMTP (SendGrid or similar)
- Archive: store PDF reports in S3-compatible storage or shared volume

**Success criteria:**
1. Report generated and emailed every Monday at 09:00 (Vietnam time) for 4 consecutive weeks
2. Report contains: city AQI averages, top-5 worst stations, 7-day trend, pollutant breakdown
3. PDF archive stored and retrievable
4. Report recipients can unsubscribe (via email link)

---

### Plan 5.4 — End-to-End Smoke Test

**Owner:** data engineering
**Outputs:** Automated smoke test, runbook documented

**Tasks:**
- Create smoke test DAG: inject high-AQI reading → verify full pipeline → verify notification delivered
  ```python
  # In dag_smoke_test.py
  insert_high_aqi >> dag_ingest_hourly >> dag_transform >> mart_air_quality__alerts >> verify_slack_notification
  ```
- Document runbook: how to diagnose failures, escalation path, contacts
- Run smoke test on-demand and on schedule (weekly)
- Create Grafana "Platform Health" dashboard summarizing smoke test status

**Success criteria:**
1. Smoke test runs successfully end-to-end: injection → Superset → Grafana → `mart_air_quality__alerts` → notification
2. Runbook accessible and covers: common failures, escalation contacts, rollback procedures
3. Smoke test runs weekly without manual intervention
4. Platform Health dashboard shows green status for 30 consecutive days

---

### Plan 5.5 — Documentation & Handoff

**Owner:** data engineering + analytics
**Outputs:** Platform runbook, architecture overview in README, alert escalation path

**Tasks:**
- Create `docs/runbook.md`:
  - Architecture overview (with diagram)
  - How to add a new data source
  - How to debug a failing DAG
  - Alert escalation path: who is paged for what
  - Rollback procedures
- Update README.md with:
  - Full architecture diagram
  - Service list with ports
  - Quick start: `docker compose up -d`
  - Troubleshooting guide
- Document MONRE discovery results (Plan 1.3 findings)
- Create architecture overview diagram for stakeholder presentations

**Success criteria:**
1. `docs/runbook.md` covers all common failure scenarios
2. README.md contains complete architecture diagram and service list
3. Escalation contacts documented and current
4. New engineer can bring up full stack from `docker compose up -d` in <30 minutes

---

## Traceability Summary

| Requirement | Phase | Plan(s) | Status |
|-------------|-------|---------|--------|
| Multi-source ingestion: test infrastructure | 1 | 0.00 | ✅ Complete |
| Multi-source ingestion (OpenWeather Air Pollution) | 1 | 1.1 | ✅ Complete |
| Multi-source ingestion (WAQI / World Air Quality Index) | 1 | 1.2 | ✅ Complete |
| Multi-source ingestion (Sensors.Community) | 1 | 1.3 | ✅ Complete |
| OpenAQ decommission | 1 | 1.4 | ✅ Complete |
| Pipeline optimization (rate limiting, parallel execution) | 1 | 1.5 | ✅ Complete |
| dbt refactor: staging | 2 | 2.1 | Pending |
| dbt refactor: intermediate | 2 | 2.2 | Pending |
| dbt refactor: marts | 2 | 2.3 | Pending |
| ClickHouse schema migration (ReplacingMergeTree) | 2 | 2.4 | Pending |
| dbt isolation & CI | 2 | 2.5 | Pending |
| Superset deployment | 3 | 3.1 | Pending |
| Superset dashboards | 3 | 3.2 | Pending |
| Grafana deployment | 3 | 3.3 | Pending |
| Grafana freshness & alerts | 3 | 3.4 | Pending |
| Docker Compose integration | 3 | 3.5 | Pending |
| OpenMetadata deployment | 4 | 4.1 | Pending |
| dbt lineage ingestion | 4 | 4.2 | Pending |
| Catalog curation | 4 | 4.3 | Pending |
| Data quality tests | 4 | 4.4 | Pending |
| Mart-based alerting DAG | 5 | 5.1 | Pending |
| Grafana infrastructure alerts | 5 | 5.2 | Pending |
| Automated weekly reports | 5 | 5.3 | Pending |
| End-to-end smoke test | 5 | 5.4 | Pending |
| Documentation & handoff | 5 | 5.5 | Pending |
| CI pipeline | 0 | 0.3 | Pending |
| AQICN-only stability baseline | 0 | 0.4 | Pending |
| Ingestion control table | 0 | 0.5 | Pending |
| Docker Compose resource hardening | 0 | 0.2 | Pending |
| Baseline codebase audit | 0 | 0.1 | Pending |

**Total: 30 plans | 29 success criteria | 6 phases**

### Phase 1: run compose, test va verified moi truong hoan thien de thuc hien cac phase sau

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 0
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd:plan-phase 1 to break down)

---

*Roadmap created: 2026-04-01*
*Generated by: gsd-roadmapper (Opus model)*
*Research basis: SUMMARY.md, FEATURES.md, ARCHITECTURE.md, PITFALLS.md*
