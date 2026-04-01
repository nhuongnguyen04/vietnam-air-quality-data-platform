# PITFALLS.md — Vietnam Air Quality Data Platform Refactor

> Research output: critical mistakes to avoid when migrating data sources,
> adding Superset/Grafana visualization, and integrating OpenMetadata on a
> brownfield air quality data platform.

---

## How to Read This Document

| Field | Meaning |
|---|---|
| **Warning Signs** | Early symptoms to watch for — detect before the mistake propagates |
| **Prevention Strategy** | Concrete, actionable steps to take now |
| **Phase** | When to address this (Planning / Build / Integrate / Operate) |

---

## 1. Multi-Source Ingestion Pitfalls

### 1.1 Inconsistent AQI Calculation Across Sources

**Why it's critical:** EPA AirNow, Sensors.Community, and IQAir/AQICN use different AQI formulas and pollutant breakpoints (PM₂.₅, PM₁₀, O₃, NO₂, SO₂, CO). Merging them naively into a single dashboard produces numbers that cannot be compared.

**Warning Signs:**
- Dashboards show AQI values from different sources that don't agree for the same station/time
- dbt marts have separate AQI columns per source instead of one normalized column
- No documentation on which AQI formula each source uses

**Prevention Strategy:**
- Document the AQI formula per source before ingesting:
  - **IQAir/AQICN**: US EPA formula adapted for different pollutant range
  - **EPA AirNow**: Standard US EPA AQI (2001 revision)
  - **Sensors.Community**: Community sensor formula — not EPA-aligned
  - **MONRE**: Vietnamese TCVN-based breakpoints (different from EPA)
- In dbt staging: create normalized AQI per source using source-specific logic
- In dbt intermediate/marts: keep one `normalized_aqi` from canonical EPA formula
- Add `aqi_calculation_method` column to mart tables

**Phase:** Planning (data modeling) + Build (dbt staging/intermediate)

---

### 1.2 Sensors.Community Data Quality Blindness

**Why it's critical:** Sensors.Community is crowdsourced with highly variable sensor hardware. Data quality ranges from research-grade to noise. Treating it as equivalent to EPA AirNow contaminates analytics.

**Warning Signs:**
- Sensors.Community measurements have extreme outliers (PM₂.₅ > 1000 µg/m³ or negative values)
- Station locations implausible (outside Vietnam bounding box: ~8°N–24°N, 102°E–110°E)
- All sensor data weighted equally in AQI aggregations

**Prevention Strategy:**
- In dbt staging: apply per-source quality rules
  - Flag PM₂.₅ outside 0–500 µg/m³ as invalid
  - Flag stations outside Vietnam bounding box
  - Add `sensor_quality_tier` column: `research` vs `community`
- Add Sensors.Community-specific staging model with hardware-aware filtering
- Document limitations in OpenMetadata
- Always expose data source in dashboards so users can filter by quality tier

**Phase:** Build (dbt staging) + Integrate (OpenMetadata)

---

### 1.3 Source-Specific Schema Assumptions Baked into dbt

**Why it's critical:** Existing OpenAQ dbt models assume specific column names. EPA AirNow and Sensors.Community have completely different schemas. Copying the OpenAQ pattern for new sources leads to silent column mismatches.

**Warning Signs:**
- New source jobs reference OpenAQ column names that don't exist in AirNow's response
- dbt models use hardcoded source names as filter conditions
- Adding a 4th source requires editing 10+ dbt model files

**Prevention Strategy:**
- Design dbt staging as source-agnostic:
  - Canonical column schema: `station_id`, `station_name`, `latitude`, `longitude`, `parameter`, `value`, `unit`, `timestamp`, `source`, `quality_flag`
  - Each source gets dedicated staging model that maps native schema → canonical schema
- Add `source_system` dimension table tracking per-source metadata
- Write dbt tests asserting all staging models produce the same canonical column set

**Phase:** Planning (data modeling) + Build (dbt refactor)

---

### 1.4 MONRE Data Access Assumptions

**Why it's critical:** Government/MONRE data may require web scraping, FTP, manual downloads, or bureaucratic agreements. Most likely source to fail or change format without notice.

**Warning Signs:**
- MONRE ingestion designed as a simple API call (it isn't one)
- No fallback plan if MONRE data is unavailable
- No monitoring for MONRE data freshness

**Prevention Strategy:**
- Treat MONRE as "unreliable until proven otherwise"
- Do discovery: confirm access method, data format, update frequency
- Add `data_provider` column to mark MONRE vs other sources
- Store MONRE data in separate raw table initially until schema confirmed stable
- Add dedicated Grafana panel tracking MONRE freshness with relaxed SLA

**Phase:** Planning (source discovery) + Build (ingestion DAGs)

---

### 1.5 API Rate Limit and Quota Exhaustion

**Why it's critical:** Adding AirNow and Sensors.Community multiplies API calls. Each source has different rate limits. Exceeding limits without proper backoff causes cascading DAG failures.

**Warning Signs:**
- DAG runs fail intermittently with HTTP 429 errors
- Rate limiter configured per-source without global coordination
- Historical backfill exhausts daily API quotas

**Prevention Strategy:**
- Implement global rate limit coordination: one rate limiter per API key, shared across all DAG tasks
- Add tenacity-based retry with exponential backoff (base=2, max=5 retries, max_wait=5min)
- Separate historical backfill DAGs from real-time ingestion; schedule at non-overlapping times
- Add rate limit hit counters to Airflow task logs; expose in Grafana
- Cache AirNow station metadata in ClickHouse; refresh daily via `dag_metadata_update`

**Phase:** Build (Python jobs) + Integrate (Airflow DAGs)

---

## 2. Visualization Layer Pitfalls (Superset + Grafana)

### 2.1 ClickHouse Query Performance in Superset

**Why it's critical:** Superset issues SQL against ClickHouse. Without materialized views, dashboards query large fact tables directly → 30+ second timeouts → unusable dashboards.

**Warning Signs:**
- Superset charts timeout or take >10s to render
- ClickHouse `Code: 160. Execution timeout exceeded`
- Cascading load when users refresh repeatedly

**Prevention Strategy:**
- Materialize all dashboard-facing tables as ClickHouse `AggregatingMergeTree` or `SummingMergeTree` pre-aggregated by hour/station/pollutant
- Add ClickHouse materialized views for common dashboard queries
- Configure Superset per-dataset query timeout (30s for large tables)
- Use Superset `CACHE_TIME` feature; set TTL based on data freshness

**Phase:** Build (dbt marts) + Integrate (Superset setup)

---

### 2.2 Dashboard AQI Mismatch Between Superset and Grafana

**Why it's critical:** Superset and Grafana may query different tables or use different AQI formulas, producing conflicting numbers. Users lose trust in both tools.

**Warning Signs:**
- Same station shows different AQI in Superset vs Grafana
- Superset uses raw data; Grafana uses dbt marts — different logic
- No version control on Superset chart configurations

**Prevention Strategy:**
- Define dbt marts as the one authoritative data layer; both tools query from it
- Store Superset chart configs in Git (`superset_dashboards/` YAML exports)
- Create canonical AQI in dbt intermediate layer; both tools reference same `mart_air_quality_hourly`
- Add dbt test asserting `normalized_aqi` values match expected ranges

**Phase:** Integrate (Superset + Grafana)

---

### 2.3 Superset Not Containerized in docker-compose

**Why it's critical:** Superset is documented but not in docker-compose.yml. Deploying separately creates configuration drift and credential management nightmares.

**Warning Signs:**
- Superset runs on developer's local machine or bare VM
- No `SUPERSET_` variables in `.env`
- No Superset service in `docker-compose.yml`

**Prevention Strategy:**
- Add Superset as first-class Docker Compose service alongside ClickHouse and Airflow
- Use `apache/superset:4.x`; configure via `SUPERSET_` env vars
- Mount `superset_config.py` volume for custom config
- Add `superset-init.sh` to bootstrap admin user, database connection, dashboard imports

**Phase:** Planning (docker-compose plan) + Build (Superset integration)

---

### 2.4 Grafana Monitoring Gaps for Data Freshness

**Why it's critical:** Existing system has no data freshness monitoring. Broken AirNow API produces stale AQI silently — team discovers from user complaints.

**Warning Signs:**
- No Grafana panels showing `max(ingest_time)` per source
- No alerting rules for stale data
- Airflow DAG status visible only in Airflow UI

**Prevention Strategy:**
- Create Grafana "Data Pipeline Health" dashboard with panels for:
  - Records ingested per source per hour
  - Most recent record timestamp per source (with threshold alerting)
  - Airflow DAG success/failure rate
  - API error rate per source
- Configure alert: `max(timestamp)` > 2× expected interval (e.g., >3h since last measurement)

**Phase:** Build (Grafana setup) + Integrate (alerting rules)

---

## 3. dbt Transformation Pitfalls

### 3.1 dbt Models Not Designed for Schema Drift from New Sources

**Why it's critical:** Existing dbt models assume OpenAQ schema. New sources have different parameter names, units, timestamp formats. Models silently drop or misclassify new source data.

**Warning Signs:**
- New source ingestion writes data but never appears in dbt marts
- dbt runs succeed but intermediate row counts << raw table counts
- Parameter names hardcoded in dbt WHERE clauses

**Prevention Strategy:**
- Audit existing dbt staging models for hardcoded source assumptions
- Create source-specific staging models (e.g., `stg_epa_airnow_measurements`) — do not modify existing OpenAQ models
- Add dbt singular tests: row count > 0, `timestamp` not null, `value` within valid range
- Add dbt macro `normalize_parameter_name()` mapping all variants to standard vocabulary

**Phase:** Build (dbt refactor) + Integrate (dbt tests)

---

### 3.2 ClickHouse ReplacingMergeTree Version Column Mismanagement

**Why it's critical:** `raw_openaq_measurements` uses MergeTree (append-only). Re-inserting same measurements creates duplicates that inflate analytics and distort AQI trends.

**Warning Signs:**
- `raw_openaq_measurements` row count grows super-linearly
- Dashboard charts show jagged spikes from duplicate values
- ClickHouse query returns different counts on repeated runs

**Prevention Strategy:**
- Convert `raw_openaq_measurements` from MergeTree to ReplacingMergeTree with `version` column
- Define dedup sort order: `ORDER BY (station_id, parameter, timestamp, ingest_batch_id)` — latest batch wins
- For audit tables that must never dedup: keep MergeTree with dbt dedup step before mart creation
- Document which tables use which engine in STACK.md

**Phase:** Build (ClickHouse schema migration) + Operate (monitor row counts)

---

### 3.3 Shared Mart Fragility from New Sources

**Why it's critical:** All sources feeding same mart → adding a source modifies shared marts → broken staging model silently corrupts entire mart.

**Warning Signs:**
- Adding EPA AirNow changes shared intermediate model, breaking IQAir/AQICN charts
- dbt failures don't isolate to the new source's staging model

**Prevention Strategy:**
- Use dbt `--select` and `--exclude` flags for isolated source runs
- Add dbt group per source (`source_epa`, `source_sensors`); require isolated `--select` for phased rollout
- Keep shared marts (`mart_aqi_hourly`) as separate models that union all sources — one source breaks, others still render
- Run dbt tests in CI before deploying to production

**Phase:** Build (dbt architecture) + Integrate (CI pipeline)

---

## 4. OpenMetadata Integration Pitfalls

### 4.1 OpenMetadata Catalog Without Ownership or Quality SLAs

**Why it's critical:** Catalog full of empty descriptions and no owners is worse than no catalog — false sense of governance.

**Warning Signs:**
- All tables show "No description" in OpenMetadata
- No data quality tests defined for any table
- Nobody knows who owns `raw_airnow_measurements`

**Prevention Strategy:**
- Assign owner to every table before loading into OpenMetadata
- Define data quality tests for every mart table:
  - `normalized_aqi` between 0 and 500
  - `timestamp` not null and not in the future
  - `latitude` between 8 and 24, `longitude` between 102 and 110
  - Row count > 0 (freshness proxy)
- Configure OpenMetadata profiler to run column-level statistics on schedule
- Add OpenMetadata registration to Airflow DAG: after dbt run, trigger metadata refresh

**Phase:** Integrate (OpenMetadata setup) + Operate (ownership enforcement)

---

### 4.2 ClickHouse Connector Misconfigured for OpenMetadata

**Why it's critical:** ClickHouse has non-standard SQL dialect (no `SCHEMA` — uses `DATABASE`). Misconfigured ingestion → tables with wrong names, no columns, or skipped entirely.

**Warning Signs:**
- OpenMetadata shows tables but no columns
- Only `system.` tables appear; `air_quality` database missing
- OpenMetadata profiler reports 0 rows for all tables

**Prevention Strategy:**
- Use OpenMetadata's native ClickHouse connector with correct params:
  - Host: `${CLICKHOUSE_HOST}`, Port: `8123`, Database: `air_quality`
  - Enable `includeDatabases: air_quality` in service definition
  - Set `markDeletedTables: true`
- Test crawler by manually triggering ingestion before relying on it
- Exclude `system.` and `_tmp` databases in config
- Register `raw_*` as "raw" tier, `mart_*` as "analytical" tier via tags

**Phase:** Integrate (OpenMetadata setup)

---

### 4.3 OpenMetadata and dbt Not Integrated

**Why it's critical:** dbt is the system of record for schemas. Without integration, OpenMetadata descriptions diverge from reality after next dbt run.

**Warning Signs:**
- OpenMetadata descriptions don't match dbt model column comments
- dbt column renamed but OpenMetadata still shows old name
- No dbt test results visible in OpenMetadata

**Prevention Strategy:**
- Use OpenMetadata's native dbt ingestion: configure Airflow `dag_transform` to trigger after `dbt run`
- Ensure dbt model's `description` property is set (via `{{ doc() }}` blocks) — flows into OpenMetadata automatically
- Configure lineage: AirNow/Sensors.Community API → Airflow → raw → staging → intermediate → marts → Superset/Grafana
- Store OpenMetadata ingestion config in repo (`openmetadata/dbt_ingestion.yml`) for version control

**Phase:** Integrate (OpenMetadata + dbt integration)

---

## 5. Cross-Cutting Pitfalls

### 5.1 Docker Compose Service Proliferation Without Resource Planning

**Why it's critical:** Adding Superset, Grafana, OpenMetadata adds 3 new services. OpenMetadata alone needs ~4GB RAM. Existing environment was sized for ClickHouse + Airflow. OOM kills likely.

**Warning Signs:**
- `docker compose up` fails with OOM on <16GB RAM machines
- Superset worker containers repeatedly `OOMKilled`
- Airflow scheduler crashes when OpenMetadata crawler runs simultaneously

**Prevention Strategy:**
- Add resource limits to all Docker Compose services from day one
- For OpenMetadata: allocate at least 4GB; use `openmetadata/docker-compose-dev.yml` as reference
- For Superset: start with `SUPERSET_WORKERS=2`, `SUPERSET_MEMORY_LIMIT=2G`
- Document minimum hardware requirements in README: 16GB RAM, 4 CPU cores
- Run resource baseline test: launch all services, run full ingestion cycle, verify no OOM in `docker stats`

**Phase:** Planning (resource estimation) + Build (docker-compose update)

---

### 5.2 Near-Real-Time Latency Target vs. Batch Architecture Mismatch

**Why it's critical:** Target is <15 min ingestion latency, but entire architecture is batch-oriented (hourly DAGs). Without careful design, "near-real-time" becomes aspirational.

**Warning Signs:**
- DAGs still run hourly; no mechanism to trigger ad-hoc runs
- Multiple sources ingested sequentially, compounding latency
- No measurable difference in data freshness before/after refactor

**Prevention Strategy:**
- Clarify SLA: target <15 min, hourly is acceptable fallback. Design for hourly first, then reduce interval
- Run source ingestion tasks in parallel using Airflow TaskFlow (not sequential chains)
- Add "fast lane": separate `dag_ingest_fast` running every 10 min for IQAir/AQICN; slower sources (Sensors.Community, MONRE) remain hourly
- Expose actual latency as metric: compute `max(timestamp) - now()` per source in Grafana

**Phase:** Planning (latency SLA) + Build (Airflow DAG optimization)

---

### 5.3 No CI Pipeline for dbt + Ingestion Changes

**Why it's critical:** No CI pipeline. Changes to dbt models, ingestion jobs, or docker-compose deployed directly to production. A broken dbt model committed to main breaks Airflow DAG and corrupts Superset dashboards.

**Warning Signs:**
- Production dashboards break after dbt model change; fix requires emergency rollback
- No way to test dbt changes against production-equivalent data
- Docker image builds not tested before pushing

**Prevention Strategy:**
- Implement minimal CI pipeline (GitHub Actions):
  1. `lint`: `sqlfluff lint dbt/`, `ruff check python_jobs/`
  2. `dbt-ci`: spin up test ClickHouse container, run `dbt deps && dbt compile && dbt test`
  3. `docker-build`: build Airflow Docker image, verify it starts
  4. `dbt-validate`: run `dbt run --target dev` against pre-seeded test dataset
- Store Superset dashboard YAML exports in `superset/dashboards/`; require PR review
- Block merge to main if any CI step fails
- Add `docker-compose.test.yml` for CI that spins up minimal infra

**Phase:** Planning (CI architecture) + Build (GitHub Actions workflows)

---

## Summary Table

| # | Pitfall | Phase | Impact |
|---|---|---|---|
| 1.1 | Inconsistent AQI calculation across sources | Planning / Build | Dashboard data incomparable |
| 1.2 | Sensors.Community data quality blindness | Build | Polluted analytics |
| 1.3 | Source-schema assumptions in dbt | Planning / Build | New sources silently fail |
| 1.4 | MONRE data access assumptions | Planning | Broken ingestion, no fallback |
| 1.5 | API rate limit exhaustion | Build | Cascading DAG failures |
| 2.1 | ClickHouse query performance in Superset | Build / Integrate | Dashboard timeouts |
| 2.2 | AQI mismatch between Superset and Grafana | Integrate | Trust erosion |
| 2.3 | Superset not in docker-compose | Planning | Untracked deployment |
| 2.4 | No Grafana data freshness monitoring | Build / Integrate | Silent data outages |
| 3.1 | dbt models not designed for schema drift | Build | New source data lost |
| 3.2 | ReplacingMergeTree version column mismanagement | Build / Operate | Duplicates, bad analytics |
| 3.3 | Shared mart fragility from new sources | Build | Cascade failures |
| 4.1 | OpenMetadata catalog without ownership | Integrate / Operate | No governance value |
| 4.2 | ClickHouse connector misconfigured | Integrate | Empty/incomplete catalog |
| 4.3 | OpenMetadata not integrated with dbt | Integrate | Stale metadata |
| 5.1 | Docker resource exhaustion | Planning | OOM kills in dev/CI |
| 5.2 | Batch architecture vs. <15 min latency target | Planning / Build | False near-real-time claim |
| 5.3 | No CI pipeline for dbt + ingestion | Planning / Build | Production failures |

---

*Research completed: 2026-04-01*
*Downstream consumer: roadmap/planning — PITFALLS.md*
