# Architecture Research — Vietnam Air Quality Data Platform (Refactor)

## 1. How Multi-Source AQ Platforms Are Structured

### 1.1 Canonical Architecture Pattern

Industry-standard AQ data platforms follow a **hub-and-spoke + medallion** structure:

```
EXTERNAL DATA SOURCES
  EPA AirNow | Sensors.Community | AQICN/IQAir | MONRE/Gov API
          │          │                  │              │
          ▼          ▼                  ▼              ▼
INGESTION LAYER (Source-Specific Python Jobs)
  AirNowClient | SensorsClient | AQICNClient | MONREClient
  Each client owns: rate limiting, auth, retry, dedup key, schema mapping
          │
          ▼
STORAGE LAYER — Medallion (ClickHouse MergeTree)

  BRONZE / RAW
    One table per source per entity type:
    ├── raw_airnow_measurements, raw_airnow_sites
    ├── raw_sensorscm_measurements, raw_sensorscm_sites
    ├── raw_aqicn_measurements, raw_aqicn_forecast
    └── raw_monre_measurements
    Properties: append-only, full payload, dedup in Python/pre-write

  SILVER / STAGING (dbt views)
    stg_airnow__measurements, stg_sensorscm__measurements,
    stg_aqicn__measurements, stg_monre__measurements
    Properties: source-specific cleaning, type casting, timezone normalization

  GOLD / MARTS (dbt tables)
    ├── fct_hourly_aqi             — unified hourly readings
    ├── fct_daily_aqi_summary      — daily AQI rollup per station
    ├── dim_locations              — deduped, geocoded station master
    ├── mart_air_quality__alerts   — threshold breach events
    └── mart_kpis__coverage        — data completeness KPIs
    Properties: pre-computed, partitioned, indexed for viz queries

          │
          ▼
SERVING & MANAGEMENT LAYER
  Superset (dashboards) | Grafana (monitoring)
  OpenMetadata (catalog) | Alerting Engine
  All query ClickHouse via native/HTTP drivers
```

### 1.2 Core Principles

| Principle | Application |
|---|---|
| **Source isolation in raw** | Each source gets its own raw table(s); never mix raw data before normalization |
| **Normalize late** | Apply unified schema only at the mart layer, not at ingestion |
| **Append-only raw** | Never UPDATE/DELETE in raw; correctness via dedup keys at write time |
| **Separation of serving** | Dashboards, catalog, and monitoring are separate services |
| **Metadata first** | OpenMetadata catalogs schema before dashboards query them |

---

## 2. Component Boundaries

### 2.1 Component Inventory (C1–C8)

| ID | Component | Boundary |
|---|---|---|
| **C1** | External Data Sources | Outbound only; platform reads from them |
| **C2** | Ingestion Jobs (Python CLI + Airflow) | Reads source APIs → writes raw tables |
| **C3** | ClickHouse Storage (raw / staging / marts) | Ingestion writes raw; dbt reads raw + writes marts; all tools read marts |
| **C4** | dbt Transformation Layer | Reads raw + staging → writes marts |
| **C5** | Orchestration (Airflow 3.1) | Triggers C2 + C4; does NOT query ClickHouse for visualization |
| **C6** | Visualization (Superset + Grafana) | Read-only queries on ClickHouse marts |
| **C7** | OpenMetadata (Data Catalog) | Metadata layer; does NOT modify data |
| **C8** | Alerting Engine | Reads mart data or Grafana metrics; sends to external channels |

### 2.2 New Modules for C2 (Ingestion)

```
Existing modules (extend):
  api_client.py          — add AirNow, Sensors.Community clients
  clickhouse_writer.py   — stays, add new raw tables
  config.py              — per-source YAML config
  rate_limiter.py        — per-source token buckets

New modules:
  airnow_client.py
  sensorscm_client.py
  monre_client.py (REST or scraper)
```

---

## 3. Data Flow

### 3.1 Per-Source Detail

| Source | API Format | Rate Limit | Vietnam Coverage | Output Tables |
|--------|-----------|-----------|-----------------|---------------|
| **EPA AirNow** | REST JSON; DateFormat `YYYYMMDDHH` | ~60 req/min (free tier: 1 req/sec) | Medium-High | `raw_airnow_sites`, `raw_airnow_measurements` |
| **Sensors.Community** | REST JSON; no auth | No limit | Low-Medium (peri-urban/rural) | `raw_sensorscm_measurements` |
| **AQICN/IQAir** | REST JSON (existing) | ~60 req/min | High | `raw_aqicn_measurements`, `raw_aqicn_forecast` (stays) |
| **MONRE** | Portal (likely scraping/manual CSV) | N/A | Major cities | `raw_monre_measurements` |

### 3.2 Cross-Source Unification (Mart Layer)

```
raw_airnow_measurements ──┐
raw_sensorscm_measurements┤
raw_aqicn_measurements ───┼──► stg_<source>__measurements
raw_monre_measurements ───┘    (per-source: type cast + normalize to UTC)

         │
         ▼
   int_unified__measurements
   (UNION ALL of all sources, normalized columns:
    station_id, station_name, lat, lon,
    parameter, value, unit, aqi, timestamp_utc)

         │
         ▼
   fct_hourly_aqi
   (GROUP BY station_id + hour;
    weighted AQI, dominant pollutant,
    source_count, min/max/avg per parameter)

         │
         ▼
   mart_air_quality__dashboard
   (Vietnam bounding box: 8N–24N, 102E–110E)
```

---

## 4. How Each New Tool Integrates

### 4.1 Superset → ClickHouse
- **Image**: `apache/superset:4.x` + `clickhouse-driver`
- **Connection**: `clickhouse+native://ch_server:9000/airquality` or `clickhouse+http://ch_server:8123`
- **Reads from**: marts only — never raw or staging
- **Recommended datasets**: `fct_hourly_aqi`, `fct_daily_aqi_summary`, `dim_locations`, `mart_air_quality__dashboard`

### 4.2 Grafana → ClickHouse + Airflow
- **Image**: `grafana/grafana` + `grafana-clickhouse-datasource` plugin
- **Data sources**:
  - **ClickHouse**: pipeline metrics, data freshness
  - **Airflow metadata DB (PostgreSQL)**: DAG run history
  - **Prometheus** (optional): `node_exporter` system metrics
- **Dashboards**: Pipeline Health, Data Freshness, Ingestion Latency, System Metrics

### 4.3 OpenMetadata → ClickHouse + dbt + Superset
- **Image**: `openmetadata/docker` (includes MySQL + Elasticsearch + NGINX)
- **Connectors**: ClickHouse (auto-crawl), dbt manifest.json (post-dbt-run), Superset (dashboard lineage)
- **Governance workflow**:
  1. OpenMetadata ingests schema from ClickHouse
  2. dbt run pushes `manifest.json` → OpenMetadata
  3. Analytics team adds descriptions + tags
  4. OpenMetadata surfaces lineage to Superset dashboards

### 4.4 Alerting Integration

| Pattern | When to use | How |
|---|---|---|
| **Grafana-native** | Infrastructure alerts (ClickHouse OOM, Airflow down) | ClickHouse query → threshold → Slack/email |
| **Mart + Airflow DAG** | AQI threshold breaches | `mart_air_quality__alerts` dbt model → Airflow HTTP operator → Slack/SendGrid |

---

## 5. Near-Real-Time Tiering

| Source | Update Frequency | Achievable Latency | Strategy |
|---|---|---|---|
| Sensors.Community | ~5 min | 10–15 min | Dedicated DAG every 10 min |
| AQICN | ~15–30 min | 20–30 min | Existing hourly DAG |
| EPA AirNow | ~1 hr | 30–60 min | Hourly DAG |
| MONRE | Daily or manual | 1–24 hr | Daily DAG |

**Target: <15 min for Sensors.Community; hourly for all others**

Recommended: `dag_sensorscm_poll` at `*/10 * * * *`, separate from `dag_ingest_hourly`.

---

## 6. Suggested Build Order

```
Phase 0: Foundation
  └── Verify existing docker-compose runs cleanly

Phase 1: New ingestion sources (additive — zero risk to existing)
  ├── AirNow client + raw_airnow_* tables
  │   → Add as parallel task in dag_ingest_hourly
  ├── Sensors.Community client + raw_sensorscm_*
  │   → New DAG: dag_sensorscm_poll (*/10 * * * *)
  └── MONRE scraper + raw_monre_*
      → New DAG: dag_monre_ingest (daily 02:00)

Phase 2: dbt refactor (requires Phase 1 raw tables)
  ├── staging/: +stg_airnow__*, +stg_sensorscm__*, +stg_monre__*
  ├── intermediate/: int_unified__measurements (UNION ALL)
  │                  int_aqi_calculations (extend for new sources)
  └── marts/: fct_hourly_aqi (multi-source)
               mart_air_quality__dashboard
               mart_air_quality__alerts

Phase 3: Visualization + Monitoring (can run in parallel)
  ├── Superset: Add to docker-compose.yml, connect ClickHouse, build dashboards
  └── Grafana: Add to docker-compose.yml, ClickHouse plugin, build dashboards

Phase 4: OpenMetadata (layered on everything)
  ├── Add to docker-compose.yml
  ├── Configure ClickHouse schema crawler
  ├── Ingest dbt manifest.json post-dbt-run
  └── Manually curate: glossary, descriptions, tags, ownership

Phase 5: Alerting (final polish)
  ├── mart_air_quality__alerts dbt model
  ├── Alert DAG (Pattern B) + Grafana rules (Pattern A)
  └── End-to-end test: inject high AQI → verify notification

Timeline:
Weeks 1–2: Phase 1 — New ingestion sources
Weeks 3–4: Phase 2 — dbt refactor
Weeks 5–6: Phase 3 — Superset + Grafana (parallel)
Week 7:    Phase 4 — OpenMetadata
Week 8:    Phase 5 — Alerting
```

---

*Research conducted: 2026-04-01*
