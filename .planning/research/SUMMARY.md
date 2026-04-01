# Research Summary — Vietnam Air Quality Data Platform Refactor

**Research date:** 2026-04-01
**Inputs:** ARCHITECTURE.md · STACK.md · FEATURES.md · PITFALLS.md · PROJECT.md
**Status:** Complete

---

## Executive Summary

This refactor transforms a single-source AQ ingestion platform into a multi-source air quality monitoring platform for Vietnam. The existing ClickHouse + dbt + Airflow pipeline is retained and extended; the main new work streams are: (1) replacing OpenAQ with EPA AirNow, Sensors.Community, and MONRE, (2) adding Superset as the user-facing BI layer, (3) adding Grafana for pipeline health and data-freshness observability, and (4) adding OpenMetadata for data catalog and lineage.

The platform is brownfield — all existing capabilities are preserved and built upon. The phased build order is: ingestion sources → dbt refactor → visualization + monitoring → OpenMetadata → alerting. MONRE is the highest-risk source and must be handled defensively. OpenMetadata only delivers value if the team commits to keeping the catalog current.

---

## Key Findings

### Recommended Stack

| Component | Version / Choice | Notes |
|-----------|-----------------|-------|
| **ClickHouse** | 25.12 | Existing — no change |
| **dbt-core / dbt-clickhouse** | 1.10.13 / 1.9.5 | Refactor for multi-source schema |
| **Airflow** | 3.1.7 | Optimize DAGs; keep LocalExecutor |
| **EPA AirNow** | REST JSON API (airnowapi.org) | New — free API key; use lat/lon queries |
| **Sensors.Community** | Luftdaten API v1 | New — no auth; quality tier = community |
| **MONRE** | TBD (likely scraping / portal) | New — highest risk; treat as unreliable |
| **Superset** | `apache/superset:4.x` | New — `clickhouse+native://`; query marts only |
| **Grafana** | `grafana/grafana:11.x` | New — ClickHouse plugin + Airflow metadata DB |
| **OpenMetadata** | `openmetadata/server:1.1.x` | New — requires 4–8 GB RAM; MySQL + ES backends |

**What NOT to use:** Kafka/Flink, NiFi, Tableau/Power BI, `clickhouse-city` driver, JDBC generic for OpenMetadata, AirNow SOAP API, Airflow CEL executor.

**Resource ceiling:** ~20 GB RAM, 9 CPU cores total. Add resource limits to docker-compose from day one.

---

### Expected Features

**Table Stakes (must have):**
- Multi-source ingestion (AirNow + Sensors.Community + MONRE + existing AQICN)
- Source health control table (queried by Grafana; drives alerting)
- ClickHouse audit columns (source, ingest_time, batch_id, raw_payload)
- dbt refactor: per-source staging → intermediate → marts (canonical AQI in marts)
- Superset AQI overview + trend dashboards (color-coded, public read-only)
- Grafana pipeline health + data freshness dashboards
- AQI threshold alerting → email/Slack/webhook
- Docker Compose deployment (all services containerized)
- Rate limiting per source with backoff (TokenBucket; tenacity retry)

**Differentiators (competitive/trust-building):**
- Source comparison dashboard (multi-source AQI side-by-side)
- Geographic station map with real-time AQI (deck.gl)
- Pollutant breakdown charts (PM2.5, PM10, O₃, NO₂, SO₂, CO)
- Forecast vs actual charts
- OpenMetadata data catalog + lineage graph (dbt → ClickHouse → Superset)
- OpenMetadata data quality tests (null rates, range checks)
- Automated PDF/email weekly reports
- Historical backfill UI (one-click date range trigger)
- Airflow DAG-level SLA monitoring

**Anti-Features (explicitly excluded):** Mobile app, Kafka/Flink streaming, per-user auth, ML/prediction, NiFi, Tableau, row-level security, Grafana Enterprise, OpenMetadata ML/PII scanning.

---

### Architecture Approach

**Pattern:** Hub-and-spoke + medallion (bronze/silver/gold) on ClickHouse MergeTree.

```
External Sources (AirNow | Sensors.Community | AQICN | MONRE)
        │
        ▼
Ingestion Layer (source-specific Python clients + Airflow)
        │
        ▼
BRONZE / RAW — one table per source per entity (append-only)
SILVER / STAGING — dbt views; type-cast + normalize to UTC
GOLD / MARTS — fct_hourly_aqi, fct_daily_aqi_summary, dim_locations,
               mart_air_quality__alerts, mart_air_quality__dashboard
        │
        ▼
Serving Layer — Superset (dashboards) | Grafana (monitoring)
                OpenMetadata (catalog) | Alerting Engine
```

**Core principles:**
- Normalize late: unified schema only at mart layer, not at ingestion
- Source isolation in raw: each source owns its raw tables
- Append-only raw: correctness via dedup keys at write time
- One authoritative mart layer: both Superset and Grafana query from it
- dbt `--select` / `--exclude` flags for isolated source runs

**Near-real-time tiering:**

| Source | Cadence | Target Latency |
|--------|---------|----------------|
| Sensors.Community | `*/10 * * * *` | 10–15 min |
| AQICN | hourly | 20–30 min |
| EPA AirNow | hourly | 30–60 min |
| MONRE | daily | 1–24 hr |

**Critical AQI rule:** Each source uses a different formula (EPA, IQAir-adapted, MONRE/TCVN, community). Normalize to canonical EPA AQI in dbt intermediate; store `aqi_calculation_method` in marts.

---

### Critical Pitfalls

1. **Inconsistent AQI across sources** — normalize in dbt intermediate before any dashboard query
2. **Sensors.Community data quality blindness** — flag implausible values, assign `sensor_quality_tier = community`
3. **Source-schema assumptions baked into dbt** — design source-agnostic canonical schema in staging
4. **MONRE data access assumptions** — treat as unreliable; separate raw table; relaxed Grafana SLA
5. **API rate limit exhaustion** — one TokenBucket per API key, shared across DAG tasks
6. **ClickHouse query performance in Superset** — materialize marts as AggregatingMergeTree; 30s timeout; 15 min TTL cache
7. **AQI mismatch between Superset and Grafana** — both must query the same dbt mart layer
8. **Superset not in docker-compose** — add as first-class service
9. **No Grafana data freshness monitoring** — alert at 2× expected interval
10. **Schema drift from new sources** — audit for hardcoded assumptions; add dbt singular tests per staging model
11. **ReplacingMergeTree version column mismanagement** — define dedup sort order; monitor row count growth
12. **Shared mart fragility** — use dbt groups per source; isolated `--select` for rollout
13. **Catalog without ownership** — assign owners before first ingestion
14. **ClickHouse connector misconfigured for OpenMetadata** — use native OpenMetadata ClickHouse connector
15. **OpenMetadata not integrated with dbt** — use native dbt manifest ingestion in `dag_transform`
16. **Docker resource exhaustion** — OpenMetadata needs 4 GB; add limits from day one
17. **Batch architecture vs. <15 min latency claim** — clarify SLA: hourly is acceptable fallback
18. **No CI pipeline** — implement GitHub Actions: lint → dbt compile + test → docker build

---

## Implications for Roadmap

### Recommended Phase Structure

| Phase | Focus | Key Deliverables | Duration |
|-------|-------|-----------------|----------|
| **1 — Ingestion** | New source clients + raw tables | `AirNowClient`, `SensorsClient`, `MONREClient`; `raw_*` tables; `ingestion_control`; rate limiter | Weeks 1–2 |
| **2 — dbt Refactor** | Multi-source mart layer | Per-source staging; `int_unified__measurements`; `fct_hourly_aqi`; mart_air_quality__dashboard + __alerts | Weeks 3–4 |
| **3 — Visualization** | Superset + Grafana (parallel) | Superset: AQI overview + trends; Grafana: pipeline health + freshness | Weeks 5–6 |
| **4 — OpenMetadata** | Catalog + lineage | ClickHouse connector; dbt manifest ingestion; glossary; data quality tests | Week 7 |
| **5 — Alerting** | End-to-end alerting | `mart_air_quality__alerts`; Grafana alert rules; notification channels; smoke test | Week 8 |

**Critical path:** Phase 1 → Phase 2 → Phase 3. OpenMetadata (Phase 4) is layered last.

**MONRE policy:** Do NOT make MONRE a Phase 1 blocker. Phase 1 ships complete with three sources (AQICN, AirNow, Sensors.Community) if MONRE is inaccessible.

---

## Confidence Assessment

| Area | Confidence | Basis |
|------|-----------|-------|
| Stack decisions (Superset, Grafana, OpenMetadata) | **High** | Official Docker Hub / docs; existing stack confirmed from codebase |
| AirNow REST API integration details | **High** | Public API docs; reuse existing `api_client.py` pattern |
| Sensors.Community integration | **High** | Public API, no auth; bounding box approach is standard |
| MONRE data access method | **Low** | Access method unconfirmed; may require scraping or formal agreement |
| dbt multi-source refactor approach | **Medium-High** | Pattern well-established; risk is un-audited existing model assumptions |
| OpenMetadata RAM requirements | **High** | Official docs: 4 GB min / 8 GB recommended |
| Phase timeline (8 weeks) | **Medium** | Assumes no major MONRE surprises |

**Overall confidence:** **Medium-High**

**Top gaps to address during planning:**
1. MONRE access method — confirm before Phase 1 or treat as optional source
2. Audit existing dbt models for hardcoded OpenAQ column assumptions before Phase 2
3. Validate ClickHouse materialized views handle Superset query volume before Phase 3

---

*Research completed: 2026-04-01*
*Ready for roadmap: yes*
