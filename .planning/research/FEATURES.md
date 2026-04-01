# Features Research — Vietnam Air Quality Data Platform Refactor

> **Research type**: Platform feature dimensioning for multi-source AQ data platform with Superset, Grafana, and OpenMetadata

## 1. Background & Scope

This platform refactor adds four new capability layers to an existing ClickHouse + dbt + Airflow pipeline:

1. **Multi-source data ingestion** (EPA AirNow, Sensors.Community, MONRE) beyond current AQICN/OpenAQ
2. **Superset dashboards** — user-facing BI layer
3. **Grafana monitoring** — pipeline health and data freshness observability
4. **OpenMetadata integration** — data catalog, lineage, and data quality tracking

---

## 2. Feature Categorization

### Table Stakes
> Must have — absence causes immediate trust collapse or operational blindness

| Feature | Complexity | Dependencies | Notes |
|---------|-----------|--------------|-------|
| Multi-source AQI ingestion (hourly, replacing OpenAQ) | Medium | Airflow, ClickHouse | AirNow + Sensors.Community + MONRE |
| Deduplication & schema normalization | Low | ClickHouse tables | ReplacingMergeTree dedup on source + record ID |
| Source health control table (per-source success/failure/lag) | Low | Airflow, ClickHouse | Queried by Grafana; drives alerting |
| ClickHouse audit columns (source, ingest_time, batch_id, raw_payload) | Low | ClickHouse init scripts | Already partially implemented |
| Superset AQI overview dashboard (current values by city) | Medium | Superset, ClickHouse dbt marts | Color-coded AQI; public read-only |
| Superset trend charts (24h/7d AQI per location) | Medium | Superset, ClickHouse dbt marts | Time-series; per-station and city rollup |
| Grafana pipeline health dashboard (DAG success rates, last run) | Low | Grafana, Airflow PostgreSQL | Traffic light per DAG; 7-day history |
| Grafana data freshness alerting (lag > threshold) | Low | Grafana, source control table | e.g., >45 min lag = warning |
| dbt transformation pipeline (staging → intermediate → marts) | Medium | dbt, ClickHouse | Refactored for multi-source canonical schema |
| Airflow orchestration (4 DAGs: ingest, historical, metadata, transform) | Medium | Airflow, Python jobs | Exists; optimize for multi-source |
| Alerting: AQI threshold breach → email/Slack/webhook | Medium | Grafana Alerting, notification channel | e.g., PM2.5 > 150 µg/m³ → Red alert |
| Docker Compose deployment (all services containerized) | Low | Docker Compose | Superset + Grafana + OpenMetadata added |
| Rate limiting per source with backoff | Low | Python jobs | TokenBucket per source; backoff on 429 |
| Ingestion latency tracking (target <15 min) | Low | Source control table, Grafana | Fallback to hourly if rate limited |

### Differentiators
> Competitive advantage — not required for basic function but valued by analysts and government users

| Feature | Complexity | Dependencies | Notes |
|---------|-----------|--------------|-------|
| Source comparison dashboard (multi-source AQI side-by-side) | Medium | Superset, dbt marts with source column | Shows disagreement — builds trust |
| Geographic station map with real-time AQI (deck.gl) | High | Superset + deck.gl, station lat/lng | Big visual impact |
| Pollutant breakdown charts (PM2.5, PM10, O₃, NO₂, SO₂, CO) | Low | Superset, dbt marts | Already in dbt intermediate layer |
| Forecast vs actual charts (AQICN forecast accuracy) | Low | Superset, dbt marts, AQICN forecast table | Forecast data already ingested |
| OpenMetadata data catalog (all tables documented, searchable) | Medium | OpenMetadata, ClickHouse | Auto-ingest via connectors |
| OpenMetadata lineage graph (dbt → ClickHouse → Superset) | Medium | OpenMetadata, dbt manifest | Proof of data flow for stakeholders |
| OpenMetadata data quality tests (null rates, range checks) | Medium | OpenMetadata, source data | SQL-based; schedule + incidents |
| Multi-day forecast charts (3-day, 7-day) | Low | Superset, dbt marts | AQICN forecast already ingested |
| Automated PDF/email reports (weekly AQ summary) | Medium | Grafana reporting or external script | Scheduled export + send |
| Historical backfill UI (one-click date range trigger) | High | Airflow, Airflow trigger API | Analyst-friendly historical loads |
| Airflow DAG-level SLA monitoring | Low | Grafana, Airflow metadata | e.g., all DAGs finish within 25 min |
| Grafana data quality anomaly alerting | Medium | Grafana, source data | Sophisticated freshness + quality |
| Airflow task retry analysis dashboard | Low | Grafana, Airflow metadata | Identifies retry-prone tasks |

### Anti-Features
> Deliberately NOT building — explicit scope decisions

| Anti-Feature | Rationale |
|-------------|-----------|
| Mobile app | Web-first; dashboards via browser |
| Real-time streaming (Kafka/Flink) | Scale doesn't warrant it; batch/near-real-time sufficient |
| Per-user dashboard authentication | Public read-only access; no auth needed |
| ML/prediction models | Deferred to future |
| NiFi | Referenced in .env but not deployed; excluded |
| Commercial BI tools (Tableau, Power BI) | Superset is chosen OSS BI layer |
| Per-user row-level security | Public data; no multi-tenant access control |
| Kafka/Pulsar message queuing | Airflow handles scheduling; no queue needed |
| Airflow CEL executor or Redis queue | LocalExecutor sufficient |
| OpenMetadata ML model metadata or PII scanning | Out of scope |
| Grafana enterprise (reporting, RBAC beyond viewer/admin) | OSS Grafana sufficient |

---

## 3. Feature Dependencies & Build Order

```
Phase 1 — Foundation
├── Multi-source ingestion (Airflow DAGs)
│   └── Rate limiter, dedup strategy, ClickHouse raw tables
├── dbt refactor for multi-source schema
│   └── Raw table schema finalized first
└── Grafana pipeline health dashboard
    └── Airflow PostgreSQL metadata access

Phase 2 — User-Facing Dashboards
├── Superset deployment + ClickHouse connector
│   └── dbt marts tables must exist
├── AQI overview + trend dashboards
│   └── Superset deployed, marts materialized
└── Grafana data freshness + operational alerting
    └── Source health control table populated

Phase 3 — Trust & Operations
├── Source comparison dashboard (Superset)
│   └── All sources ingesting; dbt marts have source column
├── Alerting: AQI threshold → email/Slack (Grafana)
│   └── Grafana, AQI marts
├── OpenMetadata data catalog + lineage
│   └── dbt manifest, ClickHouse schemas stable
└── Automated reports
    └── Superset stable; Grafana reporting or external script

Phase 4 — Polish
├── Geographic station map (deck.gl)
├── Historical backfill UI
├── OpenMetadata data quality tests
└── Multi-day forecast charts
```

---

## 4. Complexity Quick-Reference

| Layer | Dominant Complexity | Key Risk |
|-------|---------------------|---------|
| Multi-source ingestion | Rate limiting edge cases, source-specific schema differences | MONRE may need legal review or formal data agreement |
| dbt refactor | Multi-source canonical schema; source column propagation | Downstream dashboards break on schema changes |
| Superset | Query performance with large ranges on ClickHouse | Timeout if ClickHouse tables not materialized well |
| Grafana | Alert fatigue if thresholds poorly tuned | Too many alerts → ignored alerts |
| OpenMetadata | Catalog hygiene maintenance (stale docs, dead tables) | Stale catalog worse than no catalog |
| Docker Compose | OpenMetadata's own DB backend (5th service) | Port conflicts; increased ops surface |

---

## 5. Cross-Cutting Concerns

1. **Data freshness is the #1 user trust metric.** Stale dashboards destroy credibility regardless of visualization quality. Grafana freshness monitoring is table stakes.
2. **Source disagreement is a feature, not a bug.** Showing AQICN vs EPA vs Sensors.Community side-by-side demonstrates transparency and builds trust.
3. **OpenMetadata only pays off if the team commits to keeping it current.** A stale catalog is worse than no catalog.
4. **Separate operational from business alerting.** DAG failures → ops channels. AQI breaches → public dashboards + email/Slack.
5. **MONRE is the highest-risk source.** Handle partial/unavailable data gracefully. Do not make it a Phase 1 blocker.

---

*Research basis: domain knowledge of multi-source data platforms, OSS BI tooling (Superset 3.x), Grafana OSS, OpenMetadata 1.x, and air quality data domain (AQI standards, EPA/MONRE source characteristics).*
