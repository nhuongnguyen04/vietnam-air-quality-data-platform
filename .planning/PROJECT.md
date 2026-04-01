# Vietnam Air Quality Data Platform — Refactor & Upgrade

## What This Is

A comprehensive data engineering platform that ingests, transforms, and visualizes air quality data for Vietnam from multiple external sources (IQAir/AQICN, EPA AirNow, Sensors.Community, and government/MONRE data), stores it in ClickHouse, and exposes it through Superset dashboards, Grafana monitoring, and automated reports with alerting.

**This is a brownfield project.** The existing codebase already has OpenAQ ingestion, ClickHouse storage, dbt transformations, and Airflow orchestration. This refactor replaces the data source layer, modernizes the entire pipeline, and adds visualization + metadata management.

## Core Value

Reliable, near-real-time air quality monitoring for Vietnam — trusted data from multiple sources, cleaned and unified, available to analysts and the public via dashboards and alerts.

## Requirements

### Validated

<!-- Existing capabilities from the current codebase. These are confirmed working. -->

- ✓ OpenAQ ingestion (parameters, locations, sensors, measurements) — existing, to be replaced
- ✓ ClickHouse storage with MergeTree/ReplacingMergeTree tables — existing, stays
- ✓ dbt staging/intermediate/marts transformation pipeline — existing, to be refactored
- ✓ Airflow 3.1 orchestration (4 DAGs: hourly ingest, historical, metadata, transform) — existing, to be optimized
- ✓ AQICN ingestion (measurements, forecast) — existing, becomes primary source
- ✓ Docker Compose deployment (ClickHouse + Airflow + PostgreSQL) — existing

### Active

- [ ] **Multi-source ingestion**: Replace OpenAQ with EPA AirNow, Sensors.Community, and MONRE/government data
- [ ] **Pipeline optimization**: Improve ingestion reliability, rate limiting, deduplication, and orchestration
- [ ] **dbt refactor**: Redesign transformation layer for new data sources
- [ ] **Airflow optimization**: Improve DAG structure, error handling, and scheduling
- [ ] **Near-real-time ingestion**: Minimize latency from data source to dashboard (target: <15 min if stable)
- [ ] **Superset deployment**: New Superset instance connected to ClickHouse
- [ ] **Grafana monitoring**: Monitor pipeline health, data freshness, and system metrics
- [ ] **Dashboard**: AQI overview, trends, station maps, pollutant breakdowns
- [ ] **Automated reports**: Periodic air quality summary reports
- [ ] **Alerting**: AQI threshold alerts (push to email/Slack/webhook)
- [ ] **OpenMetadata integration**: Data catalog, data quality monitoring, schema registry

### Out of Scope

- Mobile app — web-first, dashboards accessible via browser
- Real-time streaming (Kafka/Flink) — too complex for current scale; batch/near-real-time sufficient
- Monetization or user authentication on dashboards — public read-only access
- Prediction/ML models — deferred to future work
- NiFi — referenced in .env.dev but not deployed; excluded from this refactor

## Context

**Existing system:** Vietnam Air Quality Data Platform — a batch ingestion pipeline (hourly) pulling from OpenAQ and AQICN into ClickHouse, transformed via dbt, orchestrated by Airflow 3.1. Infrastructure runs on Docker Compose locally.

**Pain points addressed:**
- OpenAQ API has become unreliable/restricted for Vietnam data
- No visualization layer — data is in ClickHouse but not exposed to end users
- No monitoring beyond Airflow logs
- dbt models may not handle the new multi-source schema correctly
- No metadata/data catalog — unclear what tables mean or where data comes from
- AQICN is currently the only stable source; needs diversification

**Source selection rationale:**
- **IQAir/AQICN**: Primary source (already integrated), good VN coverage
- **EPA AirNow**: Free API, global coverage, reliable government data
- **Sensors.Community**: Community sensor network, complementary data, open API
- **MONRE/Government**: Official Vietnamese government data; may require scraping or data portal access

## Constraints

- **Tech stack**: Python, ClickHouse, dbt, Airflow, Docker Compose — existing, no wholesale replacement
- **New additions**: Superset, Grafana, OpenMetadata — to be containerized alongside existing services
- **Near-real-time**: Target <15 min ingestion latency if sources and API rate limits permit; fall back to hourly if unstable
- **Vietnam focus**: All sources must have measurable Vietnam data coverage
- **API costs**: Prefer free-tier APIs; IQAir/AQICN token already exists
- **Stability over speed**: Get it working reliably before optimizing for speed

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Replace OpenAQ | Unreliable for VN data; API restrictions increasing | — Pending |
| Multi-source (4 sources) | Redundancy + broader VN coverage | — Pending |
| Near-real-time if stable | Freshness matters for air quality alerts | — Pending |
| Superset (new) | User chose new deployment over existing | — Pending |
| OpenMetadata | User selected over DataHub/Atlas/Amundsen | — Pending |
| Grafana for monitoring | Already referenced in project; monitoring layer needed | — Pending |
| Superset + Grafana in docker-compose | Keep deployment unified | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd:transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd:complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state (sources, data coverage, user feedback)

---
*Last updated: 2026-04-01 — Phase 01 complete: test infrastructure stubs in place (29 pytest fixtures/stubs, pytest.ini, Wave 1 source validators ready)*
