# State — Vietnam Air Quality Data Platform Refactor

**Project:** Vietnam Air Quality Data Platform Refactor
**Milestone:** Refactor & Upgrade
**Phase:** 0 (Foundation & Stabilization)
**Plan:** 0.1 — Baseline Codebase Audit
**Mode:** YOLO | **Granularity:** Standard

---

## Project Reference

See: `.planning/PROJECT.md` (updated 2026-04-01 after initialization)

**Core value:** Reliable, near-real-time air quality monitoring for Vietnam — trusted data from multiple sources, cleaned and unified, available to analysts and the public via dashboards and alerts.

**Current focus:** Phase 0 — Foundation & Stabilization (baseline audit + CI bootstrap + resource hardening)

---

## Phase Status

| Phase | Status | Plans | Completed |
|-------|--------|-------|-----------|
| 0 — Foundation & Stabilization | 🔵 In Progress | 5 | — |
| 1 — Multi-Source Ingestion | ⬜ Pending | 5 | — |
| 2 — dbt Refactor | ⬜ Pending | 5 | — |
| 3 — Visualization & Monitoring | ⬜ Pending | 5 | — |
| 4 — OpenMetadata Integration | ⬜ Pending | 4 | — |
| 5 — Alerting & Reporting | ⬜ Pending | 5 | — |

---

## Active Plan

**Plan 0.1 — Baseline Codebase Audit**

Next action: Inventory all ClickHouse tables, dbt models, and Airflow DAGs. Document OpenAQ schema assumptions.

---

## Upcoming

**Plan 0.2** — Docker Compose resource hardening: add mem/CPU limits to all services.

---

## Recent Commits

- `6f27455` — docs: add ecosystem research (STACK, FEATURES, ARCHITECTURE, PITFALLS, SUMMARY)
- `6fdfcf7` — docs: initialize project (PROJECT.md)

---

## Key Context

- **Stack:** ClickHouse 25.12 + dbt-core 1.10.13 + dbt-clickhouse 1.9.5 + Airflow 3.1.7
- **New tools:** Superset 4.x, Grafana 11.x, OpenMetadata 1.1.x
- **MONRE policy:** Phase 1 ships complete with 3 sources if MONRE is inaccessible — never a blocker
- **YOLO mode:** Auto-approve plan checks, verifier, and roadmap approval

---
*Last updated: 2026-04-01 after roadmap creation*
