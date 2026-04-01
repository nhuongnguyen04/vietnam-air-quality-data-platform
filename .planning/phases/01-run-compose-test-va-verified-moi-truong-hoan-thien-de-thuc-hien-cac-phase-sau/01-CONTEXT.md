# Phase 1: Multi-Source Ingestion - Context

**Gathered:** 2026-04-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace OpenAQ with EPA AirNow, Sensors.Community, and MONRE. AQICN stays as primary. All ingestion additive with zero risk to existing pipeline.

Scope is exactly the 5 plans from ROADMAP.md: Plans 1.1–1.5 covering AirNow client, Sensors.Community client, MONRE client (discovery-first), OpenAQ decommission, and rate limiter + orchestration optimization.

Scope creep is not allowed.
</domain>

<decisions>
## Implementation Decisions

### Dedup Strategy (All New Source Tables)
- **D-01:** All new raw tables (AirNow, Sensors.Community, MONRE) use `ReplacingMergeTree(ingest_time)` engine — dedup on latest ingest_time per ORDER BY key
- **D-02:** Primary key (ORDER BY) set per source to enable proper dedup: `(station_id, timestamp_utc, parameter)` for AirNow; `(sensor_id, timestamp_utc, parameter)` for Sensors.Community
- **D-03:** No Python-side dedup check for new sources — ClickHouse handles dedup server-side
- **D-04:** Resolves CONCERN #7 from Phase 0: eliminates Python dedup race condition for new sources
- **D-05:** AirNow and Sensors.Community sites/sensors tables: `ReplacingMergeTree(ingest_time)` keyed on site_id/sensor_id

### Historical Backfill Scope
- **D-06:** AirNow: backfill 3 months (90 days) from Phase 1 go-live date
- **D-07:** Sensors.Community: backfill 3 months (90 days) from Phase 1 go-live date
- **D-08:** MONRE: no backfill until schema is confirmed stable (discovery-first approach)
- **D-09:** Backfill runs via `dag_ingest_historical` (existing DAG, modified to accept source parameter)
- **D-10:** Historical backfill does NOT count toward Phase 1 success criteria — real-time ingestion success does

### Sensors.Community Quality Handling
- **D-11:** Insert ALL data including outliers — do NOT exclude before insert
- **D-12:** Add `quality_flag` column: `'implausible'` for PM2.5 outside 0–500 µg/m³; `'outlier'` for values statistically anomalous but within range; `'valid'` for normal readings
- **D-13:** Log outlier count to ingestion metrics on each run
- **D-14:** Stations outside Vietnam bounding box (lat 8.4°N–23.4°N, lon 102.1°E–109.5°E) get `quality_flag = 'outlier'` — data inserted but flagged
- **D-15:** Downstream dbt models (Phase 2) filter on `quality_flag = 'valid'` for canonical calculations

### MONRE Fallback Priority
- **D-16:** 2-week timebox for MONRE discovery: focused effort from Phase 1 Day 1
- **D-17:** Discovery scope: confirm access method (API / scraping / manual CSV / data agreement)
- **D-18:** If access confirmed within 2 weeks: implement `dag_monre_ingest` with daily schedule
- **D-19:** If access NOT confirmed within 2 weeks: archive Plan 1.3 discovery results; Phase 1 ships complete with 3 sources (AQICN + AirNow + Sensors.Community)
- **D-20:** Grafana panel shows MONRE status: 'active' / 'discovering' / 'archived' — never 'missing'
- **D-21:** MONRE never blocks Phase 1 completion

### AirNow Integration Details
- **D-22:** Vietnam bounding box: lat 8.4°N–23.4°N, lon 102.1°E–109.5°E
- **D-23:** Store AirNow pre-calculated AQI in `aqi_reported` column; canonical EPA AQI computed in Phase 2 dbt
- **D-24:** AirNow sites table updated daily via `dag_metadata_update`

### Rate Limiter + Orchestration
- **D-25:** One `TokenBucketRateLimiter` per API key, shared across all DAG tasks using that key
- **D-26:** All source ingestion tasks in `dag_ingest_hourly` run in parallel (not sequentially)
- **D-27:** `ingestion.control` updated as final task in all ingestion DAGs
- **D-28:** Zero HTTP 429 errors across all sources — tenacity retry with exponential backoff (base=2, max=5 retries, max_wait=5min)

### OpenAQ Decommission
- **D-29:** OpenAQ tasks removed from `dag_ingest_hourly`
- **D-30:** `python_jobs/jobs/openaq/` directory removed
- **D-31:** OpenAQ raw tables renamed to `raw_openaq_*_archived` (not dropped — rollback safety)
- **D-32:** `dag_metadata_update` updated to remove OpenAQ metadata ingestion

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing Codebase
- `.planning/codebase/AUDIT.md` — Phase 0 audit: OpenAQ schema assumptions, dedup strategy (CONCERN #7), ClickHouse table inventory, dbt OpenAQ references (55 lines across staging/intermediate)
- `.planning/codebase/CONCERNS.md` — CONCERN #7 (Python-side dedup, resolved by D-01–D-03), CONCERN #4 (no retry on 429, resolved by D-28), CONCERN #15 (no freshness monitoring, addressed via D-27)
- `.planning/codebase/CONVENTIONS.md` — Python style, dbt naming, Airflow DAG patterns, Docker Compose structure
- `.planning/ROADMAP.md` § Phase 1 — Phase 1 goal, Plans 1.1–1.5, success criteria, MONRE policy

### Project Planning
- `.planning/PROJECT.md` — Core value, constraints, source selection rationale, out-of-scope items
- `.planning/STATE.md` — Phase 0 completed plans, current project state

### Prior Phase Context
- `.planning/phases/00-foundation/00-CONTEXT.md` — Phase 0 decisions: Vietnam bbox, OpenAQ decommission scope, ReplacingMergeTree recommendation

### Standards
- `.planning/codebase/STACK.md` — Tech stack versions: Python 3.10, ClickHouse 25.12, dbt-core 1.10.13
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `python_jobs/common/api_client.py` — existing `APIClient` base; AirNow and Sensors.Community clients extend this
- `python_jobs/common/rate_limiter.py` — existing `TokenBucketRateLimiter`; shared instance per API key via module-level singleton
- `python_jobs/common/clickhouse_writer.py` — existing `ClickHouseWriter`; use for new source table inserts
- `python_jobs/common/config.py` — existing dataclass config; add new source configs
- `python_jobs/common/ingestion_control.py` — existing `ingestion.control` writer; use for new sources
- `python_jobs/jobs/aqicn/ingest_measurements.py` — existing reference implementation for measurements ingestion

### Established Patterns
- ClickHouse raw table: `ReplacingMergeTree(ingest_time)` with ORDER BY key on `(station_id, timestamp_utc, parameter)` — AQICN forecast table uses this pattern
- Airflow TaskFlow API (`@dag`, `@task`) — standard for all new DAGs
- `ingestion.control` table: updated per source with `source_name`, `last_run_utc`, `rows_inserted`, `lag_seconds`
- Vietnam bounding box already established: lat 8.4°N–23.4°N, lon 102.1°E–109.5°E

### Integration Points
- New DAGs (`dag_sensorscm_poll`, `dag_monre_ingest`) connect to `ingestion.control` table
- AirNow + Sensors.Community tables feed into Phase 2 dbt staging models
- `ingestion.control` feeds into Phase 3 Grafana dashboards
- OpenAQ decommission touches: `dag_ingest_hourly`, `dag_metadata_update`, `python_jobs/jobs/openaq/`, OpenAQ ClickHouse tables

### MONRE Discovery Options
The Phase 1 roadmap leaves MONRE access method open. Discovery should cover:
1. Official MONRE Vietnam environmental data portal (http://vea.gov.vn or similar)
2. Ministry of Natural Resources and Environment open data initiatives
3. Direct API if documented
4. Playwright for JS-rendered portal pages
</code_context>

<specifics>
## Specific Ideas

- MONRE Grafana panel: always shows a status — 'active' (data flowing), 'discovering' (in 2-week window), or 'archived' (timebox expired)
- Sensors.Community: `*/10 * * * *` schedule per Plan 1.2 — 10-minute polling interval
- AirNow: integrated into `dag_ingest_hourly` (hourly), not separate DAG
- Outlier logging: emit metrics/log lines with count of implausible records per run
</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within Phase 1 scope.

</deferred>

---

*Phase: 01-multi-source-ingestion*
*Context gathered: 2026-04-01*
