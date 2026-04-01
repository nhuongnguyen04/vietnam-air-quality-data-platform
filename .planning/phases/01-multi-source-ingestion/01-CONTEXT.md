# Phase 1: Multi-Source Ingestion - Context

**Gathered:** 2026-04-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace OpenAQ with OpenWeather Air Pollution API, WAQI/World Air Quality Index, and Sensors.Community. AQICN stays as primary. All ingestion additive with zero risk to existing pipeline.

EPA AirNow is NOT viable for Vietnam (US/Canada-only network). MONRE is NOT viable for Phase 1 (no public API found, requires government agreement). These two sources are replaced by OpenWeather + WAQI.

Scope is exactly 5 plans: Plans 1.1–1.5 covering OpenWeather client, WAQI client, Sensors.Community client, OpenAQ decommission, and rate limiter + orchestration optimization.

Scope creep is not allowed.
</domain>

<decisions>
## Implementation Decisions

### Source Coverage Update (2026-04-01)
- **EPA AirNow REPLACED:** AirNow API is US/Canada-only — no Vietnam stations. Replaced by OpenWeather Air Pollution API (Plan 1.1) and WAQI/World Air Quality Index (Plan 1.2).
- **MONRE REPLACED:** No public API found; government agreement required. Replaced by WAQI (already covers Hanoi, HCMC, Da Nang stations).
- Phase 1 now ships with 4 sources: AQICN (existing) + OpenWeather + WAQI + Sensors.Community.

### Dedup Strategy (All New Source Tables)
- **D-01:** All new raw tables (OpenWeather, WAQI, Sensors.Community) use `ReplacingMergeTree(ingest_time)` engine — dedup on latest ingest_time per ORDER BY key
- **D-02:** Primary key (ORDER BY) per source: `(station_id, timestamp_utc, parameter)` for OpenWeather; `(station_id, timestamp_utc, parameter)` for WAQI; `(sensor_id, timestamp_utc, parameter)` for Sensors.Community
- **D-03:** No Python-side dedup check for new sources — ClickHouse handles dedup server-side
- **D-04:** Resolves CONCERN #7 from Phase 0: eliminates Python dedup race condition for new sources
- **D-05:** Sites/sensors tables: `ReplacingMergeTree(ingest_time)` keyed on station_id/sensor_id

### Historical Backfill Scope
- **D-06:** OpenWeather: backfill available via `/history` endpoint from Nov 2020. Backfill 90 days from Phase 1 go-live date via `dag_ingest_historical`.
- **D-07:** WAQI: historical data available but limited. Backfill 30 days from Phase 1 go-live date (WAQI free tier historical is ~30 days).
- **D-08:** Sensors.Community: no historical endpoint — real-time polling only. No backfill possible.
- **D-09:** Backfill runs via `dag_ingest_historical` (existing DAG, modified to accept source parameter)
- **D-10:** Historical backfill does NOT count toward Phase 1 success criteria — real-time ingestion success does

### OpenWeather Air Pollution API Details
- **D-11:** API base: `https://api.openweathermap.org/data/2.5`
- **D-12:** Endpoints: `/air_pollution` (current), `/air_pollution/forecast` (4-day hourly), `/air_pollution/history` (from Nov 2020)
- **D-13:** Free tier: 60 req/min, 1M calls/month. One API key covers all endpoints.
- **D-14:** Vietnam polling: Use lat/lon grid (city-level: Hanoi ~21.0°N/105.8°E, HCMC ~10.8°N/106.7°E, Da Nang ~16.1°N/108.2°E) — OpenWeather uses city-centroid polling, not station discovery.
- **D-15:** Fields: PM2.5, PM10, O₃, NO₂, SO₂, CO, NH₃ + EPA AQI index (`main.aqi`)
- **D-16:** Store OpenWeather's reported AQI in `aqi_reported` column; canonical AQI computed in Phase 2 dbt

### WAQI / World Air Quality Index Details
- **D-17:** API base: `https://api.waqi.info/feed/`
- **D-18:** Vietnam bounding-box query: `/feed/geo:8.4;102.1;23.4;109.5/` — returns ALL stations in Vietnam in one call
- **D-19:** Free, no billing card. Token via query param `?token=`. Rate limit ~1,000 req/min (server-side).
- **D-20:** Fields: PM2.5, PM10, O₃, NO₂, SO₂, CO + aqicn-reported AQI
- **D-21:** Stations are real monitoring locations (Hanoi, HCMC, Da Nang, others) — not estimates
- **D-22:** Note: aqicn.org = waqi.info = same project (separate from IQAir)

### Sensors.Community Quality Handling
- **D-23:** Insert ALL data including outliers — do NOT exclude before insert
- **D-24:** Add `quality_flag` column: `'implausible'` for PM2.5 outside 0–500 µg/m³; `'outlier'` for values statistically anomalous but within range; `'valid'` for normal readings
- **D-25:** Log outlier count to ingestion metrics on each run
- **D-26:** Stations outside Vietnam bounding box get `quality_flag = 'outlier'` — data inserted but flagged
- **D-27:** Downstream dbt models (Phase 2) filter on `quality_flag = 'valid'` for canonical calculations

### Rate Limiter + Orchestration
- **D-28:** One `TokenBucketRateLimiter` per API key: `openweather` (~50 req/min safe), `waqi` (~100 req/min safe), `aqicn` (~60 req/min), `sensorscm` (unlimited — no auth)
- **D-29:** All source ingestion tasks in `dag_ingest_hourly` run in parallel (not sequentially)
- **D-30:** `ingestion.control` updated as final task in all ingestion DAGs
- **D-31:** Zero HTTP 429 errors across all sources — tenacity retry with exponential backoff (base=2, max=5 retries, max_wait=5min)

### OpenAQ Decommission
- **D-32:** OpenAQ tasks removed from `dag_ingest_hourly`
- **D-33:** `python_jobs/jobs/openaq/` directory removed
- **D-34:** OpenAQ raw tables renamed to `raw_openaq_*_archived` (not dropped — rollback safety)
- **D-35:** `dag_metadata_update` updated to remove OpenAQ metadata ingestion

### Sources Overview (Phase 1)
| Source | Plan | API | Auth | Vietnam Coverage | Historical |
|--------|------|-----|------|-----------------|------------|
| AQICN | existing | aqicn.org | token | Major cities | 3 days |
| OpenWeather | 1.1 | openweathermap.org | API key | City centroids | From Nov 2020 ✓ |
| WAQI | 1.2 | api.waqi.info | token | Real stations (Hanoi, HCMC, Da Nang, others) | ~30 days |
| Sensors.Community | 1.3 | api.luftdaten.info | none | Community sensors | None — real-time only |

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

- OpenWeather: `*/10 * * * *` schedule — 10-minute polling for current + forecast endpoints
- WAQI: `*/10 * * * *` schedule — 10-minute polling (one bounding-box call per run)
- Sensors.Community: `*/10 * * * *` schedule — 10-minute polling interval
- All three new sources run in parallel within `dag_ingest_hourly` (not sequentially)
- Outlier logging: emit metrics/log lines with count of implausible records per run
- OpenWeather city polling: use 3 city centroids (Hanoi, HCMC, Da Nang) initially; expand to more cities as needed
- WAQI: bounding-box call returns all Vietnam stations in one response — parse and insert per-station records
</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within Phase 1 scope.

</deferred>

---

*Phase: 01-multi-source-ingestion*
*Context directory: .planning/phases/01-multi-source-ingestion/*
*Context gathered: 2026-04-01*
