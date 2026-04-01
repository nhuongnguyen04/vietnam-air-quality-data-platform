# Phase 0: Foundation & Stabilization — Research

**Phase:** 00-foundation
**Role:** Research — "What do I need to know to PLAN this phase well?"
**Date:** 2026-04-01
**Status:** Complete

---

## Executive Summary

Phase 0 is purely infrastructure and process work — no new features, no new data sources. Its purpose is to make the existing brownfield baseline visible, resource-safe, testable, and stable. Five plans cover: audit (0.1), Docker hardening (0.2), CI bootstrap (0.3), stability baseline (0.4), and ingestion control (0.5).

The critical path runs **0.3 → 0.4**: CI must exist before reliable 7-day monitoring can be validated. Docker hardening (0.2) should run in parallel with audit (0.1) since no file changes overlap. The ingestion control table (0.5) is a prerequisite for Plan 3.4 Grafana freshness panels and Plan 5.2 alerting — it must be completed in Phase 0, not deferred.

---

## Plan-by-Plan Research

### Plan 0.1 — Baseline Codebase Audit

**Scope boundary (D-01, D-03):** This is a *focused* audit — inventory only what's needed for Phase 1 planning. Not a full 16-item CONCERNS inventory.

**Three deliverables required:**

#### 1. OpenAQ schema assumptions (hardcoded in dbt WHERE clauses / Python jobs)

**What to look for in dbt staging models** (`dbt/dbt_tranform/models/staging/openaq/`):
- `stg_openaq__measurements.sql` — likely assumes OpenAQ parameter names (`pm25`, `pm10`, etc.) hardcoded in WHERE or CASE
- `stg_openaq__locations.sql` — may assume OpenAQ-specific location fields
- Look for: `WHERE source = 'openaq'`, column name references from OpenAQ API response (`period_datetime_from_utc`, `location_id`, etc.)

**What to look for in Python jobs** (`python_jobs/jobs/openaq/`):
- `ingest_measurements.py` — likely assumes specific JSON shape from OpenAQ v3 API
- API response fields: `period.datetimeFrom.utc`, `period.datetimeTo.utc`, `location.id`, `sensor.id`, `parameter.id`, `value`, `coverage.*`
- Key assumption to surface: dedup key is `(location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc)` stored in `raw_payload`

**What to look for in dbt intermediate/marts**:
- `int_unified__measurements.sql` — does it UNION with other sources or is OpenAQ assumed as the only source?
- `int_aqi_calculations.sql` — AQI formula source-specific or canonical? (Current: likely OpenAQ-specific)
- Marts (`mart_air_quality__hourly`, etc.) — do they JOIN on OpenAQ-specific IDs?

#### 2. ClickHouse table inventory

| Table | Engine | Dedup Key | Phase 1 Impact |
|---|---|---|---|
| `raw_openaq_measurements` | `MergeTree()` | Python dedup (CONCERN #7) | Must migrate to `ReplacingMergeTree` in Plan 2.4 |
| `raw_openaq_locations` | `ReplacingMergeTree(ingest_time)` | `location_id` | Will be archived in Plan 1.4 |
| `raw_openaq_parameters` | `ReplacingMergeTree(ingest_time)` | `parameter_id` | Will be archived in Plan 1.4 |
| `raw_openaq_sensors` | `ReplacingMergeTree(ingest_time)` | `sensor_id` | Will be archived in Plan 1.4 |
| `raw_aqicn_measurements` | `MergeTree()` | Python dedup | Phase 1 primary — needs AirNow-style dedup |
| `raw_aqicn_forecast` | `ReplacingMergeTree(ingest_time)` | sort key | Phase 1 stays |
| `raw_aqicn_stations` | `ReplacingMergeTree(ingest_time)` | `station_id` | Phase 1 stays |

**Critical finding (CONCERN #7):** `raw_openaq_measurements` uses `MergeTree()` with a Python-side dedup strategy. If the Python dedup check fails or races, duplicates accumulate. This must be documented as a known risk in `AUDIT.md` and addressed in Plan 2.4 (ClickHouse schema migration — Phase 2).

#### 3. Airflow DAG task dependency map

From `dag_ingest_hourly.py`:
```
check_clickhouse
       ↓
ensure_metadata
       ↓
[run_openaq_measurements, run_aqicn_measurements, run_aqicn_forecast]  (parallel)
       ↓
log_completion
```

- Uses Airflow 3 TaskFlow API (`@dag`, `@task`) — correct pattern
- Env vars captured in `get_job_env_vars()` function — correct pattern (D-01)
- Sequential metadata → parallel measurements → completion — fine for current scale
- `dag_transform`: sequential dbt_deps → dbt_seed → dbt_run_staging → dbt_run_intermediate → dbt_run_marts → dbt_test
- `dag_metadata_update`: OpenAQ-only (parameters, locations, sensors)
- `dag_ingest_historical`: manual-only, `max_active_runs=1`, no semaphore (CONCERN #14)

#### DB Name Audit (D-09/D-10) — Inline Finding

| Config Location | DB Name | Note |
|---|---|---|
| `.env` | `air_quality` | With underscore |
| `docker-compose.yml` | `${CLICKHOUSE_DB}` | Passes through from `.env` — "air_quality" |
| `scripts/init-clickhouse.sql` | `${CLICKHOUSE_DB}` | Docker Compose substitutes → "air_quality" |
| `dbt/profiles.yml` dev/prod | `air_quality` | Hardcoded default in Jinja `env_var()` |
| `CONVENTIONS.md` | `airquality` | Section 6 says "air_quality"; consistent |
| CONCERNS #11 | Says: "standardize to `airquality`" | Incorrect — `.env` uses `air_quality` |

**Finding:** CONCERNS #11 is partially wrong. The DB name is already consistent across all configs as `air_quality` (with underscore). The concern suggested standardizing to `airquality` (no underscore) but the codebase already uses `air_quality`. **Decision D-10** says this decision is deferred to audit findings. The audit should verify the DB is consistently named `air_quality` and confirm no change is needed, closing CONCERN #11 as a non-issue.

**D-09/D-10 conclusion:** The DB name is already consistent as `air_quality`. No fix is needed. Audit should verify this by checking all config files.

---

### Plan 0.2 — Docker Compose Resource Hardening

**D-16 specifies** adding `mem_limit` and `cpu_limit` to ALL services. D-17 specifies RAM allocations totaling ~11GB against a 13GB Docker pool (2GB host OS headroom).

**D-20:** `airflow-webserver` has no additional memory limit (current usage is acceptable). **Do not add a mem_limit to `airflow-webserver`.**

**Current docker-compose.yml status:**
- ✅ `clickhouse`: No limits (needs: 3GB mem)
- ❌ `airflow-webserver`: No limits — **D-20 says do not add**
- ❌ `airflow-scheduler`: No limits (needs: 512MB mem)
- ❌ `airflow-dag-processor`: No limits (needs: 512MB mem)
- ❌ `airflow-triggerer`: No limits (needs: 512MB mem)
- ✅ `postgres`: No limits (needs: 1GB mem)
- ❌ `airflow-permissions`: No limits (needs: minimal, no limit needed)

**Health check status:**
- ✅ `clickhouse`: Healthcheck exists
- ✅ `postgres`: Healthcheck exists
- ✅ `airflow-webserver`: Healthcheck exists
- ❌ `airflow-scheduler`: No healthcheck (D-18)
- ❌ `airflow-dag-processor`: No healthcheck (D-18)
- ❌ `airflow-triggerer`: No healthcheck (D-18)

**Health check implementation options for scheduler/dag-processor/triggerer:**
1. Use `pg_isready` against the PostgreSQL metadata DB (all three already depend on `postgres:5432`)
2. Use Airflow's own health endpoint: `curl http://localhost:8080/health` (scheduler exposes on different ports)
3. Simpler: check process existence via `ps aux | grep [s]cheduler` in a shell healthcheck

**Recommendation:** Use a lightweight HTTP health check if Airflow exposes one, otherwise `pg_isready` against the shared PostgreSQL. The three Airflow services already depend on `postgres` being healthy.

**RAM total check:**
- ClickHouse: 3GB
- PostgreSQL: 1GB
- Airflow scheduler: 512MB
- Airflow dag-processor: 512MB
- Airflow triggerer: 512MB
- OpenMetadata (future, Phase 4): 4GB
- Superset (future, Phase 3): 1GB
- Grafana (future, Phase 3): 512MB
- **Total: ~11.5GB**

This leaves ~1.5GB headroom on the 13GB Docker pool, which is acceptable. If host machine is exactly 13GB total, Phase 0 should document 16GB minimum (D-19).

---

### Plan 0.3 — CI Pipeline Bootstrap

**D-04/D-05:** Full dbt run in CI — `dbt deps` → `dbt seed` → `dbt run` → `dbt test` against test ClickHouse. Not `dbt compile` only.

**D-07/D-08:** GitHub-hosted Ubuntu runners. Standard GitHub Actions workflow at `.github/workflows/ci.yml`.

**What currently exists:**
- `sqlfluff==3.5.0` already in `requirements.txt` ✅
- `sqlfluff-templater-dbt==3.5.0` already in `requirements.txt` ✅
- `ruff check` **NOT** in `requirements.txt` — must be added (D-08, CONCERN #8)
- `.github/workflows/` directory **does not exist** — must be created (CONCERN #8)
- `dbt target/` already in `.gitignore` ✅
- No `ci` target in `dbt/profiles.yml` — must be added

**CI pipeline design:**

```
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - checkout
      - python:3.10
      - run: pip install sqlfluff==3.5.0 sqlfluff-templater-dbt==3.5.0 ruff
      - run: sqlfluff lint dbt/dbt_tranform/ --config dbt/dbt_tranform/.sqlfluff
      - run: ruff check python_jobs/ --config .ruff.toml

  dbt-ci:
    needs: lint
    runs-on: ubuntu-latest
    services:
      clickhouse:
        image: clickhouse/clickhouse-server:25.12
        ports:
          - 8123:8123
    steps:
      - checkout
      - name: Setup test ClickHouse schema
        run: docker compose -f docker-compose.test.yml run init-schema
      - run: pip install dbt-core==1.10.13 dbt-clickhouse==1.9.5
      - run: dbt deps --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
      - run: dbt seed --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
      - run: dbt run --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
      - run: dbt test --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
```

**Key design decisions:**
1. `dbt-ci` job uses GitHub Actions `services` block for ClickHouse (no separate `docker-compose.test.yml` needed for ClickHouse alone)
2. If full `docker-compose.test.yml` is needed (PostgreSQL for Airflow metadata, etc.), use a separate compose file
3. CI target in `profiles.yml` must connect to `localhost:8123` with no auth (GitHub Actions services use exposed ports)
4. Seed data must exist for CI to pass `dbt test` — what seed files exist? Check `dbt/dbt_tranform/seeds/`

**Action item before Plan 0.3 implementation:** Verify `dbt/dbt_tranform/seeds/` directory contents. CI `dbt seed` step will fail if no seed files exist.

**D-14/D-15:** Merge blocking — use GitHub branch protection rules + `on: pull_request` trigger. Merge to main blocked if CI fails. This is a GitHub repo setting, not code.

---

### Plan 0.4 — AQICN-Only Stability Baseline

**D-12:** 100% DAG success rate over 7 consecutive days. This is a manual operational run, not code changes.

**Prerequisite:** Plan 0.2 (Docker resource limits) must be complete to rule out OOM as a failure cause during the baseline.

**What "disable OpenAQ" means in practice:**
- In `dag_ingest_hourly.py`, remove `run_openaq_measurements` from the parallel task list: `[aqicn, forecast] >> completion` instead of `[openaq, aqicn, forecast] >> completion`
- This is a single-line change in the DAG file
- OpenAQ tasks are still in the DAG file (commented out), just not wired into the dependency chain

**What to monitor during 7-day run:**
1. DAG success/failure in Airflow UI or via `docker compose ps`
2. Row count growth in `raw_aqicn_measurements` — must be linear, not super-linear (duplicate detection)
3. `docker stats` during each run — no OOM events
4. API response times — AQICN API stability
5. `dag_transform` row counts — verify staging/intermediate/mart counts are stable

**Baseline metrics to document:**
- Rows ingested per day (AQICN)
- Storage growth rate (MB/day)
- API call count (from Airflow task logs)
- DAG runtime per run

**D-13:** The 7-day baseline also validates the DB name fix from D-09/D-10. If ClickHouse queries work correctly with `air_quality` DB, the DB name is confirmed consistent.

---

### Plan 0.5 — Ingestion Control Table

**ROADMAP.md specifies this exact schema:**
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

**Key integration points:**
1. **Airflow update:** After each ingestion task completes, a PythonOperator updates `ingestion.control` with:
   - `source`: e.g., `'aqicn'`, `'airnow'`, `'sensorscm'`
   - `last_run`: current timestamp
   - `last_success`: timestamp if DAG succeeded, NULL if failed
   - `records_ingested`: count of rows written in this run
   - `lag_seconds`: `(last_success - last_run)` in seconds
   - `error_message`: Airflow task exception text if failed

2. **Grafana integration (Phase 3, Plan 3.4):** `ingestion.control` becomes the datasource for:
   - "Data Freshness" dashboard panel: `max(updated_at) WHERE source = 'aqicn'`
   - Alerting: `max(updated_at) < now() - interval 3 hour`

3. **Alerting integration (Phase 5, Plan 5.2):** `lag_seconds > 3600` for hourly sources → Slack notification

**Important:** This table must be created in Phase 0 so that Plan 3.4 and Plan 5.2 can build on it. Creating it later (Phase 3) would miss Phase 0–3 baseline data.

**Schema decision note:** The `airquality` DB name in the ROADMAP SQL (without underscore) is a typo — should be `air_quality` to match the actual DB. Audit (Plan 0.1) should catch and correct this.

**Ingestion control update implementation pattern:**
```python
# In dag_ingest_hourly.py — add as final task after [aqicn, forecast] completes
@task
def update_ingestion_control(source: str, records_count: int, success: bool, error: str = None):
    """Update ingestion.control table after each source run."""
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    now = datetime.utcnow()
    client.insert(
        'air_quality.ingestion_control',
        [[
            source,
            now,           # last_run
            now if success else None,  # last_success
            records_count,
            0 if success else -1,  # lag_seconds
            error or ''
        ]],
        column_names=['source', 'last_run', 'last_success', 'records_ingested', 'lag_seconds', 'error_message']
    )
```

---

## Cross-Plan Dependencies & Critical Path

```
Plan 0.1 (Audit)  ────────────────────────────────┐
     │                                               │
     │ (DB name findings feed D-09/D-10 decision)    │
     ▼                                               │
Plan 0.2 (Docker Hardening)  ───────────────────────┤
     │                                               │
     │ (Health checks needed for 0.4 monitoring)     │  ← Parallel with 0.1
     ▼                                               │
Plan 0.3 (CI Pipeline)                             │
     │                                               │
     │ (CI validates 0.4 stability runs)            │  ← Sequential: 0.3 before 0.4
     ▼                                               │
Plan 0.4 (Stability Baseline)                       │
     │                                               │
     │ (Control table needed for 0.5 integration)    │  ← 0.5 can run in parallel with 0.4
     ▼                                               │
Plan 0.5 (Ingestion Control)                       │
     │                                               │
     └─── Phase 3.4 (Grafana Freshness) ─────────────┘
          Phase 5.2 (Alerting)
```

---

## Known Gaps Requiring Investigation During Audit

### Gap 1: `.sqlfluff` config file
`sqlfluff==3.5.0` is in `requirements.txt` but no `.sqlfluff` or `pyproject.toml` with `[tool.sqlfluff]` exists in the repo root or `dbt/dbt_tranform/`. Plan 0.3 implementation must either create this file or use command-line overrides. **Action:** Create `.sqlfluff` with ClickHouse dialect and standard rules.

### Gap 2: `.ruff.toml` config file
`ruff check` is not in `requirements.txt`. Plan 0.3 implementation must add it AND create `.ruff.toml`. The project has no existing Python linting config. **Action:** Add `ruff` to `requirements.txt` and create `.ruff.toml` with PEP 8 rules + ignore `E501` (line length) unless the codebase is already clean.

### Gap 3: `dbt/dbt_tranform/seeds/` directory
CI step `dbt seed` requires seed files to exist. Check if this directory exists and has content. If empty, CI will error on `dbt seed`. **Action:** Verify existence during audit.

### Gap 4: `clickhouse-connect` in CI
The `update_ingestion_control` task in Plan 0.5 uses `clickhouse_connect`. This package is in `requirements.txt` but may not be available in the GitHub Actions runner. **Action:** CI must either install from `requirements.txt` or create a minimal CI-specific requirements file.

### Gap 5: `.github/` directory
Does not exist. Plan 0.3 creates it. **Action:** Create with correct permissions.

### Gap 6: `docker-compose.test.yml`
ROADMAP Plan 0.3 mentions this file. D-06 says: "minimal: ClickHouse + test runner only, no Superset/Grafana/OpenMetadata." GitHub Actions `services` block may replace this for ClickHouse-only CI, but if Airflow-level integration tests are needed, the compose file is required. **Decision:** Start with `services` block only; create `docker-compose.test.yml` only if proven necessary.

---

## What to Verify in the Audit (Plan 0.1)

1. **Verify DB name consistency:** Check all config files for `air_quality` vs `airquality`. Confirm CONCERN #11 is a non-issue (already consistent) or requires a fix.
2. **Count dbt models by layer:** staging views, intermediate views, mart tables — confirm expected numbers.
3. **Find hardcoded source names:** Search for `'openaq'` in dbt SQL files and Python jobs — this is the primary audit deliverable.
4. **Verify dedup implementation:** In Python jobs — is dedup done by checking ClickHouse before insert, or by assuming MergeTree dedupes? This is CONCERN #7.
5. **Check for API retry at HTTP level:** `api_client.py` has `urllib3.util.retry.Retry` configured for 429/5xx. Is this actually working or is it bypassed? CONCERN #4.
6. **Audit `ensure_metadata` task:** Does it skip if `raw_openaq_locations` has data? What if AQICN metadata is missing but OpenAQ is not? This is a latent bug to document.

---

## Open Questions for the Planner

1. **Hardware reality check:** Is the host machine actually 13GB RAM, or 16GB? D-19 says document 16GB minimum. The RAM math (11GB containers + 2GB headroom = 13GB pool) assumes 13GB total. If the machine is 16GB, there is 3GB headroom. Need to confirm actual machine specs.

2. **AQICN-only baseline — what does "success" mean?** D-12 says "100% DAG success rate." Does this mean all tasks in `dag_ingest_hourly` succeed? Or does it mean `dag_ingest_hourly` AND `dag_transform` both succeed? The baseline should include the full pipeline (ingestion + transform).

3. **7-day baseline timing:** When does this start? After Plans 0.1, 0.2, 0.3 are complete? Or sequentially after each plan? D-12 is ambiguous. Recommend: start after Plans 0.1 + 0.2 are complete, CI (0.3) is running.

4. **Secrets in `.env`:** CONCERN #1 (API tokens in `.env`) and CONCERN #2 (empty `FERNET_KEY`) are High Priority but not addressed in any Phase 0 plan. Should these be addressed in Phase 0 or deferred? Recommendation: Phase 0 addresses the infrastructure (CI, Docker) but secrets management (CONCERN #1) requires architectural decision (Docker secrets? Vault? GitHub Actions secrets?) and should be its own plan in Phase 0 or deferred to Phase 5.

---

## Reference: Decisions Already Made (D-01 to D-20)

These do not need re-deciding — they are constraints for the planner:

| Decision | Summary |
|---|---|
| D-01 | Audit is focused on Phase 1 scope only |
| D-04 | CI does full `dbt run` + `dbt test` — not `dbt compile` only |
| D-06 | Test infra: `docker-compose.test.yml` (minimal: ClickHouse + runner) |
| D-07 | GitHub-hosted Ubuntu runners (no self-hosted) |
| D-08 | Workflow at `.github/workflows/ci.yml` |
| D-09 | DB name fix during Phase 0 — not deferred |
| D-10 | DB name decision deferred to audit findings |
| D-12 | 7-day AQICN-only baseline = 100% success rate |
| D-14 | CI blocks merge to main |
| D-16 | `mem_limit` + `cpu_limit` on ALL services |
| D-17 | Specific RAM allocations per service |
| D-18 | Add health checks to scheduler, dag-processor, triggerer |
| D-19 | Document 16GB RAM minimum |
| D-20 | No additional memory limit on `airflow-webserver` |

---

*Research completed: 2026-04-01*
*Downstream consumers: 00-PLAN.md (plan author), Phase 1 planner*
