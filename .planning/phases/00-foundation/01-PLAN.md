---
wave: 1
depends_on: []
files_modified:
  - .planning/codebase/AUDIT.md
  - airflow/dags/dag_ingest_hourly.py
autonomous: false
---

# Plan 0.1 — Baseline Codebase Audit

**Plan:** 0.1
**Phase:** 00-foundation
**Wave:** 1 (parallel with 0.2)
**Owner:** data engineering

---
```yaml
wave: 1
depends_on: []
files_modified:
  - .planning/codebase/AUDIT.md
  - airflow/dags/dag_ingest_hourly.py
autonomous: false
```

---

## Goal

Inventory the existing codebase to surface OpenAQ-specific assumptions and ClickHouse schema details needed for Phase 1 planning. Output is `.planning/codebase/AUDIT.md`.

---

## <task id="audit-clickhouse">

<read_first>
- `scripts/init-clickhouse.sql` — all CREATE TABLE statements
- `docker-compose.yml` lines 19–22 — CLICKHOUSE_DB env var
</read_first>

<action>
Read `scripts/init-clickhouse.sql` and document every `CREATE TABLE` block in `AUDIT.md` as a table with columns:

| Column | Source |
|--------|--------|
| Table name | `CREATE TABLE IF NOT EXISTS <name>` |
| Engine type | `ENGINE = <name>` |
| ORDER BY key (dedup key) | `ORDER BY (...)` |
| PARTITION BY key | `PARTITION BY (...)` |
| SETTINGS | `SETTINGS index_granularity = ..., allow_nullable_key = ...` |
| `ingest_time` used in engine? | Whether `ReplacingMergeTree(ingest_time)` vs `MergeTree()` |
| Phase 1 impact | What changes Phase 1 requires |

The six tables to document are:
1. `raw_openaq_measurements` — MergeTree, Python-side dedup only → CONCERN #7
2. `raw_openaq_locations` — ReplacingMergeTree(ingest_time)
3. `raw_openaq_parameters` — ReplacingMergeTree(ingest_time)
4. `raw_openaq_sensors` — ReplacingMergeTree(ingest_time)
5. `raw_aqicn_measurements` — MergeTree, append-only
6. `raw_aqicn_stations` — ReplacingMergeTree(ingest_time)
7. `raw_aqicn_forecast` — ReplacingMergeTree(ingest_time)

Also verify `CLICKHOUSE_DB` resolves to `air_quality` consistently (see task 5).
</action>

<acceptance_criteria>
- `grep -c "CREATE TABLE" scripts/init-clickhouse.sql` outputs `7`
- `.planning/codebase/AUDIT.md` contains a table with all 7 rows
- Every row has engine type, ORDER BY key, and Phase 1 impact
- `raw_openaq_measurements` row has "Python-side dedup only — CONCERN #7" noted
</acceptance_criteria>

</task>

---

## <task id="audit-dbt-openaq">

<read_first>
- `dbt/dbt_tranform/models/staging/openaq/stg_openaq__measurements.sql`
- `dbt/dbt_tranform/models/staging/openaq/stg_openaq__locations.sql`
- `dbt/dbt_tranform/models/staging/openaq/stg_openaq__sensors.sql`
- `dbt/dbt_tranform/models/staging/openaq/stg_openaq__parameters.sql`
- `dbt/dbt_tranform/models/intermediate/int_unified__measurements.sql`
- `dbt/dbt_tranform/models/intermediate/int_aqi_calculations.sql`
</read_first>

<action>
Run `grep -rn "openaq\|OpenAQ" dbt/dbt_tranform/models/` (case-insensitive). For every match, record in `AUDIT.md`:

| File | Line | Pattern | Purpose | Replaceable in Phase 1? |
|------|------|---------|---------|--------------------------|
| ... | ... | ... | ... | ... |

Distinguish between:
- `{{ source('openaq', 'raw_openaq_*') }}` — dbt source refs (must keep, rename in Phase 2)
- `'openaq'` literal strings in CTEs — hardcoded source name
- `'OPENAQ_'` prefix in `int_unified__measurements.sql` — station ID prefix

Also check `dbt/dbt_tranform/models/marts/` for any remaining OpenAQ-specific JOINs or filters.
</action>

<acceptance_criteria>
- `grep -rn "openaq\|OpenAQ" dbt/dbt_tranform/models/ | wc -l` shows total count
- All `{{ source('openaq', ...) }}` calls are listed in the audit table
- All `'openaq'` literal strings in `int_unified__measurements.sql` are listed
- `'OPENAQ_'` prefix in `int_unified__measurements.sql` line 29 is documented
</acceptance_criteria>

</task>

---

## <task id="audit-python-openaq">

<read_first>
- `python_jobs/jobs/openaq/ingest_measurements.py`
- `python_jobs/jobs/openaq/ingest_locations.py`
- `python_jobs/jobs/openaq/ingest_sensors.py`
- `python_jobs/jobs/openaq/ingest_parameters.py`
- `python_jobs/jobs/aqicn/ingest_measurements.py`
- `python_jobs/common/api_client.py`
</read_first>

<action>
Run `grep -rn "'openaq'\|openaq\." python_jobs/jobs/`. For every match, record in `AUDIT.md`:

| File | Line | Pattern | Context | Notes |
|------|------|---------|---------|-------|
| ... | ... | ... | ... | ... |

Document:
- API endpoint strings (e.g., `/v3/measurements`, `/v3/locations`)
- Table name strings in ClickHouse insert statements
- Source name strings in logging or metadata
- API token header names (`X-API-KEY`)
</action>

<acceptance_criteria>
- `grep -rn "'openaq'\|openaq\." python_jobs/jobs/ | wc -l` shows count
- All API endpoints in `ingest_measurements.py` are listed
- All table name strings are listed
- API token header names are documented
</acceptance_criteria>

</task>

---

## <task id="audit-dags">

<read_first>
- `airflow/dags/dag_ingest_hourly.py`
- `airflow/dags/dag_transform.py`
- `airflow/dags/dag_metadata_update.py`
- `airflow/dags/dag_ingest_historical.py`
</read_first>

<action>
Read each DAG file. For each DAG, document in `AUDIT.md`:

1. **Task list:** Every `@task` function name
2. **Dependency chain:** ASCII diagram using `>>` and `[task_a, task_b]` notation
3. **Env var capture:** Confirm `get_job_env_vars()` is a function (not module-level dict) — correct pattern
4. **Schedule:** `schedule=` parameter
5. **DAG ID:** The `@dag` decorator's `dag_id=` (or function name if implicit)

**dag_ingest_hourly expected:**
```
check_clickhouse_connection
        ↓
ensure_metadata
        ↓
[run_openaq_measurements_ingestion, run_aqicn_measurements_ingestion, run_aqicn_forecast_ingestion]
        ↓
log_completion
```

**dag_transform expected:**
```
check_clickhouse_connection → check_dbt_ready → dbt_deps → dbt_seed
        → dbt_run_staging → dbt_run_intermediate → dbt_run_marts
        → dbt_test → log_dbt_stats → log_completion
```

**dag_metadata_update expected:** tasks for parameters, locations, sensors (OpenAQ-only)
**dag_ingest_historical expected:** manual-only, `max_active_runs=1`, no semaphore (CONCERN #14)
</action>

<acceptance_criteria>
- All 4 DAG files are documented in AUDIT.md
- ASCII dependency diagram present for each DAG
- `dag_ingest_hourly` chain matches: `check >> metadata >> [openaq, aqicn, forecast] >> completion`
- `dag_transform` chain matches: sequential dbt_deps → dbt_seed → dbt_run_staging → dbt_run_intermediate → dbt_run_marts → dbt_test
- Env var capture confirmed as function (not module-level dict) in `dag_ingest_hourly.py`
</acceptance_criteria>

</task>

---

## <task id="audit-db-name">

<read_first>
- `.env` (line 4: `CLICKHOUSE_DB=air_quality`)
- `docker-compose.yml` (line 20: `- CLICKHOUSE_DB=${CLICKHOUSE_DB}`)
- `scripts/init-clickhouse.sql` (line 2: `CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB}`)
- `dbt/dbt_tranform/profiles.yml` (lines 13, 27: `database: "{{ env_var('CLICKHOUSE_DB', 'air_quality') }}"`)
- `airflow/dags/dag_ingest_hourly.py` (line 44: default `'airquality'`)
</read_first>

<action>
Compare all DB name references. Document in `AUDIT.md`:

| File | Line | Value | Notes |
|------|------|-------|-------|
| `.env` | 4 | `air_quality` | Production value |
| `docker-compose.yml` | 20 | `${CLICKHOUSE_DB}` → `air_quality` | Resolves to .env value |
| `init-clickhouse.sql` | 2 | `${CLICKHOUSE_DB}` → `air_quality` | Resolves to .env value |
| `profiles.yml` dev | 13 | `air_quality` | Default matches .env |
| `profiles.yml` prod | 27 | `air_quality` | Default matches .env |
| `dag_ingest_hourly.py` | 44 | `'airquality'` (no underscore) | **Fallback — fix this** |

**Decision D-10:** No rename needed. DB is consistently `air_quality` in production. The `airquality` fallback in `dag_ingest_hourly.py` is never used in practice (`.env` always provides the value) but should be fixed for consistency.

Fix: Change line 44 of `dag_ingest_hourly.py` from:
```python
'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'airquality'),
```
to:
```python
'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
```
</action>

<acceptance_criteria>
- `.env` confirmed `CLICKHOUSE_DB=air_quality`
- `docker-compose.yml` uses `${CLICKHOUSE_DB}` (resolves to `air_quality`)
- `init-clickhouse.sql` uses `${CLICKHOUSE_DB}` (resolves to `air_quality`)
- `profiles.yml` defaults to `air_quality`
- `dag_ingest_hourly.py` fallback changed from `'airquality'` to `'air_quality'`
- AUDIT.md section "DB Name Consistency" contains conclusion: "DB is consistently named `air_quality`. No rename needed."
</acceptance_criteria>

</task>

---

## <task id="audit-dedup">

<read_first>
- `scripts/init-clickhouse.sql` (lines 60–63: `raw_openaq_measurements` ENGINE = MergeTree)
- `python_jobs/jobs/openaq/ingest_measurements.py` (for Python dedup logic if any)
</read_first>

<action>
Confirm from `init-clickhouse.sql` that `raw_openaq_measurements` uses `MergeTree()` (not ReplacingMergeTree). Document in `AUDIT.md`:

```
CONCERN #7: Python-Side Dedup Only

Table: raw_openaq_measurements
Engine: MergeTree()  ← No server-side deduplication
Dedup strategy: Python checks ClickHouse before insert (CONCERN #7)
Risk: If dedup check races or fails, duplicates accumulate in MergeTree
Fix: Migrate to ReplacingMergeTree(version) in Phase 2 Plan 2.4
```

Also document `raw_aqicn_measurements` dedup status: `MergeTree()` with ORDER BY `(station_id, time_v, pollutant, ingest_time)` — append-only, no dedup.

Check `python_jobs/jobs/openaq/ingest_measurements.py` for the dedup implementation. Document whether it queries ClickHouse before insert, and whether it handles the race condition.
</action>

<acceptance_criteria>
- `raw_openaq_measurements` confirmed as `MergeTree()` (not ReplacingMergeTree) in `init-clickhouse.sql`
- AUDIT.md documents CONCERN #7 with risk description and Phase 2 fix reference
- `raw_aqicn_measurements` documented as append-only MergeTree
- Python dedup implementation described (if found in ingest_measurements.py)
</acceptance_criteria>

</task>

---

## Verification

After completing all tasks, verify:
1. `.planning/codebase/AUDIT.md` exists and is non-empty (>2000 bytes)
2. AUDIT.md contains the 7-row ClickHouse table schema table
3. AUDIT.md contains a table of all OpenAQ references in dbt models
4. AUDIT.md contains a table of all OpenAQ references in Python jobs
5. AUDIT.md contains ASCII dependency diagrams for all 4 DAGs
6. AUDIT.md contains the DB name consistency section with "no rename needed" conclusion
7. `airflow/dags/dag_ingest_hourly.py` line 44 has fallback `'air_quality'` (not `'airquality'`)

---

*Plan author: gsd:quick*
