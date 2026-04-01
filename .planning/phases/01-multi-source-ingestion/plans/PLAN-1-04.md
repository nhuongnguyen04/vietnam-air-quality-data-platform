---
gsd_plan_version: 1.0
phase: 01
plan: 1.04
slug: openaq-decommission
status: draft
wave: 2
depends_on:
  - PLAN-1-01
  - PLAN-1-02
  - PLAN-1-03
autonomous: true
files_modified:
  - scripts/init-clickhouse.sql
  - airflow/dags/dag_ingest_hourly.py
  - airflow/dags/dag_metadata_update.py
  - .env
  - docker-compose.yml
  - tests/test_decommission.py
requirements_addressed:
  - decommission
created: 2026-04-01
must_haves:
  - python_jobs/jobs/openaq/ directory removed
  - dag_ingest_hourly.py contains zero OpenAQ task references (commented or active)
  - dag_metadata_update.py contains zero OpenAQ task references
  - OpenAQ ClickHouse tables renamed to raw_openaq_*_archived (not dropped)
  - OPENAQ_API_TOKEN removed from .env and docker-compose.yml
---

# Plan 1.04 — OpenAQ Decommission

**Phase:** 01-multi-source-ingestion
**Wave:** 2 (depends on all Wave 1 plans complete)
**Autonomous:** true

---

## Context

OpenAQ is being decommissioned from Phase 1. The data source is replaced by OpenWeather, WAQI, and Sensors.Community. This plan removes all OpenAQ code, renames OpenAQ tables to `raw_openaq_*_archived` for rollback safety (not dropped), and removes the OpenAQ token from environment configuration.

Key constraints:
- **D-07**: OpenAQ tables renamed to `raw_openaq_*_archived` — NOT dropped (rollback safety)
- **D-32**: OpenAQ tasks removed from `dag_ingest_hourly`
- **D-33**: `python_jobs/jobs/openaq/` directory removed
- **D-34**: `raw_openaq_measurements`, `raw_openaq_locations`, `raw_openaq_parameters`, `raw_openaq_sensors` renamed to archived
- **D-35**: `dag_metadata_update` updated to remove OpenAQ metadata ingestion
- OpenAQ token removed from `.env` and docker-compose

---

## Tasks

### 1-04-A: Rename OpenAQ ClickHouse tables to `raw_openaq_*_archived` via `scripts/init-clickhouse.sql`

<read_first>
- `scripts/init-clickhouse.sql` — existing OpenAQ table definitions: `raw_openaq_measurements`, `raw_openaq_locations`, `raw_openaq_parameters`, `raw_openaq_sensors`
- `01-CONTEXT.md` — D-07, D-34 (archived table naming convention)
</read_first>

<action>
In `scripts/init-clickhouse.sql`, add ALTER TABLE statements after the existing table definitions (before ingestion_control). These rename tables without dropping data:

```sql
-- ============================================
-- OpenAQ Decommission (Plan 1.04)
-- D-07: Rename OpenAQ tables to raw_openaq_*_archived (NOT DROP)
-- D-34: Retain data for rollback safety
-- ============================================

-- Rename OpenAQ measurements table
RENAME TABLE raw_openaq_measurements TO raw_openaq_measurements_archived;

-- Rename OpenAQ locations table
RENAME TABLE raw_openaq_locations TO raw_openaq_locations_archived;

-- Rename OpenAQ parameters table
RENAME TABLE raw_openaq_parameters TO raw_openaq_parameters_archived;

-- Rename OpenAQ sensors table
RENAME TABLE raw_openaq_sensors TO raw_openaq_sensors_archived;
```

Also update the existing table definitions in `init-clickhouse.sql` (the CREATE TABLE blocks for OpenAQ tables) by adding a leading `-- DISABLED (Plan 1.04): `RENAME TABLE ... TO raw_openaq_*_archived`` comment above each OpenAQ CREATE TABLE block so future maintainers understand why the tables are absent.

Alternatively, add a SQL comment block before the ALTER statements:
```sql
-- IMPORTANT: The following CREATE TABLE blocks for raw_openaq_* tables are DISABLED
-- in Plan 1.04. Tables have been renamed to raw_openaq_*_archived via ALTER.
-- To restore: RENAME TABLE raw_openaq_*_archived TO raw_openaq_*
```
</action>

<acceptance_criteria>
- `grep -n 'raw_openaq_measurements_archived' scripts/init-clickhouse.sql` finds the renamed table reference
- `grep -n 'raw_openaq_locations_archived' scripts/init-clickhouse.sql` finds the renamed table reference
- `grep -n 'raw_openaq_parameters_archived' scripts/init-clickhouse.sql` finds the renamed table reference
- `grep -n 'raw_openaq_sensors_archived' scripts/init-clickhouse.sql` finds the renamed table reference
- `grep -n 'RENAME TABLE' scripts/init-clickhouse.sql` finds 4 ALTER/RENAME statements
- `grep -n 'raw_openaq_measurements' scripts/init-clickhouse.sql` returns only lines with `_archived` suffix
</acceptance_criteria>

---

### 1-04-B: Remove `python_jobs/jobs/openaq/` directory

<read_first>
- `python_jobs/jobs/openaq/` — existing directory with files: `__init__.py`, `ingest_measurements.py`, `ingest_parameters.py`, `ingest_locations.py`, `ingest_sensors.py`
- `CLAUDE.md` — Python package structure conventions
</read_first>

<action>
Delete the entire `python_jobs/jobs/openaq/` directory using `rm -rf`. Verify all 5 files are removed:

```bash
rm -rf python_jobs/jobs/openaq/
```

Verify removal:
- `ls python_jobs/jobs/openaq/` → directory does not exist
- `grep -r 'jobs/openaq' python_jobs/` → no results
</action>

<acceptance_criteria>
- `ls python_jobs/jobs/openaq/` returns "No such file or directory"
- `grep -r 'openaq' python_jobs/jobs/` returns no results (openaq directory fully removed)
- `grep -r 'jobs/openaq' python_jobs/` returns no results
</acceptance_criteria>

---

### 1-04-C: Update `airflow/dags/dag_ingest_hourly.py` — remove all OpenAQ task references

<read_first>
- `airflow/dags/dag_ingest_hourly.py` — current state with commented-out OpenAQ tasks
- `01-CONTEXT.md` — D-32 (remove OpenAQ tasks from dag_ingest_hourly)
</read_first>

<action>
In `airflow/dags/dag_ingest_hourly.py`, make the following changes:

1. Remove `OPENAQ_API_TOKEN` from `get_job_env_vars()` entirely (do not keep it with empty string fallback).

2. Remove the commented-out `run_openaq_measurements_ingestion()` function block (lines with `# @task` and the disabled function).

3. Remove any OpenAQ-related commands from `ensure_metadata()`:
   - Remove the three OpenAQ metadata commands: `ingest_parameters.py`, `ingest_locations.py`, `ingest_sensors.py`
   - Update the `SELECT count(*) FROM raw_openaq_locations` check to simply return (since OpenAQ metadata is gone) or remove it entirely
   - Simplify `ensure_metadata()` so it no longer references `raw_openaq_locations`

4. Update `log_completion()` or any other function that may reference OpenAQ.

5. Remove the `check_clickhouse >> metadata >> [aqicn, forecast]` pattern comment and replace with the updated pattern.

6. Verify that no remaining text in the file contains `openaq`, `ingest_openaq`, `raw_openaq`, `OPENAQ_API_TOKEN`, `jobs/openaq`, or `ingest_parameters.py` / `ingest_locations.py` / `ingest_sensors.py`.

After changes, the `get_job_env_vars()` should contain only:
```python
return {
    'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
    'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
    'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
    'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
    'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
    # Note: OPENWEATHER_API_TOKEN and WAQI_API_TOKEN added in PLAN-1-01/PLAN-1-02
}
```

After changes, `ensure_metadata()` should no longer call OpenAQ scripts and should remove the check on `raw_openaq_locations`.

The file should retain AQICN, OpenWeather, WAQI, and Sensors.Community tasks (added in Wave 1).
</action>

<acceptance_criteria>
- `grep -in 'openaq' airflow/dags/dag_ingest_hourly.py` returns zero results (no OpenAQ references anywhere)
- `grep -in 'ingest_parameters.py' airflow/dags/dag_ingest_hourly.py` returns zero results
- `grep -in 'ingest_locations.py' airflow/dags/dag_ingest_hourly.py` returns zero results
- `grep -in 'ingest_sensors.py' airflow/dags/dag_ingest_hourly.py` returns zero results
- `grep -in 'OPENAQ_API_TOKEN' airflow/dags/dag_ingest_hourly.py` returns zero results
- `grep -in 'raw_openaq' airflow/dags/dag_ingest_hourly.py` returns zero results
- File parses as valid Python (no syntax errors)
</acceptance_criteria>

---

### 1-04-D: Update `airflow/dags/dag_metadata_update.py` — remove OpenAQ metadata ingestion

<read_first>
- `airflow/dags/dag_metadata_update.py` — current state with `refresh_openaq_parameters()`, `refresh_openaq_locations()`, `refresh_openaq_sensors()` tasks
- `01-CONTEXT.md` — D-35 (dag_metadata_update removes OpenAQ metadata refs)
</read_first>

<action>
In `airflow/dags/dag_metadata_update.py`, make the following changes:

1. Remove `OPENAQ_API_TOKEN` from `get_job_env_vars()`.

2. Remove the entire `refresh_openaq_parameters()` task function definition.

3. Remove the entire `refresh_openaq_locations()` task function definition.

4. Remove the entire `refresh_openaq_sensors()` task function definition.

5. Remove the OpenAQ chain dependency line:
   ```python
   # check_clickhouse >> refresh_params >> refresh_locations >> refresh_sensors  ← REMOVE
   ```

6. Update the stats query in `log_metadata_stats()` — remove the three OpenAQ count queries (`raw_openaq_parameters`, `raw_openaq_locations`, `raw_openaq_sensors`). Keep only the AQICN stations count.

7. Update task assignment and dependency lines to remove all OpenAQ variable references.

After changes, the DAG should have only:
- `refresh_aqicn_stations()` task (plus any new source metadata tasks from Wave 1)
- `log_metadata_stats()` with only AQICN (and Wave 1 source) stats
- Simplified dependency chain without the OpenAQ chain

8. Verify no remaining text contains `openaq`, `refresh_openaq`, `raw_openaq`, or `OPENAQ_API_TOKEN`.
</action>

<acceptance_criteria>
- `grep -in 'openaq' airflow/dags/dag_metadata_update.py` returns zero results
- `grep -in 'refresh_openaq' airflow/dags/dag_metadata_update.py` returns zero results
- `grep -in 'raw_openaq' airflow/dags/dag_metadata_update.py` returns zero results
- `grep -in 'OPENAQ_API_TOKEN' airflow/dags/dag_metadata_update.py` returns zero results
- `grep -n 'refresh_aqicn_stations' airflow/dags/dag_metadata_update.py` still returns the remaining AQICN station task
- File parses as valid Python (no syntax errors)
</acceptance_criteria>

---

### 1-04-E: Remove `OPENAQ_API_TOKEN` from `.env` and `docker-compose.yml`

<read_first>
- `.env` — current contents with `OPENAQ_API_TOKEN=f939555d...`
- `docker-compose.yml` — Airflow service env blocks containing `OPENAQ_API_TOKEN`
</read_first>

<action>
In `.env`:
- Remove the line `OPENAQ_API_TOKEN=f939555d8d416ec60bdd233ac7526dd3f8c6304c017d9d03c9b25c37d9a122c0`

In `docker-compose.yml`:
- Remove `OPENAQ_API_TOKEN=${OPENAQ_API_TOKEN}` from all Airflow service environment blocks (webserver, scheduler, dag-processor, triggerer)
- Verify the remaining tokens (`AQICN_API_TOKEN`, `OPENWEATHER_API_TOKEN`, `WAQI_API_TOKEN`) are still present
</action>

<acceptance_criteria>
- `grep -n 'OPENAQ_API_TOKEN' .env` returns zero results (OPENAQ token removed from .env)
- `grep -n 'OPENAQ_API_TOKEN' docker-compose.yml` returns zero results (OPENAQ token removed from docker-compose)
- `grep -n 'AQICN_API_TOKEN' .env` returns ≥1 line (AQICN token still present)
- `grep -c 'OPENWEATHER_API_TOKEN' docker-compose.yml` returns ≥0 (may have been added in PLAN-1-01)
- `grep -c 'WAQI_API_TOKEN' docker-compose.yml` returns ≥0 (may have been added in PLAN-1-02)
</acceptance_criteria>

---

### 1-04-F: Update `tests/test_decommission.py` — replace `assert True` with real assertions

<read_first>
- `tests/test_decommission.py` — existing stubs with `assert True` placeholders
- `python_jobs/jobs/openaq/` — should be deleted by 1-04-B
- `airflow/dags/dag_ingest_hourly.py` — updated by 1-04-C
- `airflow/dags/dag_metadata_update.py` — updated by 1-04-D
</read_first>

<action>
Replace the `assert True` placeholder assertions in `tests/test_decommission.py` with real assertions that verify the decommission is complete:

```python
def test_openaq_directory_removal():
    """python_jobs/jobs/openaq/ directory no longer exists after decommission."""
    openaq_dir = os.path.join(
        os.path.dirname(__file__), '..',
        'python_jobs', 'jobs', 'openaq'
    )
    assert not os.path.exists(openaq_dir), \
        f"OpenAQ directory still exists at {openaq_dir}"


def test_dag_ingest_hourly_no_openaq_tasks():
    """dag_ingest_hourly.py contains no OpenAQ task references."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read().lower()

    assert "openaq" not in content, "OpenAQ references still in dag_ingest_hourly"
    assert "ingest_openaq" not in content
    assert "openaq_api_token" not in content
    assert "raw_openaq" not in content


def test_openaq_tables_renamed_to_archived():
    """OpenAQ tables are renamed to raw_openaq_*_archived, not dropped."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from python_jobs.common.clickhouse_writer import create_clickhouse_writer

    writer = create_clickhouse_writer()
    result = writer.query("SHOW TABLES LIKE 'raw_openaq_%'")
    table_names = [str(row[0]) for row in result.result_rows]

    # All raw_openaq_* tables must be renamed to _archived
    legacy_tables = [t for t in table_names if "_archived" not in t and "openaq" in t.lower()]
    assert len(legacy_tables) == 0, \
        f"Non-archived OpenAQ tables still exist: {legacy_tables}"

    # Verify archived tables exist (at least some)
    archived_tables = [t for t in table_names if "_archived" in t]
    assert len(archived_tables) >= 4, \
        f"Expected ≥4 archived tables, found {len(archived_tables)}: {archived_tables}"


def test_dag_metadata_update_no_openaq():
    """dag_metadata_update.py contains no OpenAQ task references."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_metadata_update.py'
    )
    with open(dag_path) as f:
        content = f.read().lower()

    assert "openaq" not in content, "OpenAQ references still in dag_metadata_update"
    assert "refresh_openaq" not in content
    assert "raw_openaq" not in content


def test_ensure_metadata_no_longer_calls_openaq():
    """ensure_metadata task in dag_ingest_hourly no longer calls OpenAQ metadata scripts."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read()

    assert "ingest_parameters.py" not in content, \
        "ingest_parameters.py still referenced in dag_ingest_hourly"
    assert "ingest_locations.py" not in content, \
        "ingest_locations.py still referenced in dag_ingest_hourly"
    assert "ingest_sensors.py" not in content, \
        "ingest_sensors.py still referenced in dag_ingest_hourly"


def test_openaq_token_removed_from_env():
    """OPENAQ_API_TOKEN no longer present in .env or docker-compose.yml."""
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    compose_path = os.path.join(os.path.dirname(__file__), '..', 'docker-compose.yml')

    with open(env_path) as f:
        env_content = f.read()

    assert "OPENAQ_API_TOKEN" not in env_content, \
        "OPENAQ_API_TOKEN still in .env"

    with open(compose_path) as f:
        compose_content = f.read()

    assert "OPENAQ_API_TOKEN" not in compose_content, \
        "OPENAQ_API_TOKEN still in docker-compose.yml"
```

Replace the existing `assert True` stubs with these real assertions. Remove the `# PLACEHOLDER` and `assert True` lines entirely.
</action>

<acceptance_criteria>
- `grep -n 'assert True' tests/test_decommission.py` returns zero results (no placeholder assertions)
- `grep -n 'os.path.exists.*openaq' tests/test_decommission.py` returns the directory check assertion
- `grep -n 'openaq' tests/test_decommission.py` returns only within assertions (not as placeholder comments)
- `pytest tests/test_decommission.py --collect-only` produces zero errors
</acceptance_criteria>

---

## Summary

| Task | File | Action |
|------|------|--------|
| 1-04-A | `scripts/init-clickhouse.sql` | 4× `RENAME TABLE raw_openaq_* TO raw_openaq_*_archived` |
| 1-04-B | `python_jobs/jobs/openaq/` | Entire directory deleted |
| 1-04-C | `airflow/dags/dag_ingest_hourly.py` | Remove OpenAQ tasks, metadata calls, OPENAQ_API_TOKEN |
| 1-04-D | `airflow/dags/dag_metadata_update.py` | Remove 3 OpenAQ metadata tasks, stats queries, token |
| 1-04-E | `.env`, `docker-compose.yml` | Remove `OPENAQ_API_TOKEN` from both |
| 1-04-F | `tests/test_decommission.py` | Replace `assert True` stubs with real assertions |

---

*Generated: 2026-04-01*
