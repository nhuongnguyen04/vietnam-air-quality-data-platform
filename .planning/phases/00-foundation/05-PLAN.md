---
wave: 4
depends_on:
  - .planning/phases/00-foundation/02-PLAN.md
files_modified:
  - scripts/init-clickhouse.sql
  - python_jobs/common/ingestion_control.py
  - airflow/dags/dag_ingest_hourly.py
  - airflow/dags/dag_transform.py
autonomous: false
---

# Plan 0.5 — Ingestion Control Table

**Plan:** 0.5
**Phase:** 00-foundation
**Wave:** 4 (can overlap with 0.4)
**Owner:** data engineering

---
```yaml
wave: 4
depends_on:
  - .planning/phases/00-foundation/02-PLAN.md
files_modified:
  - scripts/init-clickhouse.sql
  - python_jobs/common/ingestion_control.py
  - airflow/dags/dag_ingest_hourly.py
  - airflow/dags/dag_transform.py
autonomous: false
```

---

## Goal

Create `ingestion.control` ClickHouse table and wire Airflow DAG tasks to write run metadata (source, timestamps, row counts) after every ingestion run. This table is the foundation for Grafana freshness dashboards (Plan 3.4) and alerting (Plan 5.2).

---

## <task id="control-table">

<read_first>
- `scripts/init-clickhouse.sql` (full file — existing tables at lines 13–286)
</read_first>

<action>
Add the following table definition to `scripts/init-clickhouse.sql` after the existing `raw_aqicn_stations` table (after line 215, before the `CREATE USER` comment at line 287):

```sql
-- ============================================
-- Ingestion Control Table (Plan 0.5)
-- Tracks run metadata for each data source
-- Consumed by Grafana freshness dashboards (Phase 3.4) and alerting (Phase 5.2)
-- ============================================
CREATE TABLE IF NOT EXISTS ingestion_control
(
    source              LowCardinality(String),
    last_run           DateTime DEFAULT now(),
    last_success       DateTime,
    records_ingested   UInt64 DEFAULT 0,
    lag_seconds        Int64 DEFAULT 0,
    error_message      String DEFAULT '',
    updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY source
SETTINGS index_granularity = 8192;
```

Key schema decisions:
- `ingestion_control` (no DB prefix — inherits from `USE ${CLICKHOUSE_DB}`)
- `source` as ORDER BY key — one row per source
- `ReplacingMergeTree(updated_at)` — latest row per source is returned by queries
- `lag_seconds DEFAULT 0` — calculated by the Airflow task on the next run
- `error_message DEFAULT ''` — empty string on success
</action>

<acceptance_criteria>
- `grep "CREATE TABLE IF NOT EXISTS ingestion_control" scripts/init-clickhouse.sql` returns exactly 1 match
- `grep "ENGINE = ReplacingMergeTree(updated_at)" scripts/init-clickhouse.sql` returns exactly 1 match (for ingestion_control)
- `grep "ORDER BY source" scripts/init-clickhouse.sql` returns exactly 1 match (for ingestion_control)
- `grep -A 15 "CREATE TABLE IF NOT EXISTS ingestion_control" scripts/init-clickhouse.sql | grep -c "source\|last_run\|last_success\|records_ingested\|lag_seconds\|error_message\|updated_at"` returns 7
</acceptance_criteria>

</task>

---

## <task id="control-python-lib">

<read_first>
- `python_jobs/common/` directory structure (to confirm it exists)
- `python_jobs/common/config.py` (for ClickHouse client creation pattern)
</read_first>

<action>
Create `python_jobs/common/ingestion_control.py` with the following exact content:

```python
"""
ingestion_control — ClickHouse ingestion control table writer.

This module writes run metadata to the `ingestion.control` table after each
ingestion job run. Used by Airflow DAGs via PythonOperator or @task.

Author: Air Quality Data Platform
"""

import os
from datetime import datetime, timezone
from typing import Optional


def get_clickhouse_client():
    """Create a ClickHouse client using environment variables."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', '8123')),
        username=os.environ.get('CLICKHOUSE_USER', 'admin'),
        password=os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        database=os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    )


def update_control(
    source: str,
    records_ingested: int,
    success: bool,
    error_message: str = '',
    last_run: Optional[datetime] = None,
) -> None:
    """
    Write or update a row in `ingestion.control` for the given source.

    Parameters
    ----------
    source : str
        Source name, e.g. 'aqicn', 'openaq', 'airnow', 'aqicn_forecast', 'dbt_transform'.
    records_ingested : int
        Number of rows ingested in this run.
    success : bool
        Whether the ingestion run completed successfully.
    error_message : str, optional
        Exception text or error message if the run failed.
    last_run : datetime, optional
        Override for `last_run` timestamp. Defaults to UTC now.
    """
    client = get_clickhouse_client()

    now = last_run or datetime.now(timezone.utc)
    last_success = now if success else None
    lag_seconds = 0 if success else -1

    client.insert(
        'ingestion_control',
        [[
            source,
            now,
            last_success,
            records_ingested,
            lag_seconds,
            error_message,
            now,
        ]],
        column_names=[
            'source',
            'last_run',
            'last_success',
            'records_ingested',
            'lag_seconds',
            'error_message',
            'updated_at',
        ],
    )
    client.close()
```

The module uses `clickhouse_connect` (already in `requirements.txt`).
</action>

<acceptance_criteria>
- `python_jobs/common/ingestion_control.py` exists
- `python_jobs/common/ingestion_control.py` contains `def update_control(`
- `python_jobs/common/ingestion_control.py` contains `def get_clickhouse_client()`
- `python_jobs/common/ingestion_control.py` calls `client.insert('ingestion_control', ...)`
- `python_jobs/common/ingestion_control.py` imports `datetime` and `timezone` from `datetime`
- `grep -c "import clickhouse_connect" python_jobs/common/ingestion_control.py` returns 1
</acceptance_criteria>

</task>

---

## <task id="control-dag-hourly">

<read_first>
- `airflow/dags/dag_ingest_hourly.py` (lines 154–235: task definitions and wiring)
- `python_jobs/common/ingestion_control.py` (created above)
</read_first>

<action>
Add two `@task` functions to `dag_ingest_hourly.py` after `run_aqicn_forecast_ingestion` (before `log_completion`). Also add the necessary `sys.path` import inside each task.

**Add task definitions** after `run_aqicn_forecast_ingestion` function, before `log_completion`:

```python
    @task
    def update_aqicn_control():
        """Update ingestion.control for AQICN measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='aqicn', records_ingested=0, success=True)
        print("Updated ingestion.control for aqicn")

    @task
    def update_forecast_control():
        """Update ingestion.control for AQICN forecast."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='aqicn_forecast', records_ingested=0, success=True)
        print("Updated ingestion.control for aqicn_forecast")
```

**Add task instantiation** after the existing task instantiations (after `forecast = run_aqicn_forecast_ingestion()`):

```python
    update_aqicn_control = update_aqicn_control()
    update_forecast_control = update_forecast_control()
```

**Update task wiring** (current line 235):

From:
```python
    check_clickhouse >> metadata >> [openaq, aqicn, forecast] >> completion
```

To:
```python
    check_clickhouse >> metadata >> [aqicn, forecast]
    [aqicn, forecast] >> update_aqicn_control >> update_forecast_control >> completion
```

Note: This also removes `openaq` from the wiring — `openaq` was disabled in Plan 0.4.
</action>

<acceptance_criteria>
- `grep "def update_aqicn_control" airflow/dags/dag_ingest_hourly.py` returns the task function definition
- `grep "def update_forecast_control" airflow/dags/dag_ingest_hourly.py` returns the task function definition
- `grep "from common.ingestion_control import update_control" airflow/dags/dag_ingest_hourly.py` returns 2 matches (one per task)
- Task dependency line contains `update_aqicn_control >> update_forecast_control >> completion`
- Task dependency line contains `[aqicn, forecast] >>` (OpenAQ removed per Plan 0.4)
- `grep -c "update_aqicn_control\|update_forecast_control" airflow/dags/dag_ingest_hourly.py` returns 4 (2 definitions + 2 instantiations)
</acceptance_criteria>

</task>

---

## <task id="control-dag-transform">

<read_first>
- `airflow/dags/dag_transform.py` (lines 309–326: task definitions and wiring)
- `python_jobs/common/ingestion_control.py` (created above)
</read_first>

<action>
Add a `@task` function to `dag_transform.py` after `log_completion` (before `dag_transform = dag_transform()`). Also add task instantiation and wiring.

**Add task definition** after `log_completion` function, before `dag_transform = dag_transform()`:

```python
    @task
    def update_transform_control():
        """Update ingestion.control for dbt transformation run."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='dbt_transform', records_ingested=0, success=True)
        print("Updated ingestion.control for dbt_transform")
```

**Add task instantiation** after `completion = log_completion()`:

```python
    update_transform_control = update_transform_control()
```

**Update task wiring** (current line 326):

From:
```python
    check_clickhouse >> check_dbt >> deps >> seed >> staging >> intermediate >> marts >> test >> stats >> completion
```

To:
```python
    check_clickhouse >> check_dbt >> deps >> seed >> staging >> intermediate >> marts >> test >> stats >> update_transform_control >> completion
```
</action>

<acceptance_criteria>
- `grep "def update_transform_control" airflow/dags/dag_transform.py` returns the task function definition
- `grep "from common.ingestion_control import update_control" airflow/dags/dag_transform.py` returns 1 match
- Task wiring ends with `update_transform_control >> completion`
- `grep "update_transform_control" airflow/dags/dag_transform.py | wc -l` returns 2 (1 definition + 1 instantiation/wiring)
</acceptance_criteria>

</task>

---

## <task id="control-verify">

<read_first>
- `scripts/init-clickhouse.sql` (confirmed table definition added)
- `python_jobs/common/ingestion_control.py` (confirmed function created)
- `airflow/dags/dag_ingest_hourly.py` (confirmed tasks wired)
- `airflow/dags/dag_transform.py` (confirmed task wired)
</read_first>

<action>
After restarting Airflow to pick up the DAG changes, verify the control table is populated:

```bash
# Restart services
docker compose restart airflow-scheduler airflow-dag-processor
sleep 60

# Trigger a manual DAG run (optional, wait for scheduled run)
docker compose exec airflow-scheduler airflow dags trigger dag_ingest_hourly || true

# Wait for next scheduled run (or 2 minutes)
sleep 120

# Query the control table
docker compose exec clickhouse clickhouse-client \
  --query "SELECT source, last_run, last_success, records_ingested, lag_seconds, error_message FROM air_quality.ingestion_control ORDER BY source FORMAT PrettyCompact"
```

**Expected sources:** `aqicn`, `aqicn_forecast`, `dbt_transform` (after first runs).

Also verify the `ReplacingMergeTree` behavior:
```sql
-- Insert a duplicate (same source) and verify only the latest survives
INSERT INTO air_quality.ingestion_control (source, last_run, last_success, records_ingested, lag_seconds, error_message, updated_at)
VALUES ('test_source', now(), now(), 999, 0, '', now());

SELECT source, count(*) as cnt FROM air_quality.ingestion_control GROUP BY source;
-- Should show 1 for test_source (deduped by ReplacingMergeTree)
```
</action>

<acceptance_criteria>
- `SELECT count(*) FROM air_quality.ingestion_control` returns ≥ 3 after first DAG runs
- `SELECT source FROM air_quality.ingestion_control` shows `aqicn` row
- `SELECT source FROM air_quality.ingestion_control` shows `aqicn_forecast` row
- `SELECT source FROM air_quality.ingestion_control` shows `dbt_transform` row
- `SELECT last_success FROM air_quality.ingestion_control WHERE source = 'aqicn'` returns non-NULL after successful run
- `SELECT error_message FROM air_quality.ingestion_control WHERE source = 'aqicn'` returns empty string
- ReplacingMergeTree dedup: inserting duplicate source rows shows only latest in query results
</acceptance_criteria>

</task>

---

## Verification

1. `scripts/init-clickhouse.sql` contains `ingestion_control` table with `ReplacingMergeTree(updated_at)`
2. `python_jobs/common/ingestion_control.py` exists with `update_control()` function
3. `dag_ingest_hourly.py` has `update_aqicn_control` and `update_forecast_control` tasks in dependency chain
4. `dag_transform.py` has `update_transform_control` task in dependency chain
5. After first DAG run: `SELECT * FROM air_quality.ingestion_control` returns ≥ 3 rows
6. `SELECT source FROM air_quality.ingestion_control` shows `aqicn`, `aqicn_forecast`, `dbt_transform`
7. Grafana (Phase 3 Plan 3.4) can query `ingestion.control` for freshness panels

---

## must_haves

1. `ingestion.control` table exists with schema matching ROADMAP definition (7 columns)
2. `dag_ingest_hourly` writes to `ingestion.control` after every run
3. `dag_transform` writes to `ingestion.control` after every run
4. `ingestion.control` populated within 5 minutes of DAG completion
5. Grafana (Phase 3 Plan 3.4) can query `ingestion.control` for freshness panels

---

*Plan author: gsd:quick*
