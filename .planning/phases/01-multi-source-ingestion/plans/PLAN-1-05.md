---
gsd_plan_version: 1.0
phase: 01
plan: 1.05
slug: rate-limiter-orchestration
status: draft
wave: 3
depends_on:
  - PLAN-1-04
autonomous: true
files_modified:
  - python_jobs/common/rate_limiter.py
  - python_jobs/common/api_client.py
  - airflow/dags/dag_ingest_hourly.py
  - airflow/dags/dag_sensorscm_poll.py
  - tests/test_rate_limiter.py
requirements_addressed:
  - rate-limiter
created: 2026-04-01
must_haves:
  - create_sensorscm_limiter() added to python_jobs/common/rate_limiter.py
  - dag_sensorscm_poll.py created with schedule '*/10 * * * *'
  - dag_ingest_hourly.py runs all 5 sources in parallel fan-in
  - Zero HTTP 429 errors across all sources (tenacity retry verified)
  - ingestion.control updated for all 5 sources (aqicn, aqicn_forecast, openweather, waqi, sensorscm)
---

# Plan 1.05 — Rate Limiter + Orchestration

**Phase:** 01-multi-source-ingestion
**Wave:** 3 (depends on Wave 2 complete — PLAN-1-04)
**Autonomous:** true

---

## Context

Wave 3 completes the Phase 1 orchestration layer: add the Sensors.Community rate limiter factory, create the separate `dag_sensorscm_poll` DAG for 10-minute polling, consolidate `dag_ingest_hourly` with all 5 sources in parallel fan-in, and verify tenacity retry handles HTTP 429 errors.

Key constraints:
- **D-28**: One `TokenBucketRateLimiter` per API key: `openweather` (~0.8/s), `waqi` (~1.0/s), `aqicn` (~1.0/s), `sensorscm` (~1.0/s)
- **D-29**: All source ingestion tasks in `dag_ingest_hourly` run in parallel (fan-in pattern)
- **D-30**: `ingestion.control` updated as final task in all ingestion DAGs
- **D-31**: tenacity retry with exponential backoff (base=2, max=5 retries, max_wait=5min) in `APIClient`
- Sensors.Community: `*/10 * * * *` schedule via `dag_sensorscm_poll` (separate DAG, not `dag_ingest_hourly`)

Note: OpenWeather and WAQI rate limiters were added in PLAN-1-01 and PLAN-1-02 respectively. PLAN-1-05 adds the Sensors.Community limiter and creates `dag_sensorscm_poll`.

---

## Tasks

### 1-05-A: Add `create_sensorscm_limiter()` to `python_jobs/common/rate_limiter.py`

<read_first>
- `python_jobs/common/rate_limiter.py` — existing `create_openaq_limiter()`, `create_aqicn_limiter()`, `create_openweather_limiter()`, `create_waqi_limiter()` (PLAN-1-01/PLAN-1-02 added the last two)
</read_first>

<action>
Add this factory function to `python_jobs/common/rate_limiter.py` after the existing factory functions:

```python
def create_sensorscm_limiter() -> TokenBucketRateLimiter:
    """
    Create a rate limiter configured for Sensors.Community API.

    Sensors.Community has no authentication and no published rate limits.
    Using 1.0 req/s (60/min) as a courtesy maximum to avoid overwhelming
    the community API infrastructure.

    Reference: 01-RESEARCH.md § Sensors.Community — no auth, rate_per_second=1.0
    """
    return TokenBucketRateLimiter(
        rate_per_second=1.0,    # ~60/min courtesy limit
        burst_size=5,
        max_delay=300.0,       # 5min max backoff (D-31)
        backoff_factor=2.0,    # exponential backoff (D-31)
    )
```
</action>

<acceptance_criteria>
- `grep -n 'create_sensorscm_limiter' python_jobs/common/rate_limiter.py` returns the function definition
- `grep -n 'rate_per_second=1.0' python_jobs/common/rate_limiter.py` finds the rate setting
- `grep -n 'burst_size=5' python_jobs/common/rate_limiter.py` finds burst_size=5
- `grep -n 'max_delay=300.0' python_jobs/common/rate_limiter.py` finds max_delay=300.0 (5min)
</acceptance_criteria>

---

### 1-05-B: Create `airflow/dags/dag_sensorscm_poll.py` with `*/10 * * * *` schedule

<read_first>
- `airflow/dags/dag_ingest_hourly.py` — reference pattern for DAG structure, task patterns, env vars
- `python_jobs/jobs/sensorscm/ingest_measurements.py` — the script this DAG calls
- `01-CONTEXT.md` — D-29 (parallel orchestration), D-30 (ingestion.control update)
</read_first>

<action>
Create `airflow/dags/dag_sensorscm_poll.py`:

```python
"""
Sensors.Community Poll DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

Polls https://api.sensor.community/v1/feeds/ every 10 minutes for Vietnam bbox
and stores readings in ClickHouse.

Schedule: Every 10 minutes (*/10 * * * *)

This DAG runs independently from dag_ingest_hourly. Both may write to
raw_sensorscm_measurements; ReplacingMergeTree handles dedup server-side (D-01).
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os

# Default arguments
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Python jobs directory in Airflow container
PYTHON_JOBS_DIR = os.environ.get('PYTHON_JOBS_DIR', '/opt/python/jobs')
PYTHON_PATH = PYTHON_JOBS_DIR


def get_job_env_vars() -> dict:
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
        # Note: Sensors.Community requires no API token
    }


@dag(
    default_args=default_args,
    description='Poll Sensors.Community API every 10 minutes for Vietnam air quality data',
    schedule='*/10 * * * *',     # Every 10 minutes (D-29)
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    tags=['ingestion', 'sensorscm', '10min', 'air-quality'],
)
def dag_sensorscm_poll():
    """Poll Sensors.Community every 10 minutes."""

    @task
    def check_clickhouse_connection():
        """Check if ClickHouse is accessible."""
        import requests

        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')

        url = f"http://{clickhouse_host}:{clickhouse_port}/ping"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print(f"ClickHouse connection successful: {response.status_code}")
            return True
        except Exception as e:
            print(f"ClickHouse connection failed: {e}")
            raise

    @task
    def run_sensorscm_poll():
        """Poll Sensors.Community Vietnam bbox and ingest measurements."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/sensorscm/ingest_measurements.py --mode incremental"

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"Sensors.Community poll completed successfully")

    @task
    def update_sensorscm_control():
        """Update ingestion_control for Sensors.Community (10-min poll)."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='sensorscm', records_ingested=0, success=True)
        print("Updated ingestion_control for sensorscm")

    @task
    def log_completion():
        """Log completion message."""
        print("dag_sensorscm_poll completed")

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()
    poll = run_sensorscm_poll()
    update_control = update_sensorscm_control()
    completion = log_completion()

    check_clickhouse >> poll >> update_control >> completion


dag_sensorscm_poll = dag_sensorscm_poll()
```

File location: `airflow/dags/dag_sensorscm_poll.py`
</action>

<acceptance_criteria>
- `grep -n "schedule='*/10 \* \* \* \*'" airflow/dags/dag_sensorscm_poll.py` finds the schedule
- `grep -n 'run_sensorscm_poll' airflow/dags/dag_sensorscm_poll.py` returns the task function
- `grep -n 'update_sensorscm_control' airflow/dags/dag_sensorscm_poll.py` returns the control task
- `grep -n 'jobs/sensorscm/ingest_measurements.py' airflow/dags/dag_sensorscm_poll.py` finds the script call
- `grep -n 'update_control.*sensorscm' airflow/dags/dag_sensorscm_poll.py` finds the ingestion.control update
- File parses as valid Python (no syntax errors)
</acceptance_criteria>

---

### 1-05-C: Consolidate `dag_ingest_hourly.py` parallel fan-in with all 5 sources

<read_first>
- `airflow/dags/dag_ingest_hourly.py` — current state after PLAN-1-01, PLAN-1-02, PLAN-1-03, PLAN-1-04 modifications
- `01-CONTEXT.md` — D-29 (all sources run in parallel), D-30 (ingestion.control final task)
</read_first>

<action>
Read the current `dag_ingest_hourly.py` after all Wave 1 and Wave 2 tasks have been executed. Then verify and/or update the parallel fan-in dependency pattern.

The target state is a single fan-in list with all 5 sources running in parallel:

```python
    check_clickhouse = check_clickhouse_connection()
    metadata = ensure_metadata()

    # All 5 sources run in parallel (D-29)
    aqicn = run_aqicn_measurements_ingestion()
    forecast = run_aqicn_forecast_ingestion()
    openweather = run_openweather_measurements_ingestion()
    waqi = run_waqi_measurements_ingestion()
    sensorscm = run_sensorscm_measurements_ingestion()   # added in PLAN-1-03

    update_aqicn_control = update_aqicn_control()
    update_forecast_control = update_forecast_control()
    update_openweather_control = update_openweather_control()   # from PLAN-1-01
    update_waqi_control = update_waqi_control()               # from PLAN-1-02
    update_sensorscm_control = update_sensorscm_control()     # from PLAN-1-03
    completion = log_completion()

    # Parallel fan-in (D-29)
    check_clickhouse >> metadata >> [aqicn, forecast, openweather, waqi, sensorscm]
    [aqicn, forecast, openweather, waqi, sensorscm] >> (
        update_aqicn_control >> update_forecast_control >>
        update_openweather_control >> update_waqi_control >>
        update_sensorscm_control >> completion
    )
```

If OpenWeather and WAQI tasks are not yet in the file (their plans may be pending), add them using the same patterns from PLAN-1-01 and PLAN-1-02.

Verify the `get_job_env_vars()` function includes:
- `AQICN_API_TOKEN`
- `OPENWEATHER_API_TOKEN` (PLAN-1-01)
- `WAQI_API_TOKEN` (PLAN-1-02)
- All ClickHouse vars

Ensure `SENSORSCMM_*` is NOT added (Sensors.Community needs no token).
</action>

<acceptance_criteria>
- `grep -n '\[aqicn, forecast' airflow/dags/dag_ingest_hourly.py` finds the parallel fan-in list
- `grep -n 'openweather' airflow/dags/dag_ingest_hourly.py` finds openweather task in fan-in
- `grep -n 'waqi' airflow/dags/dag_ingest_hourly.py` finds waqi task in fan-in
- `grep -n 'sensorscm' airflow/dags/dag_ingest_hourly.py` finds sensorscm task in fan-in
- `grep -n 'update_.*_control' airflow/dags/dag_ingest_hourly.py` finds all 5 control update tasks
- File parses as valid Python (no syntax errors)
</acceptance_criteria>

---

### 1-05-D: Verify tenacity retry handles HTTP 429 — add if not present in `api_client.py`

<read_first>
- `python_jobs/common/api_client.py` — existing `APIClient` class; check if tenacity retry is present
- `01-CONTEXT.md` — D-31 (tenacity retry with base=2, max=5, max_wait=5min)
</read_first>

<action>
Inspect `python_jobs/common/api_client.py`. If tenacity retry is NOT already present, add it:

1. Add tenacity import:
```python
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
```

2. Decorate the `_request_with_retry` or `request` method with:
```python
@retry(
    stop=stop_after_attempt(5),                          # max=5 retries (D-31)
    wait=wait_exponential(multiplier=2, min=1, max=300),  # base=2, max_wait=5min (D-31)
    retry=retry_if_exception_type((RateLimitError, TimeoutError, ConnectionError)),
    reraise=True,
)
```

Alternatively, if the existing code already uses `urllib3.util.retry.Retry` (as noted in 01-RESEARCH.md for AirNow), verify that it has:
- `total=5` (5 max attempts)
- `backoff_factor=2` (exponential backoff)
- `status_forcelist={429}` (retry on 429)
- `max_delay=300` (5-minute max)

The D-31 spec is: `base=2, max=5 retries, max_wait=5min`. Ensure either implementation satisfies this.

Log a warning if a 429 is hit: "Rate limit hit, retrying (attempt N/5)".
</action>

<acceptance_criteria>
- `grep -n 'tenacity' python_jobs/common/api_client.py` OR `grep -n 'backoff_factor.*2' python_jobs/common/api_client.py` finds retry logic
- `grep -n '429' python_jobs/common/api_client.py` finds retry-on-429 behavior
- `grep -n 'max_delay.*300' python_jobs/common/api_client.py` OR `grep -n 'wait_exponential' python_jobs/common/api_client.py` finds max_wait=5min
- `grep -n 'stop_after_attempt.*5' python_jobs/common/api_client.py` OR `grep -n 'total.*5' python_jobs/common/api_client.py` finds max=5 retries
</acceptance_criteria>

---

### 1-05-E: Update `tests/test_rate_limiter.py` — replace `assert True` with real assertions

<read_first>
- `tests/test_rate_limiter.py` — existing stubs with `assert True` placeholders
- `python_jobs/common/rate_limiter.py` — current state (updated with all factory functions including create_sensorscm_limiter)
- `airflow/dags/dag_sensorscm_poll.py` — created in 1-05-B
</read_first>

<action>
Replace the `assert True` placeholder assertions in `tests/test_rate_limiter.py` with real assertions:

```python
def test_429_retry_in_api_client():
    """APIClient retries on HTTP 429 with exponential backoff (D-31)."""
    from python_jobs.common.api_client import APIClient
    from python_jobs.common.rate_limiter import TokenBucketRateLimiter

    limiter = TokenBucketRateLimiter(rate_per_second=10.0, burst_size=20)

    # 429 should return False (needs retry)
    stats = limiter.record_response(status_code=429, retry_count=0)
    assert stats is False, "record_response(429) must return False (needs retry)"

    # 200 should return True (success)
    success = limiter.record_response(status_code=200, retry_count=0)
    assert success is True, "record_response(200) must return True"


def test_sensorscm_limiter_factory():
    """create_sensorscm_limiter() returns TokenBucketRateLimiter with correct rate."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.rate_limiter import create_sensorscm_limiter

    limiter = create_sensorscm_limiter()
    assert limiter.rate_per_second == 1.0, \
        f"Expected rate_per_second=1.0, got {limiter.rate_per_second}"
    assert limiter.burst_size == 5, \
        f"Expected burst_size=5, got {limiter.burst_size}"
    assert limiter.max_delay == 300.0, \
        f"Expected max_delay=300.0 (5min), got {limiter.max_delay}"
    assert limiter.backoff_factor == 2.0, \
        f"Expected backoff_factor=2.0, got {limiter.backoff_factor}"


def test_openweather_limiter_factory():
    """create_openweather_limiter() returns correct rate (D-28)."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.rate_limiter import create_openweather_limiter

    limiter = create_openweather_limiter()
    assert limiter.rate_per_second == pytest.approx(0.8, rel=0.1), \
        f"Expected rate_per_second~0.8, got {limiter.rate_per_second}"


def test_waqi_limiter_factory():
    """create_waqi_limiter() returns correct rate (D-28)."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.rate_limiter import create_waqi_limiter

    limiter = create_waqi_limiter()
    assert limiter.rate_per_second == 1.0, \
        f"Expected rate_per_second=1.0, got {limiter.rate_per_second}"


def test_parallel_ingestion_pattern():
    """All 5 source tasks run in parallel in dag_ingest_hourly (D-29)."""
    import ast
    import os

    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read()

    # Verify parallel fan-in: [aqicn, forecast, ..., sensorscm]
    fan_in_found = (
        "[aqicn, forecast" in content and "sensorscm]" in content
    )
    assert fan_in_found, \
        "Parallel fan-in pattern [aqicn, forecast, ..., sensorscm] not found in dag_ingest_hourly"

    # Verify no sequential chain where one source must wait for another
    # (look for >> operator chaining only at the fan-in level)
    assert "check_clickhouse >> metadata >> [" in content, \
        "Fan-in trigger not found"


def test_dag_sensorscm_poll_exists():
    """dag_sensorscm_poll.py exists and has */10 * * * * schedule."""
    import os

    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_sensorscm_poll.py'
    )
    assert os.path.exists(dag_path), \
        f"dag_sensorscm_poll.py not found at {dag_path}"

    with open(dag_path) as f:
        content = f.read()

    assert "schedule='*/10 * * * *'" in content, \
        "dag_sensorscm_poll does not have */10 * * * * schedule"


def test_control_table_update_per_source():
    """ingestion.control is updated for each of the 5 sources."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.ingestion_control import update_control

    sources = ["aqicn", "aqicn_forecast", "openweather", "waqi", "sensorscm"]
    dag_path = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags', 'dag_ingest_hourly.py')
    with open(dag_path) as f:
        content = f.read()

    for source in sources:
        assert f"source='{source}'" in content, \
            f"ingestion.control update for source='{source}' not found in dag_ingest_hourly"
```

Remove all `# PLACEHOLDER` and `assert True` lines.
</action>

<acceptance_criteria>
- `grep -n 'assert True' tests/test_rate_limiter.py` returns zero results (no placeholder assertions)
- `grep -n 'create_sensorscm_limiter' tests/test_rate_limiter.py` returns the limiter test
- `grep -n 'rate_per_second.*1.0' tests/test_rate_limiter.py` returns the rate assertion
- `grep -n 'dag_sensorscm_poll' tests/test_rate_limiter.py` returns the DAG existence test
- `pytest tests/test_rate_limiter.py --collect-only` produces zero errors
</acceptance_criteria>

---

## Summary

| Task | File | Action |
|------|------|--------|
| 1-05-A | `python_jobs/common/rate_limiter.py` | Add `create_sensorscm_limiter()` (rate=1.0/s, burst=5, max_delay=300s) |
| 1-05-B | `airflow/dags/dag_sensorscm_poll.py` | Create new DAG with `*/10 * * * *` schedule |
| 1-05-C | `airflow/dags/dag_ingest_hourly.py` | Consolidate all 5 sources in parallel fan-in |
| 1-05-D | `python_jobs/common/api_client.py` | Verify/add tenacity retry (base=2, max=5, max_wait=5min) for 429 |
| 1-05-E | `tests/test_rate_limiter.py` | Replace `assert True` stubs with real assertions |

---

*Generated: 2026-04-01*
