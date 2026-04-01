"""Tests for rate limiter and orchestration optimization (Plan 1.05)."""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


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
    from python_jobs.common.rate_limiter import create_openweather_limiter

    limiter = create_openweather_limiter()
    assert limiter.rate_per_second == pytest.approx(0.8, rel=0.1), \
        f"Expected rate_per_second~0.8, got {limiter.rate_per_second}"


def test_waqi_limiter_factory():
    """create_waqi_limiter() returns correct rate (D-28)."""
    from python_jobs.common.rate_limiter import create_waqi_limiter

    limiter = create_waqi_limiter()
    assert limiter.rate_per_second == 1.0, \
        f"Expected rate_per_second=1.0, got {limiter.rate_per_second}"


def test_parallel_ingestion_pattern():
    """All 5 source tasks run in parallel in dag_ingest_hourly (D-29)."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read()

    # Verify parallel fan-in: >> [aqicn, ..., waqi] (order may vary)
    fan_in_found = (
        "[aqicn," in content and
        "forecast" in content and
        "openweather" in content and
        "waqi]" in content and
        "sensorscm" in content
    )
    assert fan_in_found, \
        "Parallel fan-in pattern with all 5 sources not found in dag_ingest_hourly"

    # Verify fan-in trigger pattern
    assert "check_clickhouse >> metadata >> [" in content, \
        "Fan-in trigger not found"


def test_dag_sensorscm_poll_exists():
    """dag_sensorscm_poll.py exists and has */10 * * * * schedule."""
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
    from python_jobs.common.ingestion_control import update_control

    sources = ["aqicn", "aqicn_forecast", "openweather", "waqi", "sensorscm"]
    dag_path = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags', 'dag_ingest_hourly.py')
    with open(dag_path) as f:
        content = f.read()

    for source in sources:
        assert f"source='{source}'" in content, \
            f"ingestion.control update for source='{source}' not found in dag_ingest_hourly"
