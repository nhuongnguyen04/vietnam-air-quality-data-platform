"""Unit tests for rate limiter helpers."""

from __future__ import annotations

import pytest

from python_jobs.common.rate_limiter import (
    TokenBucketRateLimiter,
    create_openweather_limiter,
    create_sensorscm_limiter,
)


@pytest.mark.unit
def test_record_response_returns_false_and_sleeps_on_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept = []
    limiter = TokenBucketRateLimiter(initial_delay=1.0, backoff_factor=2.0, jitter=0.0)

    monkeypatch.setattr("python_jobs.common.rate_limiter.time.sleep", slept.append)

    should_stop_retrying = limiter.record_response(status_code=429, retry_count=1)

    assert should_stop_retrying is False
    assert slept == [2.0]


@pytest.mark.unit
def test_openweather_limiter_factory_uses_safe_rate() -> None:
    limiter = create_openweather_limiter()

    assert limiter.rate_per_second == pytest.approx(0.9, rel=0.01)
    assert limiter.burst_size == 4
    assert limiter.max_delay == 300.0
    assert limiter.backoff_factor == 2.0


@pytest.mark.unit
def test_sensorscm_limiter_factory_defaults() -> None:
    limiter = create_sensorscm_limiter()

    assert limiter.rate_per_second == 1.0
    assert limiter.burst_size == 5
    assert limiter.max_delay == 300.0
    assert limiter.backoff_factor == 2.0
