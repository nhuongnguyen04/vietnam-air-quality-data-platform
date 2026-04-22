"""Live OpenWeather API checks."""

from __future__ import annotations

import os

import pytest

from python_jobs.common.api_client import APIClient
from python_jobs.common.rate_limiter import create_openweather_limiter


pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("RUN_LIVE_TESTS") != "1",
        reason="RUN_LIVE_TESTS=1 is required for live tests",
    ),
    pytest.mark.skipif(
        not os.environ.get("OPENWEATHER_API_TOKEN"),
        reason="OPENWEATHER_API_TOKEN is required for live tests",
    ),
]


def test_openweather_air_pollution_live() -> None:
    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token=os.environ["OPENWEATHER_API_TOKEN"],
        timeout=30,
        max_retries=3,
        rate_limiter=create_openweather_limiter(),
        auth_header_name="appid",
    )

    try:
        response = client.get("/air_pollution", params={"lat": 21.0285, "lon": 105.8542})
    finally:
        client.close()

    assert "list" in response
    assert response["list"]
