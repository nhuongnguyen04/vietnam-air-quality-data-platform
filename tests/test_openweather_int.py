"""Integration tests for OpenWeather Air Pollution ingestion (Plan 1.01).

These tests run against the full stack (or mock) and verify end-to-end behavior.
Requires: OPENWEATHER_API_TOKEN env var set (skipped if not present).
"""

import os
import pytest

# Skip if API token not configured
SKIP_IF_NO_TOKEN = pytest.mark.skipif(
    not os.environ.get("OPENWEATHER_API_TOKEN"),
    reason="OPENWEATHER_API_TOKEN not set"
)


@SKIP_IF_NO_TOKEN
def test_openweather_api_current_call():
    """Verify /air_pollution endpoint returns data for Hanoi city centroid."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.api_client import APIClient
    from python_jobs.common.rate_limiter import create_openweather_limiter

    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token=os.environ["OPENWEATHER_API_TOKEN"],
        timeout=30,
        max_retries=3,
        rate_limiter=create_openweather_limiter(),
        auth_header_name=None,
    )

    resp = client.get(
        "/air_pollution",
        params={"lat": 21.0, "lon": 105.8, "appid": client.token}
    )
    assert "list" in resp
    assert len(resp["list"]) > 0
    client.close()


@SKIP_IF_NO_TOKEN
def test_openweather_api_forecast_call():
    """Verify /air_pollution/forecast endpoint returns data."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.api_client import APIClient
    from python_jobs.common.rate_limiter import create_openweather_limiter

    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token=os.environ["OPENWEATHER_API_TOKEN"],
        timeout=30,
        max_retries=3,
        rate_limiter=create_openweather_limiter(),
        auth_header_name=None,
    )

    resp = client.get(
        "/air_pollution/forecast",
        params={"lat": 16.1, "lon": 108.2, "appid": client.token}
    )
    assert "list" in resp
    assert len(resp["list"]) > 0
    client.close()


def test_openweather_transform_city_response(sample_openweather_response):
    """Transform function produces correct record structure."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.openweather_models import transform_city_response

    records = transform_city_response(sample_openweather_response, "Hanoi", 21.0, 105.8)
    assert len(records) > 0
    assert records[0]["station_id"].startswith("openweather:Hanoi:")
    assert records[0]["parameter"] in ["pm25", "pm10", "o3", "no2", "so2", "co", "nh3", "no"]
    assert records[0]["quality_flag"] in ["valid", "implausible"]
