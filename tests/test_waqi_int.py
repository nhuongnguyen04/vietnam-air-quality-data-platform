"""Integration tests for WAQI / World Air Quality Index ingestion (Plan 1.02).

Requires: WAQI_API_TOKEN env var set (skipped if not present).
"""

import os
import pytest

SKIP_IF_NO_TOKEN = pytest.mark.skipif(
    not os.environ.get("WAQI_API_TOKEN"),
    reason="WAQI_API_TOKEN not set"
)


@SKIP_IF_NO_TOKEN
def test_waqi_bbox_api_call():
    """Verify bounding-box endpoint returns data for Vietnam."""
    import sys
    import os as os_module
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.api_client import APIClient
    from python_jobs.common.rate_limiter import create_waqi_limiter

    client = APIClient(
        base_url="https://api.waqi.info",
        token=os.environ["WAQI_API_TOKEN"],
        timeout=30,
        max_retries=3,
        rate_limiter=create_waqi_limiter(),
        auth_header_name=None,
    )

    resp = client.get(
        "/feed/geo:8.4;102.1;23.4;109.5/",
        params={"token": client.token}
    )
    assert resp.get("status") == "ok"
    assert "data" in resp
    client.close()


def test_waqi_transform_feed_response(sample_waqi_response):
    """Transform function produces correct record structure."""
    import sys
    import os as os_module
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.waqi_models import transform_waqi_station

    records = transform_waqi_station(sample_waqi_response["data"], station_id="Hanoi")
    assert len(records) >= 1
    assert any(r["parameter"] == "pm25" for r in records)
    assert any(r["parameter"] == "pm10" for r in records)
    assert all(r["station_id"] == "Hanoi" for r in records)
    assert all("value" in r for r in records)
    assert all(r["quality_flag"] == "valid" for r in records)


def test_waqi_build_bbox_url():
    """Bounding-box URL is correctly formed."""
    import sys
    import os as os_module
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.waqi_models import build_bbox_url

    url = build_bbox_url("test_token")
    assert "geo:8.4;102.1;23.4;109.5" in url
    assert "token=test_token" in url
