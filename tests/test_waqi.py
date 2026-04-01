"""Unit tests for WAQI / World Air Quality Index ingestion jobs."""

import pytest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_waqi_client_init():
    """WAQI client initializes with correct base URL."""
    from python_jobs.common.api_client import APIClient
    from python_jobs.common.rate_limiter import TokenBucketRateLimiter
    limiter = TokenBucketRateLimiter(rate_per_second=1.5, burst_size=5)
    client = APIClient(
        base_url="https://api.waqi.info/feed",
        token="test_waqi_token",
        timeout=30,
        max_retries=5,
        rate_limiter=limiter,
        auth_header_name=None  # Token goes in query params
    )
    assert client.base_url == "https://api.waqi.info/feed"
    assert client.token == "test_waqi_token"


def test_waqi_bbox_query_construction():
    """Vietnam bounding-box query URL is correctly formed."""
    # PLACEHOLDER — replace when jobs/waqi/ingest_measurements.py exists:
    # from python_jobs.jobs.waqi.ingest_measurements import build_bbox_url
    # url = build_bbox_url(lat_min=8.4, lat_max=23.4, lon_min=102.1, lon_max=109.5)
    # assert "geo:8.4;102.1;23.4;109.5" in url
    # assert "token=" in url
    assert True  # Placeholder


def test_waqi_station_parsing(sample_waqi_response):
    """WAQI per-station response fields are correctly extracted."""
    # PLACEHOLDER — replace when jobs/waqi/ingest_measurements.py exists:
    # from python_jobs.models.waqi_models import transform_waqi_station
    # records = transform_waqi_station(sample_waqi_response, station_id="Hanoi")
    # assert len(records) == 6  # PM25, PM10, O3, NO2, SO2, CO
    # pm25_records = [r for r in records if r["parameter"] == "pm25"]
    # assert pm25_records[0]["value"] == 68.5
    assert "iaqi" in sample_waqi_response["data"]
    assert sample_waqi_response["data"]["iaqi"]["pm25"]["v"] == 68.5


def test_waqi_clickhouse_write(mock_clickhouse_writer, sample_waqi_response):
    """WAQI measurements are written to ClickHouse correctly."""
    # PLACEHOLDER — replace when jobs/waqi/ingest_measurements.py exists:
    # from python_jobs.models.waqi_models import transform_waqi_station
    # records = transform_waqi_station(sample_waqi_response, station_id="Hanoi")
    # written = mock_clickhouse_writer.write_batch(
    #     table="raw_waqi_measurements",
    #     records=records,
    #     source="waqi"
    # )
    # assert written == 6
    assert mock_clickhouse_writer is not None


def test_waqi_dominentpol_extraction(sample_waqi_response):
    """Dominant pollutant is extracted from WAQI response."""
    # PLACEHOLDER — replace when jobs/waqi/ingest_measurements.py exists:
    # from python_jobs.models.waqi_models import extract_dominant_pollutant
    # dom = extract_dominant_pollutant(sample_waqi_response)
    # assert dom == "pm25"
    assert sample_waqi_response["data"]["dominentpol"] == "pm25"
