"""Shared pytest fixtures for Vietnam Air Quality Data Platform tests."""

import os
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_clickhouse_client():
    """Return a mock ClickHouse client that behaves like clickhouse_connect.get_client."""
    client = MagicMock()
    client.insert = MagicMock(return_value=MagicMock())
    client.query = MagicMock(return_value=MagicMock())
    client.close = MagicMock()
    return client


@pytest.fixture
def mock_clickhouse_writer(mock_clickhouse_client):
    """Return a mock ClickHouseWriter that records write_batch calls."""
    with patch("clickhouse_connect.get_client", return_value=mock_clickhouse_client):
        from python_jobs.common.clickhouse_writer import ClickHouseWriter
        writer = ClickHouseWriter(
            host="localhost",
            port=8123,
            user="admin",
            password="admin",
            database="air_quality"
        )
        writer._mock_writes = []
        original_write = writer.write_batch
        def tracking_write(*args, **kwargs):
            result = original_write(*args, **kwargs)
            writer._mock_writes.append((args, kwargs))
            return result
        writer.write_batch = tracking_write
        return writer


@pytest.fixture
def mock_rate_limiter():
    """Return a mock TokenBucketRateLimiter that does not actually sleep."""
    from unittest.mock import MagicMock
    limiter = MagicMock()
    limiter.acquire = MagicMock()
    limiter.record_response = MagicMock(return_value=True)
    limiter.get_stats = MagicMock(return_value={
        "available_tokens": 5.0,
        "burst_size": 5,
        "requests_this_minute": 0,
        "rate_per_second": 1.0
    })
    return limiter


@pytest.fixture
def env_vars():
    """Return a dict of standard environment variables for jobs.
    Loads from .env if available, falling back to placeholder values.
    """
    from dotenv import load_dotenv
    load_dotenv()
    
    return {
        "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "admin"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", "admin"),
        "CLICKHOUSE_DB": os.getenv("CLICKHOUSE_DB", "air_quality"),
        "OPENAQ_API_TOKEN": os.getenv("OPENAQ_API_TOKEN", "test_openaq_token"),
        "AQICN_API_TOKEN": os.getenv("AQICN_API_TOKEN", "test_aqicn_token"),
        "OPENWEATHER_API_TOKEN": os.getenv("OPENWEATHER_API_TOKEN", "test_openweather_token"),
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", "your_test_bot_token"),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", "123456789"),
        "PYTHON_JOBS_DIR": os.getenv("PYTHON_JOBS_DIR", "/opt/python/jobs"),
    }


@pytest.fixture
def sample_openweather_response():
    """Return a mock OpenWeather Air Pollution API response."""
    return {
        "coord": [105.8, 21.0],
        "list": [
            {
                "main": {"aqi": 2},
                "components": {
                    "co": 210.5,
                    "no": 0.2,
                    "no2": 15.3,
                    "o3": 68.4,
                    "so2": 1.1,
                    "pm2_5": 12.5,
                    "pm10": 25.0,
                    "nh3": 0.8
                },
                "dt": 1743500000
            }
        ]
    }


@pytest.fixture
def sample_sensorscm_response():
    """Return a mock Sensors.Community API response."""
    return [
        {
            "id": 12345,
            "sensor": {
                "id": 67890,
                "sensor_type": {"name": "SDS011"},
                "pin": "1"
            },
            "location": {
                "latitude": 10.8231,
                "longitude": 106.6297,
                "country": "VN"
            },
            "data": [
                {
                    "sensordatavalues": [
                        {"value_type": "P1", "value": 45.2},
                        {"value_type": "P2", "value": 28.7}
                    ],
                    "timestamp": "2026-04-01T10:30:00"
                }
            ]
        }
    ]
