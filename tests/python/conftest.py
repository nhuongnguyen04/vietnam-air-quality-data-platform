"""Shared pytest fixtures for the Python test suite."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_clickhouse_client() -> MagicMock:
    """Return a ClickHouse client double with insert/query/close stubs."""
    client = MagicMock()
    client.insert = MagicMock()
    client.query = MagicMock(return_value=MagicMock())
    client.close = MagicMock()
    return client


@pytest.fixture
def sample_openweather_response() -> dict:
    """Return a representative OpenWeather air pollution payload."""
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
                    "nh3": 0.8,
                },
                "dt": 1743500000,
            }
        ],
    }
