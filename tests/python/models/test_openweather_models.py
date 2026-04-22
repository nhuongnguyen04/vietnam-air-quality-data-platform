"""Unit and integration tests for OpenWeather model helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from python_jobs.models.openweather_models import (
    assign_quality_flag,
    get_weather_clusters,
    load_ingestion_points,
    transform_city_response,
)


@pytest.mark.unit
def test_assign_quality_flag_marks_implausible_pm25_values() -> None:
    assert assign_quality_flag("pm25", 12.5) == "valid"
    assert assign_quality_flag("pm25", 999) == "implausible"
    assert assign_quality_flag("pm10", 999) == "valid"


@pytest.mark.unit
def test_transform_city_response_maps_components_and_skips_future_records() -> None:
    now = datetime.now(timezone.utc)
    response = {
        "list": [
            {
                "main": {"aqi": 2},
                "components": {"pm2_5": 12.5, "pm10": 25.0, "o3": 68.4},
                "dt": int(now.timestamp()),
            },
            {
                "main": {"aqi": 5},
                "components": {"pm2_5": 999.0},
                "dt": int((now + timedelta(minutes=10)).timestamp()),
            },
        ]
    }

    records = transform_city_response(
        response=response,
        province_name="Hà Nội",
        ward_name="Phường Hàng Bạc",
        ward_code="00123",
        lat=21.0285,
        lon=105.8542,
    )

    assert len(records) == 3
    assert {record["parameter"] for record in records} == {"pm25", "pm10", "o3"}
    pm25_record = next(record for record in records if record["parameter"] == "pm25")
    assert pm25_record["ward_code"] == "00123"
    assert pm25_record["province_name"] == "Hà Nội"
    assert pm25_record["quality_flag"] == "valid"


@pytest.mark.unit
def test_get_weather_clusters_assigns_cluster_ids() -> None:
    points = {
        "ward-a": {"lat": 21.03, "lon": 105.85, "province": "Hà Nội", "ward": "A", "code": "a"},
        "ward-b": {"lat": 21.04, "lon": 105.84, "province": "Hà Nội", "ward": "B", "code": "b"},
        "ward-c": {"lat": 10.82, "lon": 106.63, "province": "TP Hồ Chí Minh", "ward": "C", "code": "c"},
    }

    clusters = get_weather_clusters(points, grid_size=0.2)

    assert len(clusters) == 2
    assert points["ward-a"]["cluster_id"] == points["ward-b"]["cluster_id"]
    assert points["ward-c"]["cluster_id"] != points["ward-a"]["cluster_id"]


@pytest.mark.integration
def test_load_ingestion_points_reads_seed_data() -> None:
    points = load_ingestion_points()

    assert points
    sample = next(iter(points.values()))
    assert {"lat", "lon", "province", "ward", "code"} <= set(sample)
