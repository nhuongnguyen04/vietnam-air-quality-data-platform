"""
OpenWeather Air Pollution API data models.

Author: Air Quality Data Platform
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Vietnam city centroids (D-03)
VIETNAM_CITIES = {
    "Hanoi":   {"lat": 21.0, "lon": 105.8},
    "HCMC":    {"lat": 10.8, "lon": 106.7},
    "Da Nang": {"lat": 16.1, "lon": 108.2},
}

# Parameter name mapping: OpenWeather API names → canonical names
PARAMETER_MAP = {
    "co":    "co",
    "no":    "no",
    "no2":   "no2",
    "o3":    "o3",
    "so2":   "so2",
    "pm2_5": "pm25",
    "pm10":  "pm10",
    "nh3":   "nh3",
}


def assign_quality_flag(parameter: str, value: float) -> str:
    """
    Assign quality flag based on parameter and value (D-05 pattern).

    Returns 'valid' for PM2.5 in 0–500 µg/m³; 'implausible' outside range.
    All other parameters get 'valid' for now.
    """
    if parameter == "pm25":
        if 0 <= value <= 500:
            return "valid"
        return "implausible"
    return "valid"


def transform_city_response(
    response: Dict[str, Any],
    city_name: str,
    lat: float,
    lon: float,
    is_forecast: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transform OpenWeather /air_pollution or /air_pollution/forecast response
    for a single city into a list of measurement records.

    Args:
        response: API JSON response
        city_name: City display name
        lat: Latitude
        lon: Longitude
        is_forecast: If True, adds forecast_horizon_hours field; records go to
                     raw_openweather_forecast table. If False, records are
                     current observations for raw_openweather_measurements.
    """
    records = []
    items = response.get("list", [])
    now = datetime.now(timezone.utc)

    for item in items:
        dt = item.get("dt")
        if dt:
            timestamp_utc = datetime.fromtimestamp(dt, tz=timezone.utc)
        else:
            timestamp_utc = now

        aqi_reported = item.get("main", {}).get("aqi")
        components = item.get("components", {})

        # Compute forecast horizon (hours ahead of now) for forecast records
        forecast_horizon_hours: int | None = None
        if is_forecast:
            delta = timestamp_utc - now
            forecast_horizon_hours = max(1, int(delta.total_seconds() / 3600))

        for api_name, canonical_name in PARAMETER_MAP.items():
            value = components.get(api_name)
            if value is None:
                continue

            quality_flag = assign_quality_flag(canonical_name, value)

            record: Dict[str, Any] = {
                "station_id": f"openweather:{city_name}:{lat}:{lon}",
                "city_name": city_name,
                "latitude": lat,
                "longitude": lon,
                "timestamp_utc": timestamp_utc,
                "parameter": canonical_name,
                "value": float(value),
                "aqi_reported": aqi_reported,
                "quality_flag": quality_flag,
                "raw_payload": str(item),
            }

            if is_forecast:
                record["forecast_horizon_hours"] = forecast_horizon_hours

            records.append(record)

    return records


def transform_history_response(
    response: Dict[str, Any],
    city_name: str,
    lat: float,
    lon: float,
) -> List[Dict[str, Any]]:
    """
    Transform OpenWeather /air_pollution/history response.
    Same as transform_city_response but extracts from 'list' array.
    Historical data = past observations (is_forecast=False).
    """
    return transform_city_response(response, city_name, lat, lon, is_forecast=False)
