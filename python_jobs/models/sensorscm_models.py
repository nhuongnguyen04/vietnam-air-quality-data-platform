"""
Sensors.Community (Luftdaten) data models.

API base: https://data.sensor.community/static/v2/data.json (VN filtered by country='VN')
Fields: P1 → PM10, P2 → PM2.5, temperature, humidity

Quality flags per D-23, D-26:
  - 'valid'      : normal reading, in bbox, within 0-500 µg/m³ for PM
  - 'implausible': PM2.5 or PM10 outside 0-500 µg/m³ (regardless of location)
  - 'outlier'    : sensor location outside Vietnam bbox (8.4°N–23.4°N, 102.1°E–109.5°E)

All data is inserted regardless of flag (D-05).

Author: Air Quality Data Platform
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Vietnam bounding box (D-23/D-26)
VIETNAM_BBOX = {
    "lat_min": 8.4,
    "lat_max": 23.4,
    "lon_min": 102.1,
    "lon_max": 109.5,
}

# Vietnam API bbox center (used in API request)
VIETNAM_BBOX_CENTER = {
    "lat": 16.0,
    "latDelta": 7.5,
    "lng": 105.0,
    "lngDelta": 7.0,
}

# Parameter name mapping: API value_type → canonical names
PARAMETER_MAP = {
    "P1":          "pm10",
    "P2":          "pm25",
    "temperature": "temperature",
    "humidity":    "humidity",
}

# Implausible value threshold (D-23)
IMPLAUSIBLE_PM_THRESHOLD = 500.0   # µg/m³


def is_in_vietnam_bbox(lat: Optional[float], lon: Optional[float]) -> bool:
    """Check if coordinates are within Vietnam bounding box."""
    if lat is None or lon is None:
        return False
    return (
        VIETNAM_BBOX["lat_min"] <= lat <= VIETNAM_BBOX["lat_max"]
        and VIETNAM_BBOX["lon_min"] <= lon <= VIETNAM_BBOX["lon_max"]
    )


def assign_quality_flag(
    parameter: str,
    value: float,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> str:
    """
    Assign quality flag per D-23 and D-26.

    Rules (evaluated in order):
      1. 'outlier'     — lat/lon outside Vietnam bbox
      2. 'implausible' — PM2.5 or PM10 outside 0–500 µg/m³
      3. 'valid'       — all other readings

    All data is inserted regardless of flag (D-05).
    """
    # Check geographic outlier first (D-26)
    if lat is not None and lon is not None:
        if not is_in_vietnam_bbox(lat, lon):
            return "outlier"

    # Check implausible values (D-23)
    if parameter in ("pm25", "pm10"):
        if value < 0.0 or value > IMPLAUSIBLE_PM_THRESHOLD:
            return "implausible"

    return "valid"


def map_parameter(api_value_type: str) -> Optional[str]:
    """Map API value_type to canonical parameter name."""
    return PARAMETER_MAP.get(api_value_type)


def transform_sensor_reading(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform a single Sensors.Community sensor reading into measurement records.

    data.json response shape (new format):
    {
      "id": 12345,
      "timestamp": "2026-04-02 02:52:16",
      "location": {
        "id": 30138,
        "latitude": "20.964",
        "longitude": "105.83",
        "altitude": "6.8",
        "country": "VN",
        "exact_location": 0,
        "indoor": 1
      },
      "sensor": {
        "id": 44434,
        "pin": "1",
        "sensor_type": {"id": 14, "name": "SDS011", "manufacturer": "Nova Fitness"}
      },
      "sensordatavalues": [
        {"id": 66949859574, "value": "137.43", "value_type": "P1"},
        {"id": 66949859576, "value": "67.90", "value_type": "P2"}
      ]
    }

    Returns one record per sensordatavalue entry.
    """
    records = []

    sensor_id = record.get("sensor", {}).get("id")
    if not sensor_id:
        logger.warning("Record missing sensor.id, skipping")
        return records

    sensor_type = record.get("sensor", {}).get("sensor_type", {}).get("name", "unknown")
    location = record.get("location", {})

    # lat/lon can be strings or floats in data.json
    lat_raw = location.get("latitude")
    lon_raw = location.get("longitude")
    try:
        lat = float(lat_raw) if lat_raw is not None else None
    except (ValueError, TypeError):
        lat = None
    try:
        lon = float(lon_raw) if lon_raw is not None else None
    except (ValueError, TypeError):
        lon = None

    # Timestamp at record root (new format) — not nested under "data"
    timestamp_str = record.get("timestamp")
    try:
        timestamp_utc = datetime.fromisoformat(
            timestamp_str.replace("Z", "+00:00")
        ) if timestamp_str else datetime.now(timezone.utc)
    except Exception:
        timestamp_utc = datetime.now(timezone.utc)

    # sensordatavalues at record root (new format)
    sensordatavalues = record.get("sensordatavalues", [])
    for sv in sensordatavalues:
        value_type = sv.get("value_type")
        value_str = sv.get("value")

        if value_type is None or value_str is None:
            continue

        try:
            value = float(value_str)
        except (ValueError, TypeError):
            logger.warning("Non-numeric value for %s: %s", value_type, value_str)
            continue

        parameter = map_parameter(value_type)
        if parameter is None:
            # Unknown value type, skip
            continue

        quality_flag = assign_quality_flag(parameter, value, lat, lon)

        record_out = {
            "sensor_id": sensor_id,
            "station_id": sensor_id,
            "latitude": lat,
            "longitude": lon,
            "timestamp_utc": timestamp_utc,
            "parameter": parameter,
            "value": value,
            "unit": (
                "µg/m³" if parameter in ("pm10", "pm25")
                else "°C" if parameter == "temperature"
                else "%"
            ),
            "sensor_type": sensor_type,
            "quality_flag": quality_flag,
            "raw_payload": str(record),
        }
        records.append(record_out)

    return records


def transform_sensorscm_response(
    response: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Transform the full Sensors.Community API response into measurement records.

    Args:
        response: List of station objects from /feeds/ endpoint

    Returns:
        Flat list of all measurement records from all stations.
    """
    all_records = []
    for station in response:
        records = transform_sensor_reading(station)
        all_records.extend(records)
    logger.info(
        "Transformed %d stations into %d measurement records",
        len(response),
        len(all_records),
    )
    return all_records
