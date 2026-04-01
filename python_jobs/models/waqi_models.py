"""
WAQI / World Air Quality Index data models.

API base: https://api.waqi.info/feed/
Vietnam bbox: geo:8.4;102.1;23.4;109.5

Author: Air Quality Data Platform
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Vietnam bounding box (D-04)
VIETNAM_BBOX = {
    "lat_min": 8.4,
    "lat_max": 23.4,
    "lon_min": 102.1,
    "lon_max": 109.5,
}

# Parameter name mapping: WAQI iaqi keys → canonical names
PARAMETER_MAP = {
    "pm25": "pm25",
    "pm10": "pm10",
    "o3":   "o3",
    "no2":  "no2",
    "so2":  "so2",
    "co":   "co",
}


def build_bbox_url(token: str) -> str:
    """
    Build the Vietnam bounding-box query URL.

    Returns: https://api.waqi.info/feed/geo:8.4;102.1;23.4;109.5/?token={token}
    """
    lat_min = VIETNAM_BBOX["lat_min"]
    lon_min = VIETNAM_BBOX["lon_min"]
    lat_max = VIETNAM_BBOX["lat_max"]
    lon_max = VIETNAM_BBOX["lon_max"]
    return (
        f"https://api.waqi.info/feed/geo:{lat_min};{lon_min};{lat_max};{lon_max}/"
        f"?token={token}"
    )


def extract_station_name(data: Dict[str, Any]) -> str:
    """Extract station name from WAQI response data."""
    city = data.get("city", {})
    name = city.get("name", "unknown")
    return name.strip()


def extract_timestamp_utc(data: Dict[str, Any]) -> datetime:
    """Extract UTC timestamp from WAQI response time field."""
    time_field = data.get("time", {})
    time_str = time_field.get("iso") if isinstance(time_field, dict) else None
    if time_str:
        try:
            # Handle formats like "2026-04-01T10:00:00+07:00"
            return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(timezone.utc)


def extract_aqi(data: Dict[str, Any]) -> Optional[int]:
    """Extract the overall AQI value from WAQI response."""
    aqi = data.get("aqi")
    if aqi is not None:
        try:
            return int(aqi)
        except (ValueError, TypeError):
            pass
    return None


def extract_dominant_pollutant(data: Dict[str, Any]) -> Optional[str]:
    """Extract dominant pollutant string from WAQI response."""
    return data.get("dominentpol")


def transform_waqi_station(
    data: Dict[str, Any],
    station_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Transform a single WAQI station (city) data object into measurement records.

    The API returns one big response with a nested 'data' object. This function
    is called once per station from the feed response.

    Args:
        data: The 'data' subdictionary from the WAQI feed response.
        station_id: Optional station identifier (defaults to city name slug).

    Returns one record per pollutant (pm25, pm10, o3, no2, so2, co).
    """
    records = []

    city = data.get("city", {})
    city_name = city.get("name", "unknown")
    geo = city.get("geo", [])
    lat = geo[1] if len(geo) > 1 else None
    lon = geo[0] if len(geo) > 0 else None

    # Fallback station_id: slugified city name
    if station_id is None:
        station_id = f"waqi:{city_name.replace(' ', '_').lower()}"

    timestamp_utc = extract_timestamp_utc(data)
    aqi_reported = extract_aqi(data)
    dominant = extract_dominant_pollutant(data)

    iaqi = data.get("iaqi", {})

    for api_key, canonical_name in PARAMETER_MAP.items():
        iaqi_entry = iaqi.get(api_key)
        if iaqi_entry is None:
            continue

        value = iaqi_entry.get("v") if isinstance(iaqi_entry, dict) else None
        if value is None:
            continue

        try:
            value = float(value)
        except (ValueError, TypeError):
            continue

        record = {
            "station_id": station_id,
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "timestamp_utc": timestamp_utc,
            "parameter": canonical_name,
            "value": value,
            "aqi_reported": aqi_reported,
            "dominant_pollutant": dominant,
            "quality_flag": "valid",   # WAQI data is from reference-grade stations
            "raw_payload": str(data),
        }
        records.append(record)

    return records


def transform_waqi_feed_response(
    feed_response: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Transform the full WAQI bounding-box feed response into measurement records.

    The WAQI bounding-box endpoint returns a single 'data' dict (not a list).
    If the response wraps data in 'data' key, extract it; otherwise treat it as data directly.

    Args:
        feed_response: The parsed JSON response from WAQI /feed/geo:bbox/?token=...

    Returns:
        List of measurement records from all stations in the response.
    """
    records = []

    status = feed_response.get("status")
    if status != "ok":
        logger.warning(f"WAQI API returned status: {status}")
        return records

    data = feed_response.get("data")
    if not data:
        logger.warning("WAQI response has no 'data' key")
        return records

    # WAQI bbox endpoint returns a single station or a list under 'data'
    # Check if 'data' contains 'city' (single station) or 'city' is a list
    if isinstance(data, dict):
        city = data.get("city")
        if isinstance(city, dict):
            # Single station response
            recs = transform_waqi_station(data)
            records.extend(recs)
        elif isinstance(city, list):
            # Multiple stations — iterate
            for station_data in city:
                if isinstance(station_data, dict):
                    recs = transform_waqi_station(station_data)
                    records.extend(recs)

    return records
