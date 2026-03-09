"""
AQICN (World Air Quality Index) API Data Models.

This module provides data models for:
- Stations (crawled from HTML + feed API)
- Measurements (from feed endpoint)
- Forecasts (from feed endpoint)

Author: Air Quality Data Platform
"""

import re
import json
import logging
import requests
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger(__name__)


class AQICNStation(BaseModel):
    """AQICN station model from /map/bounds endpoint."""
    uid: str = Field(alias="station_id")
    station_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    station_time: Optional[str] = None
    aqi: Optional[str] = None
    
    class Config:
        populate_by_name = True


class AQICNCity(BaseModel):
    """AQICN city information."""
    geo: List[float] = Field(default_factory=list)
    name: Optional[str] = None
    url: Optional[str] = None
    location: Optional[str] = None


class AQICNTime(BaseModel):
    """AQICN time information."""
    s: Optional[str] = None  # Formatted time string
    tz: Optional[str] = None  # Timezone
    v: Optional[str] = None   # Unix timestamp
    iso: Optional[str] = None  # ISO format


class AQICNIAQI(BaseModel):
    """AQICN Individual Air Quality Index."""
    # Individual pollutant measurements
    pm25: Optional[Dict[str, Any]] = None
    pm10: Optional[Dict[str, Any]] = None
    o3: Optional[Dict[str, Any]] = None
    no2: Optional[Dict[str, Any]] = None
    so2: Optional[Dict[str, Any]] = None
    co: Optional[Dict[str, Any]] = None
    
    # Weather data
    dew: Optional[Dict[str, Any]] = None
    h: Optional[Dict[str, Any]] = None  # Humidity
    p: Optional[Dict[str, Any]] = None  # Pressure
    t: Optional[Dict[str, Any]] = None  # Temperature
    w: Optional[Dict[str, Any]] = None  # Wind
    wg: Optional[Dict[str, Any]] = None  # Wind gust


class AQICNForecastDaily(BaseModel):
    """AQICN forecast daily data."""
    pm10: Optional[List[Dict[str, Any]]] = None
    pm25: Optional[List[Dict[str, Any]]] = None
    o3: Optional[List[Dict[str, Any]]] = None
    no2: Optional[List[Dict[str, Any]]] = None
    so2: Optional[List[Dict[str, Any]]] = None
    co: Optional[List[Dict[str, Any]]] = None
    uvi: Optional[List[Dict[str, Any]]] = None


class AQICNForecast(BaseModel):
    """AQICN forecast data."""
    daily: Optional[AQICNForecastDaily] = None


class AQICNAttribution(BaseModel):
    """AQICN attribution information."""
    url: Optional[str] = None
    name: Optional[str] = None
    logo: Optional[str] = None


class AQICNMeasurement(BaseModel):
    """AQICN measurement model from /feed endpoint."""
    status: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


def fetch_and_parse_station_ids(url: str = "https://aqicn.org/city/vietnam/") -> List[str]:
    """
    Fetch and parse station IDs from the AQICN website bounds.
    
    Extracts IDs from the countryStats.showHistorical([...]) JavaScript call.
    Station IDs are in the "x" field of each JSON object.
    
    Args:
        url: URL to the AQICN country page
        
    Returns:
        List of unique station ID strings
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return []
    
    # Extract the JSON array from countryStats.showHistorical(...)
    match = re.search(r'countryStats\.showHistorical\((\[.*?\])\);', content)
    if not match:
        logger.warning("Could not find countryStats.showHistorical in HTML")
        return []
    
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse showHistorical JSON: {e}")
        return []
    
    # Extract unique station IDs from "x" field
    seen = set()
    station_ids = []
    for item in data:
        station_id = str(item.get("x", ""))
        if station_id and station_id not in seen:
            seen.add(station_id)
            station_ids.append(station_id)
    
    logger.info(f"Parsed {len(station_ids)} unique station IDs from HTML")
    return station_ids


def transform_station(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform raw station API response to database format.
    
    Args:
        raw: Raw response from /map/bounds endpoint (legacy)
        
    Returns:
        Transformed record for ClickHouse
    """
    return {
        "station_id": str(raw.get("uid", "")),
        "station_name": raw.get("station", {}).get("name"),
        "latitude": str(raw.get("lat", "")),
        "longitude": str(raw.get("lon", "")),
        "station_time": raw.get("station", {}).get("time"),
        "aqi": str(raw.get("aqi", "")),
        "raw_payload": str(raw)
    }


def transform_station_from_feed(raw: Dict[str, Any], station_id: str) -> Dict[str, Any]:
    """
    Transform feed API response into a station record for raw_aqicn_stations.
    
    Extracts station metadata (city, geo, url, etc.) from the feed API response
    and creates a record suitable for upserting into raw_aqicn_stations.
    
    Args:
        raw: Raw response from /feed endpoint (full response including status)
        station_id: Station identifier
        
    Returns:
        Transformed station record for ClickHouse
    """
    data = raw.get("data", {})
    if not data:
        return {}
    
    city = data.get("city", {})
    time_info = data.get("time", {})
    aqi = data.get("aqi")
    
    return {
        "station_id": station_id,
        "station_name": city.get("name"),
        "latitude": str(city.get("geo", [None, None])[0]) if city.get("geo") else None,
        "longitude": str(city.get("geo", [None, None])[1]) if city.get("geo") else None,
        "station_time": time_info.get("iso") or time_info.get("s"),
        "aqi": str(aqi) if aqi is not None else None,
        "city_url": city.get("url"),
        "city_location": city.get("location"),
        "raw_payload": str(raw)
    }


def transform_measurement(
    raw: Dict[str, Any],
    station_id: str
) -> List[Dict[str, Any]]:
    """
    Transform raw measurement API response to database format.
    
    Each measurement response contains one reading at a specific time,
    but may contain multiple pollutant values (iaqi). Each pollutant
    is stored as a separate row.
    
    Args:
        raw: Raw response from /feed endpoint
        station_id: Station identifier
        
    Returns:
        List of transformed records (one per pollutant)
    """
    data = raw.get("data", {})
    if not data:
        return []
    
    records = []
    
    # Extract common fields
    city = data.get("city", {})
    time_info = data.get("time", {})
    iaqi = data.get("iaqi", {})
    aqi = data.get("aqi")
    dominentpol = data.get("dominentpol")
    attributions = data.get("attributions", [])
    
    # Common fields for all pollutant records
    # NOTE: Station info (name, lat, lon, url, location) is NOT stored here.
    # It is stored in raw_aqicn_stations via transform_station_from_feed().
    base_record = {
        "station_id": station_id,
        "time_s": time_info.get("s"),
        "time_tz": time_info.get("tz"),
        "time_v": time_info.get("v"),
        "time_iso": time_info.get("iso"),
        "aqi": str(aqi) if aqi else None,
        "dominentpol": dominentpol,
        "attributions": str(attributions),
        "debug_sync": data.get("debug", {}).get("sync") if data.get("debug") else None,
        "raw_payload": str(raw)
    }
    
    # Create one record per pollutant in iaqi
    if iaqi:
        for pollutant, values in iaqi.items():
            record = base_record.copy()
            record["pollutant"] = pollutant
            record["value"] = str(values.get("v")) if values and "v" in values else None
            records.append(record)
    else:
        # If no iaqi, create one record with just the main aqi
        record = base_record.copy()
        record["pollutant"] = "aqi"
        record["value"] = str(aqi) if aqi else None
        records.append(record)
    
    return records


def transform_forecast(
    raw: Dict[str, Any],
    station_id: str
) -> List[Dict[str, Any]]:
    """
    Transform raw forecast API response to database format.
    
    Args:
        raw: Raw response from /feed endpoint
        station_id: Station identifier
        
    Returns:
        List of transformed forecast records
    """
    data = raw.get("data", {})
    if not data:
        return []
    
    records = []
    
    time_info = data.get("time", {})
    forecast = data.get("forecast", {})
    daily = forecast.get("daily", {}) if forecast else {}
    
    # Get measurement time for linking
    measurement_time_v = time_info.get("v")
    
    # Process each pollutant type with forecast data
    pollutant_types = ["pm10", "pm25", "o3", "no2", "so2", "co", "uvi"]
    
    for pollutant in pollutant_types:
        if pollutant in daily:
            for forecast_item in daily[pollutant]:
                record = {
                    "station_id": station_id,
                    "measurement_time_v": str(measurement_time_v) if measurement_time_v else None,
                    "forecast_type": "daily",
                    "pollutant": pollutant,
                    "day": forecast_item.get("day"),
                    "avg": str(forecast_item.get("avg")) if forecast_item.get("avg") else None,
                    "max": str(forecast_item.get("max")) if forecast_item.get("max") else None,
                    "min": str(forecast_item.get("min")) if forecast_item.get("min") else None,
                    "raw_forecast_item": str(forecast_item)
                }
                records.append(record)
    
    return records


def get_all_pollutants() -> List[str]:
    """Get list of all possible pollutant codes in AQICN."""
    return [
        "pm25",   # PM2.5
        "pm10",   # PM10
        "o3",     # Ozone
        "no2",    # Nitrogen Dioxide
        "so2",    # Sulfur Dioxide
        "co",     # Carbon Monoxide
        "aqi",    # Overall AQI
    ]


def get_weather_parameters() -> List[str]:
    """Get list of weather parameters in AQICN iaqi."""
    return [
        "dew",    # Dew point
        "h",      # Humidity
        "p",      # Pressure
        "t",      # Temperature
        "w",      # Wind speed
        "wg",     # Wind gust
    ]

