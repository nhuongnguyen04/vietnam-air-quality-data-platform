"""
OpenWeather Air Pollution API data models.

Author: Air Quality Data Platform
"""

import csv
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

def load_ingestion_points() -> Dict[str, Dict[str, Any]]:
    """
    Load ingestion points from dbt seed CSV.
    Returns a dictionary mapping point_id -> {lat, lon, province, district, layer_type}
    """
    # Use absolute path or relative to project root
    seeds_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "dbt/dbt_tranform/seeds/openweather_ingestion_points.csv"
    )
    
    points = {}
    try:
        with open(seeds_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row['point_id']
                points[pid] = {
                    "lat": float(row['latitude']),
                    "lon": float(row['longitude']),
                    "province": row['province'],
                    "district": row['district'],
                    "layer_type": row['layer_type']
                }
    except Exception as e:
        logger.error(f"Failed to load ingestion points from {seeds_path}: {e}")
        return {}
        
    return points

# Vietnam provinces and municipalities — centroid coordinates.
# 34 provincial-level administrative units as of the 2025 reorganization.
# Centroids represent either the main city or the geographic center of the new units.
VIETNAM_CITIES = {
    # --- Centrally-administered cities (6) ---
    "Hà Nội":           {"lat": 21.0285, "lon": 105.8542},
    "TP Hồ Chí Minh":   {"lat": 10.8231, "lon": 106.6297},
    "Huế":              {"lat": 16.4678, "lon": 107.5906},
    "Đà Nẵng":          {"lat": 16.0544, "lon": 108.2022},
    "Hải Phòng":        {"lat": 20.8449, "lon": 106.6881},
    "Cần Thơ":          {"lat": 10.0452, "lon": 105.7469},
    # --- Provinces (28) ---
    "An Giang":         {"lat": 10.5211, "lon": 105.1278},
    "Bắc Ninh":         {"lat": 21.1218, "lon": 106.0780},
    "Cà Mau":           {"lat": 9.1769,  "lon": 105.1500},
    "Cao Bằng":         {"lat": 22.6657, "lon": 106.2680},
    "Đắk Lắk":          {"lat": 12.7380, "lon": 108.2200},
    "Điện Biên":        {"lat": 21.3836, "lon": 103.0192},
    "Đồng Nai":         {"lat": 11.0686, "lon": 107.1670},
    "Đồng Tháp":        {"lat": 10.4932, "lon": 105.6882},
    "Gia Lai":          {"lat": 13.8079, "lon": 108.1094},
    "Hà Tĩnh":          {"lat": 18.3496, "lon": 105.6667},
    "Hưng Yên":         {"lat": 20.6464, "lon": 106.0538},
    "Khánh Hòa":        {"lat": 12.2588, "lon": 109.0521},
    "Lai Châu":         {"lat": 22.1000, "lon": 103.4100},
    "Lâm Đồng":         {"lat": 11.5755, "lon": 108.1426},
    "Lạng Sơn":         {"lat": 21.8538, "lon": 106.7610},
    "Lào Cai":          {"lat": 22.4862, "lon": 103.9505},
    "Nghệ An":          {"lat": 19.2483, "lon": 104.9204},
    "Ninh Bình":        {"lat": 20.2500, "lon": 105.9750},
    "Phú Thọ":          {"lat": 21.3737, "lon": 105.2225},
    "Quảng Ngãi":       {"lat": 15.1205, "lon": 108.7243},
    "Quảng Ninh":       {"lat": 21.0064, "lon": 107.2925},
    "Quảng Trị":        {"lat": 16.7386, "lon": 107.0911},
    "Sơn La":           {"lat": 21.3250, "lon": 103.9180},
    "Thái Nguyên":      {"lat": 21.5941, "lon": 105.8481},
    "Thanh Hóa":        {"lat": 19.8040, "lon": 105.4392},
    "Tây Ninh":         {"lat": 11.3667, "lon": 106.1333},
    "Tuyên Quang":      {"lat": 21.8230, "lon": 105.2149},
    "Vĩnh Long":        {"lat": 10.0833, "lon": 105.9667},
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
) -> List[Dict[str, Any]]:
    """
    Transform OpenWeather /air_pollution response for a single city 
    into a list of measurement records.

    Args:
        response: API JSON response
        city_name: City display name
        lat: Latitude
        lon: Longitude
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
        
        # Guard: Skip forecast data (timestamps in the future)
        if timestamp_utc > now + timedelta(minutes=5):
            logger.warning(f"Skipping forecast record for {city_name}: {timestamp_utc} (ingest time: {now})")
            continue

        aqi_reported = item.get("main", {}).get("aqi")
        components = item.get("components", {})

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
    Historical data = past observations.
    """
    return transform_city_response(response, city_name, lat, lon)
