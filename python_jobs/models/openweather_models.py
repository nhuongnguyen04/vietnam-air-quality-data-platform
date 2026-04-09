"""
OpenWeather Air Pollution API data models.

Author: Air Quality Data Platform
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Vietnam provinces and municipalities — centroid coordinates.
# 62 entries covering all provinces, centrally-administered cities, and
# provincial-level municipalities. Coordinates are approximate centroids.
#
# Incremental run: 62 locations × 2 endpoints (current + forecast) = 124 API calls.
# Historical run:  62 locations × 1 endpoint (history) = 62 API calls.
# OpenWeather free tier: 60 calls/min, 1M calls/month.
# Rate limited to 40 calls/min → ~3.1 min/incremental run.
VIETNAM_CITIES = {
    # --- Centrally-administered cities (3) ---
    "Hanoi":           {"lat": 21.0285, "lon": 105.8542},
    "Ho Chi Minh":     {"lat": 10.8231, "lon": 106.6297},
    "Da Nang":         {"lat": 16.0544, "lon": 108.2022},
    # --- Provincial-level municipalities (5) ---
    "Hai Phong":       {"lat": 20.8449, "lon": 106.6881},
    "Can Tho":         {"lat": 10.0452, "lon": 105.7469},
    "Thai Nguyen":     {"lat": 21.5941, "lon": 105.8481},
    "Nam Dinh":        {"lat": 20.4206, "lon": 106.1732},
    "Vinh":            {"lat": 18.6799, "lon": 105.6814},
    # --- Northern midland & mountains (25 provinces) ---
    "Ha Giang":        {"lat": 22.8024, "lon": 104.9844},
    "Cao Bang":        {"lat": 22.6657, "lon": 106.2680},
    "Lao Cai":         {"lat": 22.4862, "lon": 103.9505},
    "Yen Bai":         {"lat": 21.6800, "lon": 104.8629},
    "Tuyen Quang":     {"lat": 21.8230, "lon": 105.2149},
    "Lang Son":        {"lat": 21.8538, "lon": 106.7610},
    "Quang Ninh":      {"lat": 21.0064, "lon": 107.2925},
    "Bac Giang":       {"lat": 21.2734, "lon": 106.3223},
    "Phu Tho":         {"lat": 21.3737, "lon": 105.2225},
    "Thai Binh":       {"lat": 20.5345, "lon": 106.3495},
    "Hai Duong":       {"lat": 20.9406, "lon": 106.3209},
    "Hung Yen":        {"lat": 20.6464, "lon": 106.0538},
    "Hoa Binh":        {"lat": 20.6525, "lon": 105.3376},
    "Son La":          {"lat": 21.3250, "lon": 103.9180},
    "Lai Chau":        {"lat": 22.1000, "lon": 103.4100},
    "Dien Bien":       {"lat": 21.3836, "lon": 103.0192},
    "Bac Kan":         {"lat": 22.1466, "lon": 105.8348},
    "Bac Ninh":        {"lat": 21.1218, "lon": 106.0780},
    "Vinh Phuc":       {"lat": 21.3609, "lon": 105.5973},
    "Ninh Binh":       {"lat": 20.2500, "lon": 105.9750},
    "Ha Nam":          {"lat": 20.5833, "lon": 105.9167},
    "Thanh Hoa":       {"lat": 19.8040, "lon": 105.4392},
    "Nghe An":         {"lat": 19.2483, "lon": 104.9204},
    "Ha Tinh":         {"lat": 18.3496, "lon": 105.6667},
    "Quang Binh":      {"lat": 17.6108, "lon": 106.4250},
    "Quang Tri":       {"lat": 16.7386, "lon": 107.0911},
    "Thua Thien Hue":  {"lat": 16.4678, "lon": 107.5906},
    "Quang Nam":       {"lat": 15.5774, "lon": 108.4741},
    "Quang Ngai":      {"lat": 15.1205, "lon": 108.7243},
    "Khanh Hoa":       {"lat": 12.2588, "lon": 109.0521},
    # --- Central Highlands (5 provinces) ---
    "Kon Tum":         {"lat": 14.3498, "lon": 108.0000},
    "Gia Lai":         {"lat": 13.8079, "lon": 108.1094},
    "Dak Lak":         {"lat": 12.7380, "lon": 108.2200},
    "Dak Nong":        {"lat": 12.2645, "lon": 107.6090},
    "Lam Dong":        {"lat": 11.5755, "lon": 108.1426},
    # --- South East (7 provinces) ---
    "Binh Phuoc":      {"lat": 11.4528, "lon": 106.8833},
    "Tay Ninh":        {"lat": 11.3667, "lon": 106.1333},
    "Binh Duong":      {"lat": 11.0690, "lon": 106.6540},
    "Dong Nai":        {"lat": 11.0686, "lon": 107.1670},
    "Ba Ria Vung Tau": {"lat": 10.5417, "lon": 107.2425},
    "Binh Thuan":      {"lat": 10.9299, "lon": 108.0980},
    "Ninh Thuan":      {"lat": 11.6922, "lon": 108.9000},
    # --- Mekong Delta (13 provinces) ---
    "Long An":         {"lat": 10.6926, "lon": 106.1408},
    "Tien Giang":      {"lat": 10.3622, "lon": 106.0955},
    "Ben Tre":         {"lat": 10.2433, "lon": 106.3753},
    "Tra Vinh":        {"lat": 9.9475,  "lon": 106.3375},
    "Vinh Long":       {"lat": 10.0833, "lon": 105.9667},
    "Dong Thap":       {"lat": 10.4932, "lon": 105.6882},
    "An Giang":        {"lat": 10.5211, "lon": 105.1278},
    "Kien Giang":      {"lat": 10.1518, "lon": 105.1875},
    "Hau Giang":       {"lat": 9.7578,  "lon": 105.7128},
    "Soc Trang":       {"lat": 9.6029,  "lon": 105.9730},
    "Bac Lieu":        {"lat": 9.2942,  "lon": 105.7248},
    "Ca Mau":          {"lat": 9.1769,  "lon": 105.1500},
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
