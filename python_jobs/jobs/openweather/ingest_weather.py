#!/usr/bin/env python3
"""
OpenWeather Meteorological Data Ingestion Job.

Fetches current weather (temp, humidity, wind, etc.) for 62 Vietnam provinces
and stores them in ClickHouse.

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.rate_limiter import create_openweather_limiter
from common import get_data_writer
from common.ingestion_control import update_control
from models.openweather_models import VIETNAM_CITIES


def create_openweather_client(api_token: str) -> APIClient:
    return APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token=api_token,
        timeout=30,
        rate_limiter=create_openweather_limiter(),
    )


def transform_weather_response(resp: Dict[str, Any], city_name: str, lat: float, lon: float) -> Dict[str, Any]:
    """Transform OpenWeather /weather response to ClickHouse schema."""
    main = resp.get("main", {})
    wind = resp.get("wind", {})
    clouds = resp.get("clouds", {})
    
    return {
        "source": "openweather",
        "ingest_time": datetime.now(timezone.utc),
        "ingest_batch_id": f"weather_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "province": city_name,
        "latitude": lat,
        "longitude": lon,
        "timestamp_utc": datetime.fromtimestamp(resp.get("dt"), tz=timezone.utc) if resp.get("dt") else datetime.now(timezone.utc),
        "temp": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "visibility": resp.get("visibility"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "clouds_all": clouds.get("all"),
        "raw_payload": str(resp)
    }


def run_weather_ingestion(writer, client) -> int:
    logger = logging.getLogger(__name__)
    all_weather = []

    def fetch_city_weather(city_name: str, coords: Dict[str, float]) -> Optional[Dict[str, Any]]:
        lat, lon = coords["lat"], coords["lon"]
        try:
            resp = client.get(
                "/weather",
                params={"lat": lat, "lon": lon, "units": "metric", "appid": client.token}
            )
            return transform_weather_response(resp, city_name, lat, lon)
        except Exception as e:
            logger.warning(f"[{city_name}] weather fetch failed: {e}")
            return None

    # Use parallel threads to overlap latency. Rate limiter handles the throttling.
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_city = {
            executor.submit(fetch_city_weather, city, coords): city 
            for city, coords in VIETNAM_CITIES.items()
        }
        
        for future in as_completed(future_to_city):
            record = future.result()
            if record:
                all_weather.append(record)

    if all_weather:
        writer.write_batch("raw_openweather_meteorology", all_weather, source="openweather")
        logger.info(f"Wrote {len(all_weather)} weather records")
    
    return len(all_weather)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    api_token = os.environ.get("OPENWEATHER_API_TOKEN")
    if not api_token:
        logger.error("OPENWEATHER_API_TOKEN not set")
        sys.exit(1)

    client = create_openweather_client(api_token)
    writer = get_data_writer()

    try:
        count = run_weather_ingestion(writer, client)
        update_control(source="openweather_weather", records_ingested=count, success=True)
        logger.info(f"Meteorology ingestion done: {count} records")
    except Exception as e:
        logger.error(f"Meteorology ingestion failed: {e}")
        update_control(source="openweather_weather", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
