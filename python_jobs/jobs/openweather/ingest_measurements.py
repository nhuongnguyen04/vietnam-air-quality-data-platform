#!/usr/bin/env python3
"""
OpenWeather Air Pollution Measurements Ingestion Job.

Fetches current air quality data for 62 Vietnam provinces/cities
and stores them in ClickHouse.

Usage:
    python jobs/openweather/ingest_measurements.py --mode incremental
    python jobs/openweather/ingest_measurements.py --mode historical --start-date 2026-01-01 --end-date 2026-03-31

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
from models.openweather_models import (
    VIETNAM_CITIES,
    transform_city_response,
    transform_history_response,
)


def create_openweather_client(api_token: str) -> APIClient:
    """Create a configured OpenWeather API client."""
    return APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token=api_token,
        timeout=30,
        max_retries=5,
        backoff_factor=2.0,
        rate_limiter=create_openweather_limiter(),
        auth_header_name=None,   # Token goes in query param, not header
    )


def fetch_city_data(
    client: APIClient,
    city_name: str,
    lat: float,
    lon: float,
) -> List[Dict[str, Any]]:
    """
    Fetch current air quality data for one city.

    Returns:
        List of current records for raw_openweather_measurements.
    """
    logger = logging.getLogger(__name__)
    current_records = []

    # Current conditions
    try:
        resp = client.get(
            "/air_pollution",
            params={"lat": lat, "lon": lon, "appid": client.token}
        )
        records = transform_city_response(resp, city_name, lat, lon)
        current_records.extend(records)
        logger.info(f"[{city_name}] current: {len(records)} records")
    except Exception as e:
        logger.warning(f"[{city_name}] current fetch failed: {e}")

    return current_records


def fetch_historical_data(
    client: APIClient,
    city_name: str,
    lat: float,
    lon: float,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    """
    Fetch historical air quality data for one city.

    Calls: GET /air_pollution/history?lat={lat}&lon={lon}&start={unix}&end={unix}&appid={token}

    start/end: datetime objects; converted to Unix timestamps.
    """
    logger = logging.getLogger(__name__)
    all_records = []

    try:
        resp = client.get(
            "/air_pollution/history",
            params={
                "lat": lat,
                "lon": lon,
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
                "appid": client.token,
            }
        )
        records = transform_history_response(resp, city_name, lat, lon)
        all_records.extend(records)
        logger.info(f"[{city_name}] historical: {len(records)} records from {start.date()} to {end.date()}")
    except Exception as e:
        logger.warning(f"[{city_name}] historical fetch failed: {e}")

    return all_records


def run_incremental(writer, client) -> int:
    """Fetch + write current measurements for all cities. Returns row count."""
    logger = logging.getLogger(__name__)
    all_measurements = []

    def fetch_one(city_name: str, coords: Dict[str, float]) -> List[Dict[str, Any]]:
        lat, lon = coords["lat"], coords["lon"]
        return fetch_city_data(client, city_name, lat, lon)

    # Parallelize cities. Rate limiter ensures safety.
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_one, name, c) for name, c in VIETNAM_CITIES.items()]
        for future in as_completed(futures):
            try:
                all_measurements.extend(future.result())
            except Exception as e:
                logger.error(f"Worker failed: {e}")

    if all_measurements:
        writer.write_batch("raw_openweather_measurements", all_measurements, source="openweather")
        logger.info(f"Wrote {len(all_measurements)} openweather measurements")
    else:
        logger.warning("No openweather records collected")

    return len(all_measurements)


def run_historical(writer, client, start_date: datetime, end_date: datetime) -> int:
    """Fetch + write historical data for all cities. Returns row count."""
    logger = logging.getLogger(__name__)
    all_records = []

    def fetch_one(city_name: str, coords: Dict[str, float]) -> List[Dict[str, Any]]:
        lat, lon = coords["lat"], coords["lon"]
        return fetch_historical_data(client, city_name, lat, lon, start_date, end_date)

    # Parallelize cities. Rate limiter ensures safety.
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_one, name, c) for name, c in VIETNAM_CITIES.items()]
        for future in as_completed(futures):
            try:
                all_records.extend(future.result())
            except Exception as e:
                logger.error(f"Worker failed: {e}")

    if all_records:
        writer.write_batch("raw_openweather_measurements", all_records, source="openweather")
        logger.info(f"Wrote {len(all_records)} openweather historical measurements")
    else:
        logger.warning("No openweather historical records collected")

    return len(all_records)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Ingest OpenWeather air pollution data")
    parser.add_argument("--mode", choices=["incremental", "historical"], default="incremental")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (historical mode)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (historical mode)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger(__name__)

    api_token = os.environ.get("OPENWEATHER_API_TOKEN")
    if not api_token:
        logger.error("OPENWEATHER_API_TOKEN not set")
        sys.exit(1)

    client = create_openweather_client(api_token)
    writer = get_data_writer()

    try:
        if args.mode == "incremental":
            count = run_incremental(writer, client)
        else:
            start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
            count = run_historical(writer, client, start, end)

        update_control(source="openweather", records_ingested=count, success=True)
        logger.info(f"OpenWeather ingestion done: {count} records")
    except Exception as e:
        logger.error(f"OpenWeather ingestion failed: {e}")
        update_control(source="openweather", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
