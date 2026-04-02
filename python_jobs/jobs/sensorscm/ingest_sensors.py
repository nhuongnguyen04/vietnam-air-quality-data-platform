#!/usr/bin/env python3
"""
Sensors.Community Sensor Metadata Ingestion Job.

Extracts sensor/station metadata from https://data.sensor.community/static/v2/data.json
for Vietnam sensors (country='VN') and stores in raw_sensorscm_sensors.

Usage:
    python jobs/sensorscm/ingest_sensors.py --mode rewrite

Author: Air Quality Data Platform
"""

import logging
from typing import Dict, Any, List

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.clickhouse_writer import create_clickhouse_writer


def create_sensorscm_client() -> APIClient:
    """Create a configured Sensors.Community API client (no auth required)."""
    return APIClient(
        base_url="https://data.sensor.community",
        token=None,
        timeout=60,
        max_retries=5,
        backoff_factor=2.0,
        auth_header_name=None,
    )


def fetch_vietnam_sensors() -> List[Dict[str, Any]]:
    """
    Fetch all sensor metadata for Vietnam from data.sensor.community.

    API: GET https://data.sensor.community/static/v2/data.json
    Returns all sensors globally; Vietnam sensors filtered by country='VN'.

    Returns:
        List of unique sensor metadata records.
    """
    logger = logging.getLogger(__name__)

    client = create_sensorscm_client()
    try:
        response = client.get("/static/v2/data.json")

        if not isinstance(response, list):
            logger.warning(
                "Unexpected response type: %s, expected list",
                type(response),
            )
            return []

        # Extract unique sensors from Vietnam records
        seen: set = set()
        sensors: List[Dict[str, Any]] = []

        for record in response:
            location = record.get("location", {})
            if location.get("country") != "VN":
                continue

            sensor_info = record.get("sensor", {})
            sensor_id = sensor_info.get("id")
            if not sensor_id:
                continue

            # Deduplicate on sensor_id
            if sensor_id in seen:
                continue
            seen.add(sensor_id)

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

            sensors.append({
                "sensor_id": sensor_id,
                "station_id": location.get("id"),
                "latitude": lat,
                "longitude": lon,
                "sensor_type": sensor_info.get("sensor_type", {}).get("name", "unknown"),
                "is_indoor": bool(location.get("indoor", 0)),
                "raw_payload": str(record),
            })

        logger.info(
            "Sensors.Community: %d unique Vietnam sensors extracted",
            len(sensors),
        )
        return sensors
    finally:
        client.close()


def run_ingest(mode: str = "rewrite") -> int:
    """
    Fetch + write sensor metadata for Vietnam.

    Args:
        mode: Ingestion mode. "rewrite" truncates table before insert;
              anything else appends (leverages ReplacingMergeTree dedup).

    Returns:
        Row count written.
    """
    logger = logging.getLogger(__name__)

    sensors = fetch_vietnam_sensors()
    if not sensors:
        logger.warning(
            "No Sensors.Community sensors found for Vietnam"
        )
        return 0

    writer = create_clickhouse_writer()
    if mode == "rewrite":
        written = writer.write_batch_rewrite(
            "raw_sensorscm_sensors", sensors, source="sensorscm"
        )
    else:
        written = writer.write_batch(
            "raw_sensorscm_sensors", sensors, source="sensorscm"
        )
    logger.info("Wrote %d Sensors.Community sensor records", written)

    return written


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest Sensors.Community sensor metadata"
    )
    parser.add_argument("--mode", choices=["rewrite", "append"], default="rewrite",
                        help="rewrite (truncate+insert) or append (default: rewrite)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    count = 0
    try:
        count = run_ingest(mode=args.mode)
        logger.info("Sensors.Community sensors ingest done: %d records", count)
    except Exception as e:
        logger.error("Sensors.Community sensors ingest failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
