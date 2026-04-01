#!/usr/bin/env python3
"""
Sensors.Community Measurements Ingestion Job.

Polls https://api.sensor.community/v1/feeds/ with Vietnam bounding-box filter
(lat=16.0, latDelta=7.5, lng=105.0, lngDelta=7.0) and stores readings in ClickHouse.

No authentication required. All data inserted with quality_flag.

Usage:
    python jobs/sensorscm/ingest_measurements.py --mode incremental

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.clickhouse_writer import create_clickhouse_writer
from common.ingestion_control import update_control
from models.sensorscm_models import (
    VIETNAM_BBOX_CENTER,
    transform_sensorscm_response,
)


def create_sensorscm_client() -> APIClient:
    """Create a configured Sensors.Community API client (no auth required)."""
    return APIClient(
        base_url="https://api.sensor.community",
        token=None,
        timeout=30,
        max_retries=5,
        backoff_factor=2.0,
        auth_header_name=None,
    )


def fetch_vietnam_sensors() -> List[Dict[str, Any]]:
    """
    Fetch all sensor readings within the Vietnam bounding box.

    API: GET /v1/feeds/?lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0

    Returns:
        List of station records (no pagination — single call returns all).
    """
    logger = logging.getLogger(__name__)

    client = create_sensorscm_client()
    try:
        params = {
            "lat": VIETNAM_BBOX_CENTER["lat"],
            "latDelta": VIETNAM_BBOX_CENTER["latDelta"],
            "lng": VIETNAM_BBOX_CENTER["lng"],
            "lngDelta": VIETNAM_BBOX_CENTER["lngDelta"],
        }
        response = client.get("/v1/feeds/", params=params)

        if not isinstance(response, list):
            logger.warning(
                "Unexpected response type: %s, expected list",
                type(response),
            )
            return []

        logger.info("Sensors.Community: %d stations returned", len(response))
        return response
    finally:
        client.close()


def run_incremental() -> int:
    """Fetch + write current sensor readings for Vietnam. Returns row count."""
    logger = logging.getLogger(__name__)

    stations = fetch_vietnam_sensors()
    if not stations:
        logger.warning(
            "No Sensors.Community stations returned for Vietnam bbox"
        )
        return 0

    records = transform_sensorscm_response(stations)
    if not records:
        logger.warning(
            "No Sensors.Community measurement records after transform"
        )
        return 0

    writer = create_clickhouse_writer()
    writer.write_batch("raw_sensorscm_measurements", records, source="sensorscm")
    logger.info("Wrote %d Sensors.Community measurements", len(records))

    return len(records)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest Sensors.Community air quality data"
    )
    parser.add_argument("--mode", choices=["incremental"], default="incremental")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    count = 0
    try:
        if args.mode == "incremental":
            count = run_incremental()
        update_control(
            source="sensorscm",
            records_ingested=count,
            success=True,
        )
        logger.info("Sensors.Community ingestion done: %d records", count)
    except Exception as e:
        logger.error("Sensors.Community ingestion failed: %s", e)
        update_control(
            source="sensorscm",
            records_ingested=0,
            success=False,
            error_message=str(e),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
