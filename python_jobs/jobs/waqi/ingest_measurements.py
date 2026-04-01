#!/usr/bin/env python3
"""
WAQI / World Air Quality Index Measurements Ingestion Job.

Fetches all Vietnam monitoring stations via one bounding-box query and stores
measurements in ClickHouse.

Usage:
    python jobs/waqi/ingest_measurements.py --mode incremental
    python jobs/waqi/ingest_measurements.py --mode historical --days-back 30

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.rate_limiter import create_waqi_limiter
from common.clickhouse_writer import create_clickhouse_writer
from common.ingestion_control import update_control
from models.waqi_models import (
    transform_waqi_feed_response,
    build_bbox_url,
)


def create_waqi_client(api_token: str) -> APIClient:
    """Create a configured WAQI API client."""
    return APIClient(
        base_url="https://api.waqi.info",
        token=api_token,
        timeout=30,
        max_retries=5,
        backoff_factor=2.0,
        rate_limiter=create_waqi_limiter(),
        auth_header_name=None,   # Token goes in query param, not header
    )


def fetch_vietnam_feed(client: APIClient) -> List[Dict[str, Any]]:
    """
    Fetch the Vietnam bounding-box feed and return measurement records.

    Makes one call: GET /feed/geo:8.4;102.1;23.4;109.5/?token={token}
    Returns all station records from the response.
    """
    logger = logging.getLogger(__name__)

    # Make request using the client — token goes in query param
    response = client.get(
        "/feed/geo:8.4;102.1;23.4;109.5/",
        params={"token": client.token}
    )

    records = transform_waqi_feed_response(response)
    logger.info(f"WAQI Vietnam bbox: {len(records)} measurement records from feed")
    return records


def run_incremental(writer) -> int:
    """Fetch + write current WAQI data for Vietnam. Returns row count."""
    logger = logging.getLogger(__name__)
    api_token = os.environ.get("WAQI_API_TOKEN")
    if not api_token:
        logger.error("WAQI_API_TOKEN not set")
        sys.exit(1)

    client = create_waqi_client(api_token)
    try:
        records = fetch_vietnam_feed(client)
    finally:
        client.close()

    if records:
        writer.write_batch("raw_waqi_measurements", records, source="waqi")
        logger.info(f"Wrote {len(records)} WAQI measurements")
    else:
        logger.warning("No WAQI records collected")

    return len(records)


def run_historical(writer, days_back: int = 30) -> int:
    """
    Historical backfill for WAQI.

    Note: WAQI free tier provides ~30 days of historical data.
    Uses the same bounding-box endpoint; historical data is limited.

    Args:
        writer: ClickHouse writer
        days_back: Number of days to backfill (default 30)

    Returns:
        Total row count written.
    """
    logger = logging.getLogger(__name__)
    logger.warning(
        "WAQI historical backfill is limited (~30 days). "
        "For full historical data use dag_ingest_historical with --days-back 30."
    )

    # WAQI doesn't have a historical endpoint; simulate by running incremental
    # for each day. In practice, WAQI bounding-box returns only latest values.
    # Log a warning and fall back to incremental.
    return run_incremental(writer)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Ingest WAQI air quality data")
    parser.add_argument(
        "--mode",
        choices=["incremental", "historical"],
        default="incremental"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days to backfill (historical mode, default 30)"
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger(__name__)

    writer = create_clickhouse_writer()

    try:
        if args.mode == "incremental":
            count = run_incremental(writer)
        else:
            count = run_historical(writer, days_back=args.days_back)

        update_control(source="waqi", records_ingested=count, success=True)
        logger.info(f"WAQI ingestion done: {count} records")
    except Exception as e:
        logger.error(f"WAQI ingestion failed: {e}")
        update_control(source="waqi", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
