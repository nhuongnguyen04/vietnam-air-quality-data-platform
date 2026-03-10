#!/usr/bin/env python3
"""
AQICN Measurements Ingestion Job.

This job fetches current measurements for all AQICN stations in Vietnam
and stores them in ClickHouse. Station IDs are parsed from crawl.html.
Station metadata is simultaneously upserted from the feed API response.

Usage:
    # Incremental (latest measurements):
    python jobs/aqicn/ingest_measurements.py --mode incremental
    
    # Historical backfill for all stations:
    python jobs/aqicn/ingest_measurements.py --mode historical

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import (
    create_aqicn_client,
    create_clickhouse_writer,
    JobLogger,
    log_job_stats,
    create_aqicn_limiter
)
from models.aqicn_models import (
    fetch_and_parse_station_ids,
    transform_measurement,
    transform_station_from_feed,
)


# Default path to crawl.html relative to project root
DEFAULT_HTML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "crawl.html"
)


def fetch_measurement_for_station(
    client,
    station_id: str
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch current measurement for a specific station.
    
    Returns both measurement records and station metadata record.
    
    Args:
        client: AQICN API client
        station_id: Station ID
        
    Returns:
        Tuple of (measurement records list, station record dict)
    """
    logger = logging.getLogger(__name__)
    
    try:
        response = client.get(
            f"/feed/@{station_id}/",
            params={
                "token": client.token
            }
        )
        
        # Check response status
        if isinstance(response, dict):
            status = response.get("status")
            if status != "ok":
                logger.warning(f"API returned status {status} for station {station_id}")
                return [], {}
            
            data = response.get("data", {})
            if not data:
                return [], {}
            
            # Transform measurements (without station info)
            measurements = transform_measurement(response, station_id)
            
            # Transform station info from the same response
            station_record = transform_station_from_feed(response, station_id)
            
            return measurements, station_record
        else:
            logger.warning(f"Unexpected response type for station {station_id}: {type(response)}")
            return [], {}
            
    except Exception as e:
        logger.error(f"Error fetching measurement for station {station_id}: {e}")
        return [], {}


def process_stations_batch(
    client,
    station_ids: List[str],
    max_workers: int = 4
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process a batch of stations to fetch measurements and station info.
    
    Args:
        client: AQICN API client
        station_ids: List of station IDs to process
        max_workers: Number of parallel workers
        
    Returns:
        Tuple of (all measurement records, all station records)
    """
    logger = logging.getLogger(__name__)
    
    all_measurements = []
    all_stations = []
    total_stations = len(station_ids)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_station = {
            executor.submit(
                fetch_measurement_for_station,
                client,
                sid
            ): sid
            for sid in station_ids
        }
        
        completed = 0
        for future in as_completed(future_to_station):
            station_id = future_to_station[future]
            completed += 1
            
            try:
                measurements, station_record = future.result()
                all_measurements.extend(measurements)
                if station_record:
                    all_stations.append(station_record)
                
                if completed % 20 == 0:
                    logger.info(f"Progress: {completed}/{total_stations} stations")
                    
            except Exception as e:
                logger.error(f"Error processing station {station_id}: {e}")
    
    return all_measurements, all_stations


def run_incremental_ingestion(
    client,
    clickhouse_writer,
    max_workers: int = 4
) -> Dict[str, Any]:
    """
    Run incremental ingestion (latest measurements only).
    
    Args:
        client: AQICN API client
        clickhouse_writer: ClickHouse writer
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Parse station IDs from HTML (no dependency on ClickHouse stations)
    station_ids = fetch_and_parse_station_ids()
    
    if not station_ids:
        logger.error("No station IDs found in HTML. Check AQICN website.")
        return {"stations": 0, "measurements": 0}
    
    logger.info(f"Processing {len(station_ids)} stations")
    
    # Process all stations to get latest measurements + station info
    measurements, station_records = process_stations_batch(client, station_ids, max_workers)
    
    # Write station records (upsert via ReplacingMergeTree)
    stations_written = 0
    if station_records:
        stations_written = clickhouse_writer.write_batch(
            table="raw_aqicn_stations",
            records=station_records,
            source="aqicn"
        )
        logger.info(f"Upserted {stations_written} station records")
    
    # Write measurement records
    measurements_written = 0
    if measurements:
        measurements_written = clickhouse_writer.write_batch(
            table="raw_aqicn_measurements",
            records=measurements,
            source="aqicn"
        )
    
    return {
        "stations": len(station_ids),
        "stations_written": stations_written,
        "measurements": measurements_written,
        "timestamp": datetime.now().isoformat()
    }


def run_historical_ingestion(
    client,
    clickhouse_writer,
    html_path: str,
    days_back: int = 30,
    max_workers: int = 4
) -> Dict[str, Any]:
    """
    Run historical backfill ingestion.
    
    Note: AQICN API only provides current measurements via /feed endpoint.
    Historical data would need a different approach (e.g., storing daily snapshots).
    
    Args:
        client: AQICN API client
        clickhouse_writer: ClickHouse writer
        days_back: Number of days to backfill (stores daily snapshots)
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Parse station IDs from HTML
    station_ids = fetch_and_parse_station_ids()
    
    if not station_ids:
        logger.error("No station IDs found in HTML. Check AQICN website.")
        return {"stations": 0, "measurements": 0}
    
    logger.info(f"Processing {len(station_ids)} stations for {days_back} days historical")
    
    # For historical, we'll take snapshots for each day
    total_measurements = 0
    
    # Calculate dates to backfill
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)
    
    days = []
    current = start_date
    while current < end_date:
        days.append(current)
        current += timedelta(days=1)
    
    logger.info(f"Processing {len(days)} days")
    
    for i, day in enumerate(days):
        logger.info(f"Processing day {i+1}/{len(days)}: {day.strftime('%Y-%m-%d')}")
        
        measurements, station_records = process_stations_batch(client, station_ids, max_workers)
        
        # Upsert station records (only on first day to avoid redundant writes)
        if i == 0 and station_records:
            clickhouse_writer.write_batch(
                table="raw_aqicn_stations",
                records=station_records,
                source="aqicn"
            )
        
        if measurements:
            # Add day info to each measurement for historical tracking
            for m in measurements:
                m["snapshot_date"] = day.strftime('%Y-%m-%d')
            
            written = clickhouse_writer.write_batch(
                table="raw_aqicn_measurements",
                records=measurements,
                source="aqicn"
            )
            total_measurements += written
        
        # Log progress
        if (i + 1) % 5 == 0:
            logger.info(f"Progress: {i+1}/{len(days)} days, {total_measurements} total measurements")
    
    return {
        "stations": len(station_ids),
        "measurements": total_measurements,
        "days_processed": len(days),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest AQICN measurements")
    parser.add_argument(
        "--mode",
        choices=["incremental", "historical"],
        default="incremental",
        help="Ingestion mode"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days to backfill for historical mode"
    )
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_aqicn_measurements", source="aqicn", level=args.log_level) as logger:
        logger.info(f"Starting AQICN measurements ingestion in {args.mode} mode")
        
        # Get API token
        api_token = os.environ.get("AQICN_API_TOKEN")
        if not api_token:
            logger.error("AQICN_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        aqicn_client = create_aqicn_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Run ingestion based on mode
        if args.mode == "incremental":
            stats = run_incremental_ingestion(
                aqicn_client,
                clickhouse_writer,
                max_workers=args.max_workers
            )
        else:
            stats = run_historical_ingestion(
                aqicn_client,
                clickhouse_writer,
                days_back=args.days_back,
                max_workers=args.max_workers
            )
        
        logger.info(f"Ingestion completed: {stats}")
        
        # Log statistics
        log_job_stats(logger, "ingest_aqicn_measurements", {
            **stats,
            "mode": args.mode,
            "timestamp": datetime.now().isoformat()
        })


if __name__ == "__main__":
    main()
