#!/usr/bin/env python3
"""
OpenAQ Measurements Ingestion Job.

This job fetches measurements for all sensors in Vietnam from OpenAQ API
and stores them in ClickHouse. Supports:
- Hourly incremental ingestion
- Full historical backfill
- Rate limiting (60 requests/minute)

Usage:
    # Incremental (last hour):
    python jobs/openaq/ingest_measurements.py --mode incremental
    
    # Historical backfill:
    python jobs/openaq/ingest_measurements.py --mode historical --start-date 2024-01-01

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser as date_parser

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import (
    create_openaq_client,
    create_clickhouse_writer,
    JobLogger,
    log_job_stats,
    create_openaq_limiter
)
from models.openaq_models import transform_measurement


def fetch_measurements_for_sensor(
    client,
    sensor_id: int,
    location_id: int,
    parameter_id: int,
    date_from: str,
    date_to: str,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fetch measurements for a specific sensor within a time range.
    
    Args:
        client: OpenAQ API client
        sensor_id: Sensor ID
        location_id: Location ID
        parameter_id: Parameter ID
        date_from: Start date (ISO format)
        date_to: End date (ISO format)
        limit: Number of results per page
        
    Returns:
        List of measurement records
    """
    logger = logging.getLogger(__name__)
    
    try:
        all_measurements = []
        page = 1
        
        while True:
            params = {
                "limit": limit,
                "page": page,
                "date_from": date_from,
                "date_to": date_to
            }
            
            response = client.get(f"/v3/sensors/{sensor_id}/measurements", params=params)
            
            results = response.get("results", [])
            
            if not results:
                break
            
            # Transform each measurement
            for measurement in results:
                try:
                    transformed = transform_measurement(
                        measurement,
                        location_id=location_id,
                        sensor_id=sensor_id,
                        parameter_id=parameter_id
                    )
                    all_measurements.append(transformed)
                except Exception as e:
                    logger.warning(f"Error transforming measurement: {e}")
                    continue
            
            # Check if more pages
            meta = response.get("meta", {})
            found_raw = str(meta.get("found", 0))
            # OpenAQ API may return ">1000" style strings for large result counts
            found_clean = found_raw.lstrip(">").strip()
            found = int(found_clean) if found_clean.isdigit() else 10_000
            
            if found > 0 and len(all_measurements) >= found:
                break
            
            page += 1
        
        return all_measurements
        
    except Exception as e:
        logger.error(f"Error fetching measurements for sensor {sensor_id}: {e}")
        return []


def get_sensors_from_clickhouse(clickhouse_url: str) -> List[Dict[str, Any]]:
    """
    Get sensors with their location and parameter IDs from ClickHouse.
    
    Args:
        clickhouse_url: ClickHouse URL
        
    Returns:
        List of sensor dictionaries with sensor_id, location_id, parameter_id
    """
    import requests
    
    query = """
    SELECT sensor_id, location_id, parameter_id 
    FROM raw_openaq_sensors 
    ORDER BY sensor_id
    """
    url = f"{clickhouse_url}&query={query}" if "?" in clickhouse_url else f"{clickhouse_url}?query={query}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            # Skip header
            sensors = []
            for line in lines[1:]:
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        sensors.append({
                            "sensor_id": int(parts[0]),
                            "location_id": int(parts[1]),
                            "parameter_id": int(parts[2])
                        })
            return sensors
    except Exception as e:
        logging.getLogger(__name__).error(f"Error fetching sensors: {e}")
    
    return []


def get_latest_ingestion_time(clickhouse_url: str) -> Optional[datetime]:
    """
    Get the latest ingestion time for measurements.
    
    Args:
        clickhouse_url: ClickHouse URL
        
    Returns:
        Latest ingestion datetime or None
    """
    import requests
    
    query = "SELECT max(ingest_time) FROM raw_openaq_measurements"
    url = f"{clickhouse_url}?query={query.replace(chr(10), ' ')}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            if len(lines) > 1 and lines[1].strip():
                # Parse the datetime
                dt_str = lines[1].strip()
                return date_parser.parse(dt_str)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error fetching latest ingestion time: {e}")
    
    return None


def process_hour_batch(
    client,
    sensors: List[Dict[str, Any]],
    hour_start: datetime,
    max_workers: int = 4
) -> List[Dict[str, Any]]:
    """
    Process a batch of sensors for a specific hour.
    
    Args:
        client: OpenAQ API client
        sensors: List of sensors to process
        hour_start: Start of the hour to process
        max_workers: Number of parallel workers
        
    Returns:
        List of all measurement records
    """
    logger = logging.getLogger(__name__)
    
    date_from = hour_start.strftime("%Y-%m-%dT%H:%M:%S.000000")
    date_to = (hour_start + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000000")
    
    all_measurements = []
    total_sensors = len(sensors)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sensor = {
            executor.submit(
                fetch_measurements_for_sensor,
                client,
                sensor["sensor_id"],
                sensor["location_id"],
                sensor["parameter_id"],
                date_from,
                date_to
            ): sensor["sensor_id"]
            for sensor in sensors
        }
        
        completed = 0
        for future in as_completed(future_to_sensor):
            sensor_id = future_to_sensor[future]
            completed += 1
            
            try:
                measurements = future.result()
                all_measurements.extend(measurements)
                
                if completed % 50 == 0:
                    logger.info(f"Progress: {completed}/{total_sensors} sensors for hour {date_from}")
                    
            except Exception as e:
                logger.error(f"Error processing sensor {sensor_id}: {e}")
    
    return all_measurements


def run_incremental_ingestion(
    client,
    clickhouse_writer,
    max_workers: int = 4
) -> Dict[str, int]:
    """
    Run incremental ingestion (last hour only).
    
    Args:
        client: OpenAQ API client
        clickhouse_writer: ClickHouse writer
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Get sensors from ClickHouse
    sensors = get_sensors_from_clickhouse(clickhouse_writer._url)
    
    if not sensors:
        logger.error("No sensors found in ClickHouse. Run ingest_sensors first.")
        return {"sensors": 0, "measurements": 0}
    
    logger.info(f"Processing {len(sensors)} sensors")
    
    # Determine the hour to ingest
    latest_ingestion = get_latest_ingestion_time(clickhouse_writer._url)
    
    if latest_ingestion:
        # Ingest the next hour after latest
        hour_start = latest_ingestion + timedelta(hours=1)
    else:
        # No existing data, ingest last hour
        hour_start = datetime.utcnow() - timedelta(hours=1)
    
    # Align to hour
    hour_start = hour_start.replace(minute=0, second=0, microsecond=0)
    
    logger.info(f"Ingesting measurements for hour: {hour_start}")
    
    # Process this hour
    measurements = process_hour_batch(client, sensors, hour_start, max_workers)
    
    if measurements:
        written = clickhouse_writer.write_batch(
            table="raw_openaq_measurements",
            records=measurements,
            source="openaq"
        )
    else:
        written = 0
    
    return {
        "sensors": len(sensors),
        "measurements": written,
        "hour_processed": hour_start.isoformat()
    }


def run_historical_ingestion(
    client,
    clickhouse_writer,
    start_date: datetime,
    end_date: Optional[datetime],
    max_workers: int = 4
) -> Dict[str, int]:
    """
    Run historical backfill ingestion.
    
    Args:
        client: OpenAQ API client
        clickhouse_writer: ClickHouse writer
        start_date: Start date for backfill
        end_date: End date for backfill (default: now)
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Get sensors from ClickHouse
    sensors = get_sensors_from_clickhouse(clickhouse_writer._url)
    
    if not sensors:
        logger.error("No sensors found in ClickHouse. Run ingest_sensors first.")
        return {"sensors": 0, "measurements": 0}
    
    if end_date is None:
        end_date = datetime.utcnow()
    
    logger.info(f"Processing {len(sensors)} sensors from {start_date} to {end_date}")
    
    # Calculate hours to process
    hours = []
    current = start_date
    while current < end_date:
        hours.append(current.replace(minute=0, second=0, microsecond=0))
        current += timedelta(hours=1)
    
    logger.info(f"Processing {len(hours)} hours")
    
    total_measurements = 0
    
    for i, hour_start in enumerate(hours):
        logger.info(f"Processing hour {i+1}/{len(hours)}: {hour_start}")
        
        measurements = process_hour_batch(client, sensors, hour_start, max_workers)
        
        if measurements:
            written = clickhouse_writer.write_batch(
                table="raw_openaq_measurements",
                records=measurements,
                source="openaq"
            )
            total_measurements += written
        
        # Log progress
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i+1}/{len(hours)} hours, {total_measurements} total measurements")
    
    return {
        "sensors": len(sensors),
        "measurements": total_measurements,
        "hours_processed": len(hours),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest OpenAQ measurements")
    parser.add_argument(
        "--mode",
        choices=["incremental", "historical"],
        default="incremental",
        help="Ingestion mode"
    )
    parser.add_argument(
        "--start-date",
        help="Start date for historical backfill (ISO format: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        help="End date for historical backfill (ISO format: YYYY-MM-DD)"
    )
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_openaq_measurements", source="openaq", level=args.log_level) as logger:
        logger.info(f"Starting OpenAQ measurements ingestion in {args.mode} mode")
        
        # Get API token
        api_token = os.environ.get("OPENAQ_API_TOKEN")
        if not api_token:
            logger.error("OPENAQ_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        openaq_client = create_openaq_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Run ingestion based on mode
        if args.mode == "incremental":
            stats = run_incremental_ingestion(
                openaq_client,
                clickhouse_writer,
                max_workers=args.max_workers
            )
        else:
            # Historical mode
            if not args.start_date:
                logger.error("--start-date is required for historical mode")
                sys.exit(1)
            
            start_date = date_parser.parse(args.start_date)
            end_date = date_parser.parse(args.end_date) if args.end_date else None
            
            stats = run_historical_ingestion(
                openaq_client,
                clickhouse_writer,
                start_date,
                end_date,
                max_workers=args.max_workers
            )
        
        logger.info(f"Ingestion completed: {stats}")
        
        # Log statistics
        log_job_stats(logger, "ingest_openaq_measurements", {
            **stats,
            "mode": args.mode,
            "timestamp": datetime.now().isoformat()
        })


if __name__ == "__main__":
    main()

