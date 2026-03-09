#!/usr/bin/env python3
"""
OpenAQ Sensors Ingestion Job.

This job fetches all sensors for locations in Vietnam from OpenAQ API
and stores them in ClickHouse.

Usage:
    python jobs/openaq/ingest_sensors.py [--location-ids 1,2,3]

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import (
    create_openaq_client,
    create_clickhouse_writer,
    JobLogger,
    log_job_stats,
    create_openaq_limiter
)
from models.openaq_models import transform_sensor


def fetch_sensors_for_location(
    client,
    location_id: int,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fetch all sensors for a specific location.
    
    Args:
        client: OpenAQ API client
        location_id: Location ID
        limit: Number of results per page
        
    Returns:
        List of sensor records
    """
    logger = logging.getLogger(__name__)
    
    try:
        all_sensors = []
        page = 1
        
        while True:
            params = {
                "limit": limit,
                "page": page
            }
            
            response = client.get(f"/v3/locations/{location_id}/sensors", params=params)
            
            results = response.get("results", [])
            
            if not results:
                break
            
            # Transform each sensor
            for sensor in results:
                try:
                    transformed = transform_sensor(sensor, location_id)
                    all_sensors.append(transformed)
                except Exception as e:
                    logger.warning(f"Error transforming sensor: {e}")
                    continue
            
            # Check if more pages
            meta = response.get("meta", {})
            found = int(meta.get("found", 0))
            
            if found > 0 and len(all_sensors) >= found:
                break
            
            page += 1
        
        return all_sensors
        
    except Exception as e:
        logger.error(f"Error fetching sensors for location {location_id}: {e}")
        return []


def fetch_all_sensors(
    client,
    location_ids: List[int],
    max_workers: int = 4
) -> List[Dict[str, Any]]:
    """
    Fetch sensors for multiple locations in parallel.
    
    Args:
        client: OpenAQ API client
        location_ids: List of location IDs
        max_workers: Number of parallel workers
        
    Returns:
        List of all sensor records
    """
    logger = logging.getLogger(__name__)
    
    all_sensors = []
    total_locations = len(location_ids)
    
    logger.info(f"Fetching sensors for {total_locations} locations with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_location = {
            executor.submit(fetch_sensors_for_location, client, loc_id): loc_id
            for loc_id in location_ids
        }
        
        completed = 0
        for future in as_completed(future_to_location):
            loc_id = future_to_location[future]
            completed += 1
            
            try:
                sensors = future.result()
                all_sensors.extend(sensors)
                
                if completed % 10 == 0:
                    logger.info(f"Progress: {completed}/{total_locations} locations, "
                              f"{len(all_sensors)} sensors total")
                    
            except Exception as e:
                logger.error(f"Error processing location {loc_id}: {e}")
    
    logger.info(f"Total sensors fetched: {len(all_sensors)}")
    return all_sensors


def get_location_ids_from_clickhouse(clickhouse_host: str) -> List[int]:
    """
    Get location IDs from ClickHouse.
    
    Args:
        clickhouse_host: ClickHouse host URL
        
    Returns:
        List of location IDs
    """
    import requests
    
    # Simple query to get distinct location IDs
    query = "SELECT DISTINCT location_id FROM raw_openaq_locations ORDER BY location_id"
    url = f"{clickhouse_host}&query={query}" if "?" in clickhouse_host else f"{clickhouse_host}?query={query}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            # Parse IDs directly as ClickHouse returns TabSeparated without headers by default
            return [int(line) for line in lines if line.strip()]
    except Exception as e:
        logging.getLogger(__name__).error(f"Error fetching location IDs: {e}")
    
    return []


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest OpenAQ sensors")
    parser.add_argument(
        "--location-ids",
        help="Comma-separated list of location IDs (if not provided, fetch from ClickHouse)"
    )
    parser.add_argument("--limit", type=int, default=1000, help="Results per page")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--mode", choices=["rewrite", "append"], default="rewrite",
                        help="Ingestion mode: rewrite (truncate+insert) or append (default: rewrite)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_openaq_sensors", source="openaq", level=args.log_level) as logger:
        logger.info("Starting OpenAQ sensors ingestion")
        
        # Get API token
        api_token = os.environ.get("OPENAQ_API_TOKEN")
        if not api_token:
            logger.error("OPENAQ_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        openaq_client = create_openaq_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Get location IDs
        if args.location_ids:
            location_ids = [int(x.strip()) for x in args.location_ids.split(",")]
            logger.info(f"Using provided location IDs: {location_ids}")
        else:
            logger.info("Fetching location IDs from ClickHouse")
            # Use rate limiter to fetch location IDs
            rate_limiter = create_openaq_limiter()
            rate_limiter.acquire()
            
            location_ids = get_location_ids_from_clickhouse(clickhouse_writer._url)
            
            if not location_ids:
                logger.error("No location IDs found. Please provide location IDs or run ingest_locations first.")
                sys.exit(1)
            
            logger.info(f"Found {len(location_ids)} locations in ClickHouse")
        
        # Fetch sensors
        sensors = fetch_all_sensors(
            openaq_client,
            location_ids,
            max_workers=args.max_workers
        )
        
        if sensors:
            # Write to ClickHouse
            if args.mode == "rewrite":
                written = clickhouse_writer.write_batch_rewrite(
                    table="raw_openaq_sensors",
                    records=sensors,
                    source="openaq"
                )
            else:
                written = clickhouse_writer.write_batch(
                    table="raw_openaq_sensors",
                    records=sensors,
                    source="openaq"
                )
            
            logger.info(f"Successfully wrote {written} sensors to ClickHouse (mode={args.mode})")
            
            # Log statistics
            log_job_stats(logger, "ingest_openaq_sensors", {
                "sensors_fetched": len(sensors),
                "sensors_written": written,
                "locations_processed": len(location_ids),
                "timestamp": datetime.now().isoformat()
            })
        else:
            logger.warning("No sensors fetched")
        
        logger.info("OpenAQ sensors ingestion completed")


if __name__ == "__main__":
    main()

