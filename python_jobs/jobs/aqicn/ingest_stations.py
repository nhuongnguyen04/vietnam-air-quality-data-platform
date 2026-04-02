#!/usr/bin/env python3
"""
AQICN Stations Ingestion Job.

This job fetches all AQICN monitoring stations for Vietnam by:
1. Parsing station IDs from crawl.html (https://aqicn.org/city/vietnam/)
2. Fetching station details from the feed API for each ID
3. Storing station metadata in ClickHouse

Usage:
    python jobs/aqicn/ingest_stations.py
    python jobs/aqicn/ingest_stations.py --html-path /path/to/crawl.html

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any
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
from models.aqicn_models import fetch_and_parse_station_ids, transform_station_from_feed


def fetch_station_from_feed(client, station_id: str) -> Dict[str, Any]:
    """
    Fetch station details from feed API and transform to station record.
    
    Args:
        client: AQICN API client
        station_id: Station ID
        
    Returns:
        Transformed station record, or empty dict on failure
    """
    logger = logging.getLogger(__name__)
    
    try:
        response = client.get(
            f"/feed/@{station_id}/",
            params={
                "token": client.token
            }
        )
        
        if isinstance(response, dict):
            status = response.get("status")
            if status == "ok":
                return transform_station_from_feed(response, station_id)
            else:
                logger.warning(f"API returned status '{status}' for station {station_id}")
        
    except Exception as e:
        logger.error(f"Error fetching details for station {station_id}: {e}")
    
    return {}


def run_station_ingestion(
    client,
    clickhouse_writer,
    max_workers: int = 4,
    mode: str = "rewrite"
) -> Dict[str, Any]:
    """
    Run station ingestion job.

    Args:
        client: AQICN API client
        clickhouse_writer: ClickHouse writer
        max_workers: Number of parallel workers
        mode: Ingestion mode (rewrite or append)

    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Fetch and parse station IDs from AQICN
    logger.info("Fetching station IDs from AQICN website")
    station_ids = fetch_and_parse_station_ids()
    
    if not station_ids:
        logger.warning("No station IDs found in HTML")
        return {"total": 0, "fetched": 0, "written": 0}
    
    logger.info(f"Found {len(station_ids)} station IDs from HTML")
    
    # Fetch station details from feed API in parallel
    all_records = []
    errors = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(fetch_station_from_feed, client, sid): sid
            for sid in station_ids
        }
        
        completed = 0
        for future in as_completed(future_to_id):
            station_id = future_to_id[future]
            completed += 1
            
            try:
                record = future.result()
                if record:
                    all_records.append(record)
                else:
                    errors += 1
                    
                if completed % 20 == 0:
                    logger.info(f"Progress: {completed}/{len(station_ids)} stations")
                    
            except Exception as e:
                logger.error(f"Error processing station {station_id}: {e}")
                errors += 1
    
    logger.info(f"Fetched {len(all_records)} station records ({errors} errors)")
    
    # Write to ClickHouse
    if all_records:
        if mode == "rewrite":
            written = clickhouse_writer.write_batch_rewrite(
                table="raw_aqicn_stations",
                records=all_records,
                source="aqicn"
            )
        else:
            written = clickhouse_writer.write_batch(
                table="raw_aqicn_stations",
                records=all_records,
                source="aqicn"
            )
    else:
        written = 0
    
    return {
        "total": len(station_ids),
        "fetched": len(all_records),
        "errors": errors,
        "written": written
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest AQICN stations")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--mode", choices=["rewrite", "append"], default="rewrite",
                        help="Ingestion mode: rewrite (truncate+insert) or append (default: rewrite)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_aqicn_stations", source="aqicn", level=args.log_level) as logger:
        logger.info("Starting AQICN stations ingestion")
        
        # Get API token
        api_token = os.environ.get("AQICN_API_TOKEN")
        if not api_token:
            logger.error("AQICN_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        aqicn_client = create_aqicn_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Run ingestion
        stats = run_station_ingestion(
            aqicn_client,
            clickhouse_writer,
            max_workers=args.max_workers,
            mode=args.mode
        )
        
        logger.info(f"Ingestion completed: {stats}")
        
        # Log statistics
        from datetime import datetime
        log_job_stats(logger, "ingest_aqicn_stations", {
            **stats,
            "timestamp": datetime.now().isoformat()
        })


if __name__ == "__main__":
    main()
