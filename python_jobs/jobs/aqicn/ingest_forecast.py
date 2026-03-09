#!/usr/bin/env python3
"""
AQICN Forecast Ingestion Job.

This job fetches forecast data for all AQICN stations in Vietnam
and stores them in ClickHouse. Station IDs are parsed from crawl.html.
Station metadata is simultaneously upserted from the feed API response.

Usage:
    # Incremental (latest forecasts):
    python jobs/aqicn/ingest_forecast.py --mode incremental
    
    # Historical backfill:
    python jobs/aqicn/ingest_forecast.py --mode historical

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
    parse_station_ids_from_html,
    transform_forecast,
    transform_station_from_feed,
)


# Default path to crawl.html relative to project root
DEFAULT_HTML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "crawl.html"
)


def fetch_forecast_for_station(
    client,
    station_id: str
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch forecast for a specific station.
    
    Returns both forecast records and station metadata record.
    
    Args:
        client: AQICN API client
        station_id: Station ID
        
    Returns:
        Tuple of (forecast records list, station record dict)
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
            
            # Extract station info from the same response
            station_record = transform_station_from_feed(response, station_id)
            
            # Check if forecast data exists
            forecast = data.get("forecast", {})
            if not forecast:
                logger.debug(f"No forecast data for station {station_id}")
                return [], station_record
            
            # Transform the forecast
            forecasts = transform_forecast(response, station_id)
            return forecasts, station_record
        else:
            logger.warning(f"Unexpected response type for station {station_id}: {type(response)}")
            return [], {}
            
    except Exception as e:
        logger.error(f"Error fetching forecast for station {station_id}: {e}")
        return [], {}


def process_stations_batch(
    client,
    station_ids: List[str],
    max_workers: int = 4
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process a batch of stations to fetch forecasts and station info.
    
    Args:
        client: AQICN API client
        station_ids: List of station IDs to process
        max_workers: Number of parallel workers
        
    Returns:
        Tuple of (all forecast records, all station records)
    """
    logger = logging.getLogger(__name__)
    
    all_forecasts = []
    all_stations = []
    total_stations = len(station_ids)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_station = {
            executor.submit(
                fetch_forecast_for_station,
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
                forecasts, station_record = future.result()
                all_forecasts.extend(forecasts)
                if station_record:
                    all_stations.append(station_record)
                
                if completed % 20 == 0:
                    logger.info(f"Progress: {completed}/{total_stations} stations")
                    
            except Exception as e:
                logger.error(f"Error processing station {station_id}: {e}")
    
    return all_forecasts, all_stations


def run_incremental_ingestion(
    client,
    clickhouse_writer,
    html_path: str,
    max_workers: int = 4
) -> Dict[str, Any]:
    """
    Run incremental ingestion (latest forecasts only).
    
    Args:
        client: AQICN API client
        clickhouse_writer: ClickHouse writer
        html_path: Path to crawl.html
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Parse station IDs from HTML (no dependency on ClickHouse stations)
    station_ids = parse_station_ids_from_html(html_path)
    
    if not station_ids:
        logger.error("No station IDs found in HTML. Check crawl.html path.")
        return {"stations": 0, "forecasts": 0}
    
    logger.info(f"Processing {len(station_ids)} stations for forecasts")
    
    # Process all stations to get latest forecasts + station info
    forecasts, station_records = process_stations_batch(client, station_ids, max_workers)
    
    # Write station records (upsert via ReplacingMergeTree)
    stations_written = 0
    if station_records:
        stations_written = clickhouse_writer.write_batch(
            table="raw_aqicn_stations",
            records=station_records,
            source="aqicn"
        )
        logger.info(f"Upserted {stations_written} station records")
    
    # Write forecast records
    forecasts_written = 0
    if forecasts:
        forecasts_written = clickhouse_writer.write_batch(
            table="raw_aqicn_forecast",
            records=forecasts,
            source="aqicn"
        )
    
    return {
        "stations": len(station_ids),
        "stations_written": stations_written,
        "forecasts": forecasts_written,
        "timestamp": datetime.now().isoformat()
    }


def run_historical_ingestion(
    client,
    clickhouse_writer,
    html_path: str,
    days_back: int = 7,
    max_workers: int = 4
) -> Dict[str, Any]:
    """
    Run historical backfill ingestion.
    
    Note: AQICN API provides forecasts from the current day forward.
    Historical backfill simulates this by running the same query for each day.
    
    Args:
        client: AQICN API client
        clickhouse_writer: ClickHouse writer
        html_path: Path to crawl.html
        days_back: Number of days to backfill
        max_workers: Number of parallel workers
        
    Returns:
        Statistics dictionary
    """
    logger = logging.getLogger(__name__)
    
    # Parse station IDs from HTML
    station_ids = parse_station_ids_from_html(html_path)
    
    if not station_ids:
        logger.error("No station IDs found in HTML. Check crawl.html path.")
        return {"stations": 0, "forecasts": 0}
    
    logger.info(f"Processing {len(station_ids)} stations for {days_back} days historical forecasts")
    
    # For historical, we'll take snapshots for each day
    total_forecasts = 0
    
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
        
        forecasts, station_records = process_stations_batch(client, station_ids, max_workers)
        
        # Upsert station records (only on first day)
        if i == 0 and station_records:
            clickhouse_writer.write_batch(
                table="raw_aqicn_stations",
                records=station_records,
                source="aqicn"
            )
        
        if forecasts:
            # Add day info to each forecast for historical tracking
            for f in forecasts:
                f["snapshot_date"] = day.strftime('%Y-%m-%d')
            
            written = clickhouse_writer.write_batch(
                table="raw_aqicn_forecast",
                records=forecasts,
                source="aqicn"
            )
            total_forecasts += written
        
        # Log progress
        if (i + 1) % 2 == 0:
            logger.info(f"Progress: {i+1}/{len(days)} days, {total_forecasts} total forecasts")
    
    return {
        "stations": len(station_ids),
        "forecasts": total_forecasts,
        "days_processed": len(days),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest AQICN forecasts")
    parser.add_argument(
        "--mode",
        choices=["incremental", "historical"],
        default="incremental",
        help="Ingestion mode"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Number of days to backfill for historical mode"
    )
    parser.add_argument("--html-path", default=DEFAULT_HTML_PATH,
                        help="Path to crawl.html file")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_aqicn_forecast", source="aqicn", level=args.log_level) as logger:
        logger.info(f"Starting AQICN forecast ingestion in {args.mode} mode")
        
        # Get API token
        api_token = os.environ.get("AQICN_API_TOKEN")
        if not api_token:
            logger.error("AQICN_API_TOKEN not set")
            sys.exit(1)
        
        # Check HTML file exists
        if not os.path.exists(args.html_path):
            logger.error(f"HTML file not found: {args.html_path}")
            sys.exit(1)
        
        # Create clients
        aqicn_client = create_aqicn_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Run ingestion based on mode
        if args.mode == "incremental":
            stats = run_incremental_ingestion(
                aqicn_client,
                clickhouse_writer,
                html_path=args.html_path,
                max_workers=args.max_workers
            )
        else:
            stats = run_historical_ingestion(
                aqicn_client,
                clickhouse_writer,
                html_path=args.html_path,
                days_back=args.days_back,
                max_workers=args.max_workers
            )
        
        logger.info(f"Ingestion completed: {stats}")
        
        # Log statistics
        log_job_stats(logger, "ingest_aqicn_forecast", {
            **stats,
            "mode": args.mode,
            "timestamp": datetime.now().isoformat()
        })


if __name__ == "__main__":
    main()
