#!/usr/bin/env python3
"""
OpenAQ Locations Ingestion Job.

This job fetches all locations in Vietnam (countries_id=56) from OpenAQ API
and stores them in ClickHouse.

Usage:
    python jobs/openaq/ingest_locations.py [--countries_id 56] [--limit 1000]

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import (
    create_openaq_client,
    create_clickhouse_writer,
    JobLogger,
    log_job_stats
)
from models.openaq_models import transform_location


def fetch_locations(client, countries_id: int = 56, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch all locations for a countries from OpenAQ API.
    
    Args:
        client: OpenAQ API client
        countries_id: countries ID to filter by
        limit: Number of results per page
        
    Returns:
        List of location records
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching locations for countries: {countries_id}")
    
    try:
        all_locations = []
        page = 1
        
        while True:
            params = {
                "countries_id": countries_id,
                "limit": limit,
                "page": page
            }
            
            response = client.get("/v3/locations", params=params)
            
            results = response.get("results", [])
            
            if not results:
                break
            
            logger.info(f"Fetched page {page}: {len(results)} locations")
            
            # Transform each location
            for loc in results:
                try:
                    transformed = transform_location(loc)
                    all_locations.append(transformed)
                except Exception as e:
                    logger.warning(f"Error transforming location: {e}")
                    continue
            
            # Check if more pages
            meta = response.get("meta", {})
            found_val = meta.get("found", 0)
            
            try:
                found = int(found_val) if found_val is not None else 0
            except (ValueError, TypeError):
                found = 0
            
            if found > 0 and len(all_locations) >= found:
                break
            
            page += 1
        
        logger.info(f"Total locations fetched: {len(all_locations)}")
        return all_locations
        
    except Exception as e:
        logger.error(f"Error fetching locations: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest OpenAQ locations")
    parser.add_argument("--countries_id", type=int, default=56, help="Country code to filter by")
    parser.add_argument("--limit", type=int, default=1000, help="Results per page")
    parser.add_argument("--mode", choices=["rewrite", "append"], default="rewrite",
                        help="Ingestion mode: rewrite (truncate+insert) or append (default: rewrite)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_openaq_locations", source="openaq", level=args.log_level) as logger:
        logger.info(f"Starting OpenAQ locations ingestion for {args.countries_id}")
        
        # Get API token
        api_token = os.environ.get("OPENAQ_API_TOKEN")
        if not api_token:
            logger.error("OPENAQ_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        openaq_client = create_openaq_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Fetch locations
        locations = fetch_locations(openaq_client, countries_id=args.countries_id, limit=args.limit)
        
        if locations:
            # Write to ClickHouse
            if args.mode == "rewrite":
                written = clickhouse_writer.write_batch_rewrite(
                    table="raw_openaq_locations",
                    records=locations,
                    source="openaq"
                )
            else:
                written = clickhouse_writer.write_batch(
                    table="raw_openaq_locations",
                    records=locations,
                    source="openaq"
                )
            
            logger.info(f"Successfully wrote {written} locations to ClickHouse (mode={args.mode})")
            
            # Log statistics
            log_job_stats(logger, "ingest_openaq_locations", {
                "locations_fetched": len(locations),
                "locations_written": written,
                "countries": args.countries_id,
                "timestamp": datetime.now().isoformat()
            })
        else:
            logger.warning("No locations fetched")
        
        logger.info("OpenAQ locations ingestion completed")


if __name__ == "__main__":
    main()

