#!/usr/bin/env python3
"""
OpenAQ Parameters Ingestion Job.

This job fetches all available parameters from OpenAQ API and stores them
in ClickHouse for reference.

Usage:
    python jobs/openaq/ingest_parameters.py [--config config.yaml]

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
    setup_logging,
    log_job_stats,
    JobLogger
)
from models.openaq_models import transform_parameter


def fetch_parameters(client, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch all parameters from OpenAQ API.
    
    Args:
        client: OpenAQ API client
        limit: Number of results per page
        
    Returns:
        List of parameter records
    """
    logger = logging.getLogger(__name__)
    logger.info("Fetching parameters from OpenAQ API")
    
    try:
        response = client.get("/v3/parameters", params={"limit": limit})
        
        results = response.get("results", [])
        logger.info(f"Fetched {len(results)} parameters")
        
        # Transform each parameter
        transformed = [transform_parameter(p) for p in results]
        
        return transformed
        
    except Exception as e:
        logger.error(f"Error fetching parameters: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest OpenAQ parameters")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--limit", type=int, default=100, help="Max parameters to fetch")
    parser.add_argument("--mode", choices=["rewrite", "append"], default="rewrite",
                        help="Ingestion mode: rewrite (truncate+insert) or append (default: rewrite)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Setup logging
    with JobLogger("ingest_openaq_parameters", source="openaq", level=args.log_level) as logger:
        logger.info("Starting OpenAQ parameters ingestion")
        
        # Get API token
        api_token = os.environ.get("OPENAQ_API_TOKEN")
        if not api_token:
            logger.error("OPENAQ_API_TOKEN not set")
            sys.exit(1)
        
        # Create clients
        openaq_client = create_openaq_client(api_token)
        clickhouse_writer = create_clickhouse_writer()
        
        # Fetch parameters
        parameters = fetch_parameters(openaq_client, limit=args.limit)
        
        if parameters:
            # Write to ClickHouse
            if args.mode == "rewrite":
                written = clickhouse_writer.write_batch_rewrite(
                    table="raw_openaq_parameters",
                    records=parameters,
                    source="openaq"
                )
            else:
                written = clickhouse_writer.write_batch(
                    table="raw_openaq_parameters",
                    records=parameters,
                    source="openaq"
                )
            
            logger.info(f"Successfully wrote {written} parameters to ClickHouse (mode={args.mode})")
            
            # Log statistics
            log_job_stats(logger, "ingest_openaq_parameters", {
                "parameters_fetched": len(parameters),
                "parameters_written": written,
                "timestamp": datetime.now().isoformat()
            })
        else:
            logger.warning("No parameters fetched")
        
        logger.info("OpenAQ parameters ingestion completed")


if __name__ == "__main__":
    main()

