"""
Writer Factory Module - Centralized factory for creating data writers.
"""

import os
from typing import Optional
from .base_writer import DataWriter
from .csv_writer import CSVWriter
from .clickhouse_writer import ClickHouseWriter

def get_data_writer(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    batch_size: int = 1000
) -> DataWriter:
    """
    Factory function to create a writer based on environment mode.
    
    Args:
        host: ClickHouse host
        port: ClickHouse port
        database: ClickHouse database
        batch_size: Number of records per batch
        
    Returns:
        An implementation of DataWriter (CSVWriter or ClickHouseWriter)
    """
    mode = os.environ.get("INGEST_MODE", "clickhouse").lower()
    
    if mode == "csv":
        output_dir = os.environ.get("CSV_OUTPUT_DIR", "landing_zone")
        return CSVWriter(output_dir=output_dir)
    else:
        return ClickHouseWriter(
            host=host or os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=port or int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            database=database or os.environ.get("CLICKHOUSE_DB", "airquality"),
            user=os.environ.get("CLICKHOUSE_USER", "admin"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "admin"),
            batch_size=batch_size
        )

# For backward compatibility during migration
create_clickhouse_writer = get_data_writer
