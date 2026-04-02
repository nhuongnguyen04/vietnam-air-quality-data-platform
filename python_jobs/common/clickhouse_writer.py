"""
ClickHouse Writer Module - Batch writer for ClickHouse database.

This module provides a ClickHouseWriter class for efficient batch inserts
using JDBC connection. It handles:
- Batch inserts with configurable batch size
- Upsert operations using ReplacingMergeTree
- Error handling and retry logic
- Logging

Author: Air Quality Data Platform
"""

import logging
import os
from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


class ClickHouseWriter:
    """
    ClickHouse batch writer using JDBC.
    
    Features:
    - Configurable batch sizes
    - Automatic retry on failure
    - Support for upsert operations
    - Progress logging
    
    Usage:
        writer = ClickHouseWriter(host="clickhouse", port=8123, database="airquality")
        writer.write_batch("raw_openaq_measurements", records)
    """
    
    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        database: str = "airquality",
        user: str = "admin",
        password: str = "admin",
        batch_size: int = 1000,
        max_retries: int = 3
    ):
        """
        Initialize the ClickHouse writer.
        
        Args:
            host: ClickHouse host
            port: ClickHouse HTTP port
            database: Database name
            user: Username
            password: Password
            batch_size: Number of records per batch insert
            max_retries: Maximum retry attempts on failure
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        self._url = f"http://{user}:{password}@{host}:{port}/?database={database}"
        
        logger.info(
            f"ClickHouseWriter initialized: host={host}, database={database}, "
            f"batch_size={batch_size}"
        )
    
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for this ingestion run."""
        return f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def _prepare_records(
        self,
        records: List[Dict[str, Any]],
        table: str,
        batch_id: str
    ) -> List[Dict[str, Any]]:
        """
        Prepare records with metadata fields.
        
        Adds standard metadata fields:
        - source: Data source identifier
        - ingest_time: Timestamp of ingestion
        - ingest_batch_id: Unique batch identifier
        """
        prepared = []
        
        for record in records:
            prepared_record = record.copy()
            
            # Add metadata if not present
            if "source" not in prepared_record:
                # Infer source from table name
                if "openaq" in table.lower():
                    prepared_record["source"] = "openaq"
                elif "aqicn" in table.lower():
                    prepared_record["source"] = "aqicn"
            
            if "ingest_time" not in prepared_record:
                prepared_record["ingest_time"] = datetime.now()
            
            if "ingest_batch_id" not in prepared_record:
                prepared_record["ingest_batch_id"] = batch_id
            
            prepared.append(prepared_record)
        
        return prepared
    
    def _convert_value(self, value: Any) -> str:
        """
        Convert Python value to ClickHouse-compatible format.
        
        Handles:
        - None/NoneType -> NULL
        - bool -> 0/1
        - datetime -> string in ClickHouse format
        - dict/list -> JSON string
        - float -> string to preserve precision
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, dict):
            import json
            return f"'{json.dumps(value).replace(chr(39), chr(39)+chr(39))}'"
        elif isinstance(value, list):
            import json
            return f"'{json.dumps(value).replace(chr(39), chr(39)+chr(39))}'"
        else:
            # String - escape single quotes
            return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"
    
    def _build_insert_query(
        self,
        table: str,
        records: List[Dict[str, Any]]
    ) -> str:
        """Build INSERT query for ClickHouse."""
        if not records:
            return ""
        
        # Get all unique columns
        columns = list(records[0].keys())
        
        # Build column list
        columns_str = ", ".join(columns)
        
        # Build values
        values_parts = []
        for record in records:
            values = [self._convert_value(record.get(col)) for col in columns]
            values_parts.append(f"({', '.join(values)})")
        
        values_str = ",\n".join(values_parts)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES\n{values_str}"
        
        return query
    
    def _execute_query(self, query: str) -> bool:
        """Execute a query against ClickHouse."""
        import requests
        
        headers = {"Content-Type": "text/plain"}
        
        response = requests.post(
            self._url,
            data=query.encode("utf-8"),
            headers=headers,
            timeout=60
        )
        
        if response.status_code != 200:
            logger.error(f"ClickHouse error: {response.text}")
            response.raise_for_status()
        
        return True
    
    def write_batch(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        """
        Write a batch of records to ClickHouse.
        
        Args:
            table: Table name
            records: List of records to insert
            source: Optional source identifier (overrides auto-detection)
            
        Returns:
            Number of records written
            
        Raises:
            Exception: On failure after all retries
        """
        if not records:
            logger.warning(f"No records to write to {table}")
            return 0
        
        batch_id = self._generate_batch_id()
        
        # Prepare records with metadata
        prepared_records = self._prepare_records(records, table, batch_id)
        
        total_written = 0
        total_batches = (len(prepared_records) + self.batch_size - 1) // self.batch_size
        
        logger.info(
            f"Writing {len(prepared_records)} records to {table} "
            f"in {total_batches} batches (batch_id={batch_id})"
        )
        
        for i in range(0, len(prepared_records), self.batch_size):
            batch = prepared_records[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            for attempt in range(self.max_retries):
                try:
                    query = self._build_insert_query(table, batch)
                    self._execute_query(query)
                    
                    total_written += len(batch)
                    logger.debug(
                        f"Batch {batch_num}/{total_batches} written "
                        f"({total_written}/{len(prepared_records)} records)"
                    )
                    break
                    
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"Error writing batch {batch_num} (attempt {attempt + 1}): {e}. "
                            f"Retrying..."
                        )
                    else:
                        logger.error(
                            f"Failed to write batch {batch_num} after "
                            f"{self.max_retries} attempts: {e}"
                        )
                        raise
        
        logger.info(f"Successfully wrote {total_written} records to {table}")
        return total_written

    def truncate_table(self, table: str) -> bool:
        """
        Truncate a ClickHouse table (delete all rows).
        
        Args:
            table: Table name to truncate
            
        Returns:
            True if successful
            
        Raises:
            Exception: On failure
        """
        query = f"TRUNCATE TABLE {table}"
        logger.info(f"Truncating table {table}")
        self._execute_query(query)
        logger.info(f"Table {table} truncated successfully")
        return True

    def write_batch_rewrite(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        """
        Rewrite a table: truncate all existing data, then insert fresh records.
        
        Use this for metadata/reference tables that should be fully refreshed
        on each ingestion run (e.g., parameters, locations, sensors, stations).
        
        Args:
            table: Table name
            records: List of records to insert
            source: Optional source identifier
            
        Returns:
            Number of records written
        """
        if not records:
            logger.warning(f"No records to write to {table}, skipping truncate")
            return 0

        self.truncate_table(table)
        return self.write_batch(table, records, source)
    
    def write_with_deduplication(
        self,
        table: str,
        records: List[Dict[str, Any]],
        dedup_key: str = "ingest_batch_id"
    ) -> int:
        """
        Write records with deduplication using ReplacingMergeTree.
        
        For tables with ReplacingMergeTree engine, this ensures only
        the latest version of each record is kept.
        
        Args:
            table: Table name
            records: List of records to insert
            dedup_key: Column used for deduplication
            
        Returns:
            Number of records written
        """
        return self.write_batch(table, records)
    
    def check_table_exists(self, table: str) -> bool:
        """
        Check if a table exists in ClickHouse.
        
        Args:
            table: Table name
            
        Returns:
            True if table exists
        """
        query = f"EXISTS TABLE {table}"
        
        try:
            response = requests.get(
                f"{self._url}?query={query}",
                timeout=10
            )
            
            if response.status_code == 200:
                # Parse response - format is "0\n" (doesn't exist) or "1\n" (exists)
                return response.text.strip() == "1"
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
        
        return False
    
    def get_table_count(self, table: str, where: Optional[str] = None) -> int:
        """
        Get row count for a table.
        
        Args:
            table: Table name
            where: Optional WHERE clause
            
        Returns:
            Number of rows
        """
        query = f"SELECT count() FROM {table}"
        if where:
            query += f" WHERE {where}"
        
        try:
            response = requests.get(
                f"{self._url}?query={query}",
                timeout=10
            )
            
            if response.status_code == 200:
                return int(response.text.strip())
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
        
        return 0


# Factory function
def create_clickhouse_writer(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    batch_size: int = 1000
) -> ClickHouseWriter:
    """
    Create a ClickHouse writer using environment variables.
    
    Reads configuration from environment variables:
    - CLICKHOUSE_HOST
    - CLICKHOUSE_PORT
    - CLICKHOUSE_DB
    - CLICKHOUSE_USER
    - CLICKHOUSE_PASSWORD
    
    Args:
        host: Override host
        port: Override port
        database: Override database
        batch_size: Batch size for writes
        
    Returns:
        Configured ClickHouseWriter
    """
    return ClickHouseWriter(
        host=host or os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=port or int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        database=database or os.environ.get("CLICKHOUSE_DB", "airquality"),
        user=os.environ.get("CLICKHOUSE_USER", "admin"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "admin"),
        batch_size=batch_size
    )

