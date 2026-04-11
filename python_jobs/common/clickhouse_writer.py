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
import csv
from typing import List, Dict, Any, Optional, Protocol
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


class DataWriter(Protocol):
    """Protocol defining the interface for data writers."""
    
    def write_batch(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        ...
        
    def write_batch_rewrite(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        ...


class ClickHouseWriter:
    """
    ClickHouse batch writer using JDBC.
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
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        self._url = f"http://{user}:{password}@{host}:{port}/?database={database}"
        
        logger.info(
            f"ClickHouseWriter initialized: host={host}, database={database}"
        )
    
    def _generate_batch_id(self) -> str:
        return f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def _prepare_records(
        self,
        records: List[Dict[str, Any]],
        table: str,
        batch_id: str
    ) -> List[Dict[str, Any]]:
        prepared = []
        for record in records:
            prepared_record = record.copy()
            if "source" not in prepared_record:
                if "openaq" in table.lower():
                    prepared_record["source"] = "openaq"
                elif "aqicn" in table.lower():
                    prepared_record["source"] = "aqicn"
                elif "tomtom" in table.lower():
                    prepared_record["source"] = "tomtom"
                elif "openweather" in table.lower():
                    prepared_record["source"] = "openweather"
            
            if "ingest_time" not in prepared_record:
                prepared_record["ingest_time"] = datetime.now()
            
            if "ingest_batch_id" not in prepared_record:
                prepared_record["ingest_batch_id"] = batch_id
            
            prepared.append(prepared_record)
        return prepared
    
    def _convert_value(self, value: Any) -> str:
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, (dict, list)):
            import json
            return f"'{json.dumps(value).replace(chr(39), chr(39)+chr(39))}'"
        else:
            return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"
    
    def _build_insert_query(self, table: str, records: List[Dict[str, Any]]) -> str:
        if not records: return ""
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        values_parts = []
        for record in records:
            values = [self._convert_value(record.get(col)) for col in columns]
            values_parts.append(f"({', '.join(values)})")
        return f"INSERT INTO {table} ({columns_str}) VALUES\n{', '.join(values_parts)}"
    
    def _execute_query(self, query: str) -> bool:
        import requests
        headers = {"Content-Type": "text/plain"}
        response = requests.post(self._url, data=query.encode("utf-8"), headers=headers, timeout=60)
        if response.status_code != 200:
            logger.error(f"ClickHouse error: {response.text}")
            response.raise_for_status()
        return True
    
    def write_batch(self, table: str, records: List[Dict[str, Any]], source: Optional[str] = None) -> int:
        if not records: return 0
        batch_id = self._generate_batch_id()
        prepared_records = self._prepare_records(records, table, batch_id)
        
        total_written = 0
        for i in range(0, len(prepared_records), self.batch_size):
            batch = prepared_records[i:i + self.batch_size]
            for attempt in range(self.max_retries):
                try:
                    query = self._build_insert_query(table, batch)
                    self._execute_query(query)
                    total_written += len(batch)
                    break
                except Exception as e:
                    if attempt == self.max_retries - 1: raise
                    logger.warning(f"Retry {attempt+1} due to {e}")
        return total_written

    def truncate_table(self, table: str) -> bool:
        self._execute_query(f"TRUNCATE TABLE {table}")
        return True

    def write_batch_rewrite(self, table: str, records: List[Dict[str, Any]], source: Optional[str] = None) -> int:
        if not records: return 0
        self.truncate_table(table)
        return self.write_batch(table, records, source)


class CSVWriter:
    """
    CSV Data Writer for GitHub Actions ingestion.
    Writes records to CSV files following the naming convention:
    [source]_[type]_%Y%m%d_%H%M.csv
    """
    
    def __init__(self, output_dir: str = "landing_zone"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        logger.info(f"CSVWriter initialized: output_dir={output_dir}")

    def _get_filename(self, table: str, source: Optional[str]) -> str:
        """Generate filename based on source, type and timestamp."""
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        
        # Determine source and type from table name if not provided
        # Example table: raw_openweather_measurements
        parts = table.split('_')
        src = source or (parts[1] if len(parts) > 1 else "misc")
        dtype = parts[2] if len(parts) > 2 else "raw"
        
        # Map specific types to user's suggested short names
        type_map = {
            "measurements": "meas",
            "weather": "weat",
            "traffic": "traf",
            "flow": "flow"
        }
        short_type = type_map.get(dtype, dtype[:4])
        
        return f"{src}_{short_type}_{ts}.csv"

    def write_batch(
        self, 
        table: str, 
        records: List[Dict[str, Any]], 
        source: Optional[str] = None
    ) -> int:
        if not records:
            return 0
            
        filename = self._get_filename(table, source)
        full_path = os.path.join(self.output_dir, filename)
        
        # Add metadata like ClickHouseWriter does
        batch_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        prepared_records = []
        for r in records:
            pr = r.copy()
            pr["ingest_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pr["ingest_batch_id"] = batch_id
            if "source" not in pr:
                pr["source"] = source or table.split('_')[1] if '_' in table else "unknown"
            prepared_records.append(pr)

        keys = prepared_records[0].keys()
        
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(prepared_records)
            
        logger.info(f"Successfully wrote {len(prepared_records)} records to {full_path}")
        return len(prepared_records)

    def write_batch_rewrite(
        self, 
        table: str, 
        records: List[Dict[str, Any]], 
        source: Optional[str] = None
    ) -> int:
        # For CSV, rewrite is same as write_batch but we might want to flag it
        # as a full refresh in metadata if needed. For now, just write.
        return self.write_batch(table, records, source)


def create_clickhouse_writer(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    batch_size: int = 1000
) -> DataWriter:
    """
    Factory function to create a writer based on environment mode.
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

