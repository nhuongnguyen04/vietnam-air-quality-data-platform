"""
ClickHouse Writer Module - Batch writer for ClickHouse database.

This module provides a ClickHouseWriter class for efficient batch inserts.
"""

import logging
import uuid
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from .base_writer import DataWriter

logger = logging.getLogger(__name__)

class ClickHouseWriter:
    """
    ClickHouse batch writer using HTTP Interface.
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
