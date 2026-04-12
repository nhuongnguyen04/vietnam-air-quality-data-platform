"""
CSV Writer Module - Specialized writer for local landing zone storage.
"""

import os
import csv
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from .base_writer import DataWriter

logger = logging.getLogger(__name__)

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
        # Use Vietnam Time (UTC+7) to ensure correct date for early morning runs
        ict_tz = timezone(timedelta(hours=7))
        ts = datetime.now(ict_tz).strftime("%Y%m%d_%H%M")
        
        # Determine source and type from table name if not provided
        parts = table.split('_')
        src = source or (parts[1] if len(parts) > 1 else "misc")
        dtype = parts[2] if len(parts) > 2 else "raw"
        
        # Map specific types to user's suggested short names
        type_map = {
            "measurements": "meas",
            "weather": "weat",
            "meteorology": "weat",   # Map OpenWeather meteorology to weat
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
        
        # Add metadata
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
        return self.write_batch(table, records, source)
