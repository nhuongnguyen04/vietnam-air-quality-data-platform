"""
Base Writer Module - Defines the interface for all data writers.
"""

from typing import List, Dict, Any, Optional, Protocol

class DataWriter(Protocol):
    """Protocol defining the interface for data writers."""
    
    def write_batch(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        """Write a batch of records to the destination."""
        ...
        
    def write_batch_rewrite(
        self,
        table: str,
        records: List[Dict[str, Any]],
        source: Optional[str] = None
    ) -> int:
        """Truncate then write a batch of records (full refresh)."""
        ...
