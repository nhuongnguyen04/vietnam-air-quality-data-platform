"""
Common utilities for Air Quality Data Platform Jobs.

This package provides:
- rate_limiter: Token bucket rate limiting for API calls
- api_client: HTTP client with retry logic
- base_writer: DataWriter protocol
- csv_writer: CSVWriter implementation
- clickhouse_writer: ClickHouseWriter implementation
- writer_factory: Factory for creating writers (get_data_writer)
- config: Configuration management
- logging_config: Structured logging

Author: Air Quality Data Platform
"""

import os
try:
    from dotenv import load_dotenv
    _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    _env_path = os.path.join(_project_root, '.env')
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
except ImportError:
    pass

from .rate_limiter import (
    TokenBucketRateLimiter,
    AdaptiveRateLimiter
)

from .api_client import (
    APIClient,
    PaginatedAPIClient
)

from .base_writer import DataWriter

from .csv_writer import CSVWriter

from .clickhouse_writer import ClickHouseWriter

from .writer_factory import (
    get_data_writer,
    create_clickhouse_writer
)

from .config import (
    ClickHouseConfig,
    APIConfig,
    JobConfig,
    IngestionConfig,
    get_config,
    reset_config
)

from .logging_config import (
    setup_logging,
    get_logger,
    JobLogger,
    log_job_stats
)

__all__ = [
    # Rate limiter
    "TokenBucketRateLimiter",
    "AdaptiveRateLimiter",
    
    # API client
    "APIClient",
    "PaginatedAPIClient",
    
    # Writers
    "DataWriter",
    "CSVWriter",
    "ClickHouseWriter",
    "get_data_writer",
    "create_clickhouse_writer",
    
    # Config
    "ClickHouseConfig",
    "APIConfig",
    "JobConfig",
    "IngestionConfig",
    "get_config",
    "reset_config",
    
    # Logging
    "setup_logging",
    "get_logger",
    "JobLogger",
    "log_job_stats"
]
