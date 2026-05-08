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

from .api_client import APIClient, PaginatedAPIClient
from .base_writer import DataWriter
from .clickhouse_writer import ClickHouseWriter
from .config import APIConfig, ClickHouseConfig, IngestionConfig, JobConfig, get_config, reset_config
from .csv_writer import CSVWriter
from .logging_config import JobLogger, get_logger, log_job_stats, setup_logging
from .rate_limiter import AdaptiveRateLimiter, TokenBucketRateLimiter
from .writer_factory import create_clickhouse_writer, get_data_writer

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
