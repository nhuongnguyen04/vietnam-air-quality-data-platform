"""
Configuration Module - Configuration management for Python jobs.

This module provides configuration classes that read from:
- Environment variables
- Configuration files (YAML/JSON)
- Command-line arguments

Author: Air Quality Data Platform
"""

import logging
import os
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ClickHouseConfig:
    """ClickHouse database configuration."""
    host: str = "clickhouse"
    port: int = 8123
    database: str = "air_quality"
    user: str = "admin"
    password: str = "admin"

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Create config from environment variables."""
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            database=os.environ.get("CLICKHOUSE_DB", "air_quality"),
            user=os.environ.get("CLICKHOUSE_USER", "admin"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "admin")
        )


@dataclass
class APIConfig:
    """API configuration."""
    aqicn_token: str | None = None
    openweather_token: str | None = None

    @classmethod
    def from_env(cls) -> "APIConfig":
        """Create config from environment variables."""
        return cls(
            aqicn_token=os.environ.get("AQICN_API_TOKEN"),
            openweather_token=os.environ.get("OPENWEATHER_API_TOKEN"),
        )


@dataclass
class JobConfig:
    """Job-specific configuration."""
    # Rate limiting
    rate_limit_aqicn: float = 1.0

    # Batch settings
    batch_size: int = 1000
    max_workers: int = 4

    # Time range settings
    default_hour_range: int = 1  # hours
    max_historical_days: int = 365

    # Retry settings
    max_retries: int = 3
    retry_delay: int = 5

    # Vietnam bounds for AQICN
    vietnam_bounds: dict[str, float] = field(default_factory=lambda: {
        "lat_min": 10.246389,
        "lon_min": 103.584222,
        "lat_max": 22.428611,
        "lon_max": 109.555611
    })

    @classmethod
    def from_yaml(cls, config_path: str) -> "JobConfig":
        """Load config from YAML file."""
        if os.path.exists(config_path):
            with open(config_path) as f:
                config_dict = yaml.safe_load(f)
                return cls(**config_dict)
        return cls()

    @classmethod
    def from_env(cls) -> "JobConfig":
        """Create config from environment variables."""
        return cls(
            rate_limit_aqicn=float(os.environ.get("RATE_LIMIT_AQICN", "1.0")),
            batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
            max_workers=int(os.environ.get("MAX_WORKERS", "4")),
            max_retries=int(os.environ.get("MAX_RETRIES", "3"))
        )


@dataclass
class IngestionConfig:
    """Complete ingestion configuration."""
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    job: JobConfig = field(default_factory=JobConfig)

    @classmethod
    def load(cls, config_path: str | None = None) -> "IngestionConfig":
        """
        Load complete configuration.

        Args:
            config_path: Optional path to YAML config file

        Returns:
            Complete IngestionConfig
        """
        # Start with defaults
        config = cls(
            clickhouse=ClickHouseConfig.from_env(),
            api=APIConfig.from_env(),
            job=JobConfig.from_env()
        )

        # Override with YAML if provided
        if config_path and os.path.exists(config_path):
            with open(config_path) as f:
                config_dict = yaml.safe_load(f)
                if config_dict:
                    if 'clickhouse' in config_dict:
                        config.clickhouse = ClickHouseConfig(**config_dict['clickhouse'])
                    if 'api' in config_dict:
                        config.api = APIConfig(**config_dict['api'])
                    if 'job' in config_dict:
                        config.job = JobConfig(**config_dict['job'])

        return config

    def validate(self) -> bool:
        """
        Validate configuration.

        Returns:
            True if valid

        Raises:
            ValueError: If configuration is invalid
        """
        if not self.api.aqicn_token:
            logger.warning("AQICN_API_TOKEN not set")

        if self.job.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.job.max_workers <= 0:
            raise ValueError("max_workers must be positive")

        return True


# Global config instance
_config: IngestionConfig | None = None


def get_config(config_path: str | None = None) -> IngestionConfig:
    """
    Get global configuration instance.

    Args:
        config_path: Optional path to config file

    Returns:
        IngestionConfig instance
    """
    global _config

    if _config is None:
        _config = IngestionConfig.load(config_path)

    return _config


def reset_config() -> None:
    """Reset global configuration (useful for testing)."""
    global _config
    _config = None


def require_env(name: str) -> str:
    """
    Get an environment variable or raise RuntimeError if it is not set or is empty.

    Args:
        name: The name of the environment variable.

    Returns:
        The value of the environment variable.

    Raises:
        RuntimeError: If the environment variable is not set or is empty.
    """
    value = os.environ.get(name)
    if value in (None, ''):
        raise RuntimeError(f"{name} environment variable is required")
    return value


def get_clickhouse_env_vars() -> dict[str, str]:
    """
    Get ClickHouse connection parameters as environment variables,
    ensuring CLICKHOUSE_PASSWORD is set.
    """
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': require_env('CLICKHOUSE_PASSWORD'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    }

