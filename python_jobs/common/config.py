"""
Configuration Module - Configuration management for Python jobs.

This module provides configuration classes that read from:
- Environment variables
- Configuration files (YAML/JSON)
- Command-line arguments

Author: Air Quality Data Platform
"""

import os
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import yaml

logger = logging.getLogger(__name__)


@dataclass
class ClickHouseConfig:
    """ClickHouse database configuration."""
    host: str = "clickhouse"
    port: int = 8123
    database: str = "airquality"
    user: str = "admin"
    password: str = "admin"
    
    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Create config from environment variables."""
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            database=os.environ.get("CLICKHOUSE_DB", "airquality"),
            user=os.environ.get("CLICKHOUSE_USER", "admin"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "admin")
        )


@dataclass
class APIConfig:
    """API configuration."""
    openaq_token: Optional[str] = None
    aqicn_token: Optional[str] = None
    openweather_token: Optional[str] = None

    @classmethod
    def from_env(cls) -> "APIConfig":
        """Create config from environment variables."""
        return cls(
            openaq_token=os.environ.get("OPENAQ_API_TOKEN"),
            aqicn_token=os.environ.get("AQICN_API_TOKEN"),
            openweather_token=os.environ.get("OPENWEATHER_API_TOKEN"),
        )


@dataclass
class JobConfig:
    """Job-specific configuration."""
    # Rate limiting
    rate_limit_openaq: float = 0.8  # requests per second
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
    vietnam_bounds: Dict[str, float] = field(default_factory=lambda: {
        "lat_min": 10.246389,
        "lon_min": 103.584222,
        "lat_max": 22.428611,
        "lon_max": 109.555611
    })
    
    # Country code for OpenAQ
    country_code: str = "VN"
    country_id: int = 56  # Vietnam
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "JobConfig":
        """Load config from YAML file."""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_dict = yaml.safe_load(f)
                return cls(**config_dict)
        return cls()
    
    @classmethod
    def from_env(cls) -> "JobConfig":
        """Create config from environment variables."""
        return cls(
            rate_limit_openaq=float(os.environ.get("RATE_LIMIT_OPENAQ", "0.8")),
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
    def load(cls, config_path: Optional[str] = None) -> "IngestionConfig":
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
            with open(config_path, 'r') as f:
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
        if not self.api.openaq_token:
            logger.warning("OPENAQ_API_TOKEN not set")
        
        if not self.api.aqicn_token:
            logger.warning("AQICN_API_TOKEN not set")
        
        if self.job.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        
        if self.job.max_workers <= 0:
            raise ValueError("max_workers must be positive")
        
        return True


# Global config instance
_config: Optional[IngestionConfig] = None


def get_config(config_path: Optional[str] = None) -> IngestionConfig:
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

