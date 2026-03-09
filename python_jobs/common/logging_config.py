"""
Logging Configuration Module - Structured logging for Python jobs.

This module provides:
- JSON-structured logging
- Consistent log format across all jobs
- Log level configuration
- File and console output

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import json
from datetime import datetime
from typing import Any, Dict, Optional
from pythonjsonlogger import jsonlogger


class StructuredLogFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter for structured logging.
    
    Adds extra fields like:
    - timestamp
    - level
    - logger name
    - message
    - module/function/line for debugging
    """
    
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        """Add custom fields to log record."""
        super().add_fields(log_record, record, message_dict)
        
        # Add timestamp
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        # Add level
        log_record['level'] = record.levelname
        
        # Add logger name
        log_record['logger'] = record.name
        
        # Add source location
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno
        
        # Add job context if available
        job_context = getattr(record, 'job_context', None)
        if job_context:
            log_record['job_context'] = job_context


class JobContextFilter(logging.Filter):
    """
    Logging filter to add job context to all log records.
    
    Usage:
        logger.addFilter(JobContextFilter(job_id="job123", source="openaq"))
    """
    
    def __init__(self, job_id: Optional[str] = None, source: Optional[str] = None):
        super().__init__()
        self.job_id = job_id
        self.source = source
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add job context to record."""
        if self.job_id:
            record.job_context = {"job_id": self.job_id, "source": self.source}
        return True


def get_default_log_dir() -> str:
    """Get the default log directory based on the environment."""
    if os.environ.get("JOB_LOG_DIR"):
        return os.environ["JOB_LOG_DIR"]
    if os.environ.get("AIRFLOW_HOME"):
        return os.path.join(os.environ["AIRFLOW_HOME"], "logs")
    if os.path.exists("/opt/airflow"):
        return "/opt/airflow/logs"
    return "logs"


def setup_logging(
    level: str = "INFO",
    log_to_file: bool = True,
    log_dir: Optional[str] = None,
    job_name: Optional[str] = None,
    source: Optional[str] = None
) -> logging.Logger:
    """
    Setup structured logging for a job.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_to_file: Whether to log to file
        log_dir: Directory for log files (defaults to env-aware path)
        job_name: Name of the job (used for log file name)
        source: Data source (openaq, aqicn)
        
    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger()
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Set level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    
    # Add JSON console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_formatter = StructuredLogFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if requested
    if log_to_file:
        if log_dir is None:
            log_dir = get_default_log_dir()
            
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
            # Determine log file name
            if job_name:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                log_file = os.path.join(log_dir, f"{job_name}_{timestamp}.log")
            else:
                log_file = os.path.join(log_dir, "python_jobs.log")
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(numeric_level)
            
            # Use same formatter for file
            file_handler.setFormatter(console_formatter)
            logger.addHandler(file_handler)
            
            logger.info(f"Logging to file: {log_file}")
    
    # Add job context filter if job_name provided
    if job_name:
        context_filter = JobContextFilter(job_id=job_name, source=source)
        logger.addFilter(context_filter)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class JobLogger:
    """
    Context manager for job-level logging.
    
    Usage:
        with JobLogger("ingest_openaq_measurements", source="openaq") as logger:
            logger.info("Starting job", extra={"records_processed": 0})
    """
    
    def __init__(self, job_name: str, source: Optional[str] = None, level: str = "INFO"):
        self.job_name = job_name
        self.source = source
        self.level = level
        self.logger: Optional[logging.Logger] = None
    
    def __enter__(self) -> logging.Logger:
        self.logger = setup_logging(
            level=self.level,
            log_to_file=True,
            job_name=self.job_name,
            source=self.source
        )
        
        # Log job start
        self.logger.info(
            f"Job started: {self.job_name}",
            extra={
                "job_name": self.job_name,
                "source": self.source,
                "event": "job_start"
            }
        )
        
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.logger:
            if exc_type:
                self.logger.error(
                    f"Job failed: {self.job_name}",
                    extra={
                        "job_name": self.job_name,
                        "source": self.source,
                        "event": "job_failure",
                        "error": str(exc_val)
                    },
                    exc_info=True
                )
            else:
                self.logger.info(
                    f"Job completed: {self.job_name}",
                    extra={
                        "job_name": self.job_name,
                        "source": self.source,
                        "event": "job_complete"
                    }
                )


def log_job_stats(
    logger: logging.Logger,
    job_name: str,
    stats: Dict[str, Any]
) -> None:
    """
    Log job statistics in a structured way.
    
    Args:
        logger: Logger instance
        job_name: Name of the job
        stats: Dictionary of statistics to log
    """
    logger.info(
        f"Job stats: {job_name}",
        extra={
            "job_name": job_name,
            "event": "job_stats",
            "stats": stats
        }
    )

