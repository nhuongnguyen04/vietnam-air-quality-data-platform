"""
ingestion_control — ClickHouse ingestion control table writer.

This module writes run metadata to the `ingestion_control` table after each
ingestion job run. Used by Airflow DAGs via PythonOperator or @task.

Author: Air Quality Data Platform
"""

import os
from datetime import datetime, timezone
from typing import Optional


def get_clickhouse_client():
    """Create a ClickHouse client using environment variables."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', '8123')),
        username=os.environ.get('CLICKHOUSE_USER', 'admin'),
        password=os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        database=os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    )


def update_control(
    source: str,
    records_ingested: int,
    success: bool,
    error_message: str = '',
    last_run: Optional[datetime] = None,
) -> None:
    """
    Write or update a row in `ingestion_control` for the given source.
    """
    # Skip if we are in CSV mode (e.g. GitHub Actions) as there is no ClickHouse
    if os.environ.get('INGEST_MODE') == 'csv':
        logging_level = 'INFO' if success else 'ERROR'
        import logging
        logger = logging.getLogger(__name__)
        logger.log(getattr(logging, logging_level), f"SKIPPING INGESTION_CONTROL UPDATE (CSV MODE): source={source}, success={success}, records={records_ingested}")
        return

    client = get_clickhouse_client()

    now = last_run or datetime.now(timezone.utc)
    # Sentinel value for "never succeeded" — clickhouse_connect 0.9.2 crashes
    # when a datetime column contains bare Python None.
    NEVER_SUCCEEDED = datetime(1970, 1, 1, tzinfo=timezone.utc)
    last_success = now if success else NEVER_SUCCEEDED
    lag_seconds = 0 if success else -1

    client.insert(
        'ingestion_control',
        [[
            source,
            now,
            last_success,
            records_ingested,
            lag_seconds,
            error_message,
            now,
        ]],
        column_names=[
            'source',
            'last_run',
            'last_success',
            'records_ingested',
            'lag_seconds',
            'error_message',
            'updated_at',
        ],
    )
    client.close()
