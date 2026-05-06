"""
ingestion_control — ClickHouse ingestion control table writer.

This module writes run metadata to the `ingestion_control` table after each
ingestion job run. Used by Airflow DAGs via PythonOperator or @task.

Author: Air Quality Data Platform
"""

import os
from datetime import datetime, timezone
from typing import Optional

NEVER_SUCCEEDED = datetime(1970, 1, 1, tzinfo=timezone.utc)


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


def _normalize_utc_timestamp(value: object) -> Optional[datetime]:
    """Normalize ClickHouse timestamps into timezone-aware UTC datetimes."""
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _get_previous_last_success(client, source: str) -> Optional[datetime]:
    """Return the latest successful run timestamp already stored for a source."""
    safe_source = source.replace("'", "''")
    result = client.query(
        "SELECT max(last_success) "
        f"FROM ingestion_control WHERE source = '{safe_source}'"
    )
    if not result.result_rows:
        return None

    previous = _normalize_utc_timestamp(result.result_rows[0][0])
    if previous is None or previous <= NEVER_SUCCEEDED:
        return None
    return previous


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
    try:
        now = last_run or datetime.now(timezone.utc)
        previous_last_success = _get_previous_last_success(client, source)
        effective_last_success = now if success else previous_last_success

        if effective_last_success is None:
            stored_last_success = NEVER_SUCCEEDED
            lag_seconds = -1
        else:
            stored_last_success = effective_last_success
            lag_seconds = 0 if success else int((now - effective_last_success).total_seconds())

        client.insert(
            'ingestion_control',
            [[
                source,
                now,
                stored_last_success,
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
    finally:
        client.close()
