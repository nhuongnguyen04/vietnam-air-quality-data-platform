"""ClickHouse client for Streamlit dashboard."""
from __future__ import annotations
import os
import clickhouse_connect
import pandas as pd


def get_client():
    """Create ClickHouse client from environment variables.

    Required env vars: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
                      CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
    """
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    if not password:
        raise ValueError(
            "CLICKHOUSE_PASSWORD environment variable is required. "
            "Set it before running the dashboard."
        )

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "admin"),
        password=password,
        database=os.environ.get("CLICKHOUSE_DB", "air_quality"),
        settings={
            "connect_timeout": 30,
            "receive_timeout": 60,
        },
    )


def query_df(query: str, client=None) -> pd.DataFrame:
    """Execute query and return DataFrame."""
    if client is None:
        client = get_client()
    result = client.query(query)
    return pd.DataFrame(result.named_results())
