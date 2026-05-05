from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from python_jobs.text_to_sql.app import PreviewStore
from python_jobs.text_to_sql.clickhouse_executor import ClickHouseExecutor


@pytest.mark.unit
def test_default_limit_detects_multiline_limit() -> None:
    executor = ClickHouseExecutor(max_rows=500)

    sql, limit_was_added = executor._apply_default_limit(
        """
        SELECT
          province,
          current_aqi_vn
        FROM dm_aqi_current_status
        ORDER BY current_aqi_vn DESC
        LIMIT 1
        """
    )

    assert limit_was_added is False
    assert sql.count("LIMIT") == 1


@pytest.mark.unit
def test_default_limit_is_added_when_missing() -> None:
    executor = ClickHouseExecutor(max_rows=500)

    sql, limit_was_added = executor._apply_default_limit(
        "SELECT province FROM dm_aqi_current_status"
    )

    assert limit_was_added is True
    assert sql.endswith("LIMIT 500")


@pytest.mark.unit
def test_clickhouse_executor_requires_dedicated_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEXT_TO_SQL_CLICKHOUSE_USER", raising=False)
    monkeypatch.delenv("TEXT_TO_SQL_CLICKHOUSE_PASSWORD", raising=False)
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "fallback-should-not-be-used")

    with pytest.raises(
        ValueError,
        match="Dedicated TEXT_TO_SQL_CLICKHOUSE_USER and TEXT_TO_SQL_CLICKHOUSE_PASSWORD are required",
    ):
        ClickHouseExecutor()._get_client()


@pytest.mark.unit
def test_clickhouse_executor_uses_dedicated_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEXT_TO_SQL_CLICKHOUSE_USER", "aqi_reader")
    monkeypatch.setenv("TEXT_TO_SQL_CLICKHOUSE_PASSWORD", "secret")

    with patch(
        "python_jobs.text_to_sql.clickhouse_executor.clickhouse_connect.get_client",
        return_value=MagicMock(),
    ) as mock_get_client:
        ClickHouseExecutor()._get_client()

    kwargs = mock_get_client.call_args.kwargs
    assert kwargs["username"] == "aqi_reader"
    assert kwargs["password"] == "secret"


@pytest.mark.unit
def test_preview_store_requires_preview_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEXT_TO_SQL_PREVIEW_SECRET", raising=False)

    with pytest.raises(
        RuntimeError,
        match="TEXT_TO_SQL_PREVIEW_SECRET environment variable is required",
    ):
        PreviewStore()
