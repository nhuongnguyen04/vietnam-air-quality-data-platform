"""Tests for ingestion_control writes."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from python_jobs.common.ingestion_control import update_control


@pytest.mark.unit
def test_update_control_skips_clickhouse_in_csv_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client_factory = MagicMock()
    monkeypatch.setenv("INGEST_MODE", "csv")
    monkeypatch.setattr(
        "python_jobs.common.ingestion_control.get_clickhouse_client",
        fake_client_factory,
    )

    update_control(source="aqiin", records_ingested=12, success=True)

    fake_client_factory.assert_not_called()


@pytest.mark.integration
def test_update_control_inserts_expected_row(
    monkeypatch: pytest.MonkeyPatch,
    mock_clickhouse_client: MagicMock,
) -> None:
    last_run = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    monkeypatch.delenv("INGEST_MODE", raising=False)
    monkeypatch.setattr(
        "python_jobs.common.ingestion_control.get_clickhouse_client",
        MagicMock(return_value=mock_clickhouse_client),
    )

    update_control(
        source="openweather",
        records_ingested=25,
        success=False,
        error_message="temporary error",
        last_run=last_run,
    )

    mock_clickhouse_client.insert.assert_called_once()
    args, kwargs = mock_clickhouse_client.insert.call_args
    assert args[0] == "ingestion_control"
    row = args[1][0]
    assert row[0] == "openweather"
    assert row[1] == last_run
    assert row[2] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert row[3] == 25
    assert row[4] == -1
    assert row[5] == "temporary error"
    assert kwargs["column_names"] == [
        "source",
        "last_run",
        "last_success",
        "records_ingested",
        "lag_seconds",
        "error_message",
        "updated_at",
    ]
    mock_clickhouse_client.close.assert_called_once()
