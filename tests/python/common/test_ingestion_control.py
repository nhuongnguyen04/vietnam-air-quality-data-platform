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
def test_update_control_inserts_expected_row_for_failed_run_preserving_last_success(
    monkeypatch: pytest.MonkeyPatch,
    mock_clickhouse_client: MagicMock,
) -> None:
    last_run = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    previous_success = datetime(2026, 4, 22, 9, 0, tzinfo=timezone.utc)
    monkeypatch.delenv("INGEST_MODE", raising=False)
    mock_clickhouse_client.query.return_value.result_rows = [[previous_success]]
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
    assert row[2] == previous_success
    assert row[3] == 25
    assert row[4] == 3600
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


@pytest.mark.integration
def test_update_control_success_sets_last_success_to_current_run(
    monkeypatch: pytest.MonkeyPatch,
    mock_clickhouse_client: MagicMock,
) -> None:
    last_run = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    monkeypatch.delenv("INGEST_MODE", raising=False)
    mock_clickhouse_client.query.return_value.result_rows = [[None]]
    monkeypatch.setattr(
        "python_jobs.common.ingestion_control.get_clickhouse_client",
        MagicMock(return_value=mock_clickhouse_client),
    )

    update_control(
        source="dag_sync_gdrive",
        records_ingested=4,
        success=True,
        last_run=last_run,
    )

    args, _ = mock_clickhouse_client.insert.call_args
    row = args[1][0]
    assert row[0] == "dag_sync_gdrive"
    assert row[1] == last_run
    assert row[2] == last_run
    assert row[3] == 4
    assert row[4] == 0
    assert row[5] == ""
