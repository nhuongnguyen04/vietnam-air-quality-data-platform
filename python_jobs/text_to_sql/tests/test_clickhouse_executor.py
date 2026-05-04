from __future__ import annotations

import pytest

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
