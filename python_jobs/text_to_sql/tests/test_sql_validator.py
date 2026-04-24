from __future__ import annotations

import pytest

from python_jobs.text_to_sql.sql_validator import SqlValidationError, validate_sql


@pytest.mark.unit
def test_rejects_drop_statement():
    with pytest.raises(SqlValidationError, match="DROP"):
        validate_sql("DROP TABLE dm_aqi_current_status")


@pytest.mark.unit
def test_rejects_raw_table_access():
    with pytest.raises(SqlValidationError, match="raw_aqiin_measurements"):
        validate_sql("SELECT * FROM raw_aqiin_measurements")


@pytest.mark.unit
def test_rejects_staging_table_access():
    with pytest.raises(SqlValidationError, match="stg_openweather__meteorology"):
        validate_sql("SELECT * FROM stg_openweather__meteorology")


@pytest.mark.unit
def test_rejects_system_schema_access():
    with pytest.raises(SqlValidationError, match="system"):
        validate_sql("SELECT * FROM system.tables")


@pytest.mark.unit
def test_rejects_multi_statement_input():
    with pytest.raises(SqlValidationError, match="Multi-statement"):
        validate_sql("SELECT 1; SELECT 2")


@pytest.mark.unit
def test_accepts_allowlisted_select_with_limit():
    result = validate_sql("SELECT * FROM dm_aqi_current_status LIMIT 10")

    assert "dm_aqi_current_status" in result.referenced_tables
    assert result.warnings == ["SELECT * may return unnecessary columns."]


@pytest.mark.unit
def test_accepts_allowlisted_with_query():
    sql = """
    WITH latest AS (
        SELECT province, current_aqi_vn
        FROM dm_aqi_current_status
        LIMIT 20
    )
    SELECT province, current_aqi_vn
    FROM latest
    LIMIT 10
    """

    result = validate_sql(sql)

    assert result.referenced_tables == ["dm_aqi_current_status"]
    assert "dm_aqi_current_status" in result.sql
