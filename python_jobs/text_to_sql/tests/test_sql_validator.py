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
        validate_sql(
            "SELECT * FROM dm_aqi_current_status LIMIT 1; "
            "SELECT * FROM dm_air_quality_overview_daily LIMIT 1"
        )


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


@pytest.mark.unit
def test_rejects_non_aggregated_columns_without_group_by():
    with pytest.raises(SqlValidationError, match="requires a GROUP BY clause"):
        validate_sql("SELECT province, MAX(pm25_avg) FROM fct_air_quality_province_level_daily")

    sql = """
    SELECT h.province, h.max_aqi_vn, hp.province, hp.max_aqi_vn
    FROM (
      SELECT province, MAX(max_aqi_vn) AS max_aqi_vn
      FROM fct_air_quality_province_level_daily
      WHERE province = 'Hà Nội'
    ) AS h
    CROSS JOIN (
      SELECT province, MAX(max_aqi_vn) AS max_aqi_vn
      FROM fct_air_quality_province_level_daily
      WHERE province = 'Hải Phòng'
    ) AS hp
    """
    with pytest.raises(SqlValidationError, match="requires a GROUP BY clause"):
        validate_sql(sql)


@pytest.mark.unit
def test_accepts_valid_aggregation_with_group_by():
    sql_1 = "SELECT province, MAX(pm25_avg) FROM fct_air_quality_province_level_daily GROUP BY province"
    result_1 = validate_sql(sql_1)
    assert result_1.referenced_tables == ["fct_air_quality_province_level_daily"]

    sql_2 = """
    SELECT
      h.province AS province_1,
      h.max_aqi AS max_aqi_1,
      hp.province AS province_2,
      hp.max_aqi AS max_aqi_2,
      (h.max_aqi - hp.max_aqi) AS difference
    FROM (
      SELECT province, MAX(max_aqi_vn) AS max_aqi
      FROM fct_air_quality_province_level_daily
      WHERE province = 'Hà Nội'
      GROUP BY province
    ) AS h
    CROSS JOIN (
      SELECT province, MAX(max_aqi_vn) AS max_aqi
      FROM fct_air_quality_province_level_daily
      WHERE province = 'Hải Phòng'
      GROUP BY province
    ) AS hp
    """
    result_2 = validate_sql(sql_2)
    assert result_2.referenced_tables == ["fct_air_quality_province_level_daily"]


@pytest.mark.unit
def test_translates_vietnamese_regions():
    sql = "SELECT * FROM fct_air_quality_province_level_daily WHERE region_3 = 'Miền Bắc' AND region_8 = 'đồng bằng sông cửu long' LIMIT 5"
    result = validate_sql(sql)
    assert "region_3 = 'Northern'" in result.sql
    assert "region_8 = 'Mekong Delta'" in result.sql


