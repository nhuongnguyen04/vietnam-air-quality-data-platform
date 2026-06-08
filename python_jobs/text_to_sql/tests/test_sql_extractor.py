from __future__ import annotations

import pytest

from python_jobs.text_to_sql.sql_extractor import extract_sql_statement


@pytest.mark.unit
def test_extract_simple_sql():
    response = "Here is the query:\nSELECT * FROM table;"
    assert extract_sql_statement(response) == "SELECT * FROM table"


@pytest.mark.unit
def test_extract_sql_with_fenced_blocks():
    response = "Sure! ```sql\nSELECT * FROM table;\n```"
    assert extract_sql_statement(response) == "SELECT * FROM table"


@pytest.mark.unit
def test_extract_sql_with_subqueries():
    response = """
SELECT
  (hn.max_aqi - hp.max_aqi) AS aqi_difference
FROM (
  SELECT MAX(max_aqi_vn) AS max_aqi
  FROM fct_air_quality_province_level_daily
  WHERE province = 'Hà Nội'
) AS hn
CROSS JOIN (
  SELECT MAX(max_aqi_vn) AS max_aqi
  FROM fct_air_quality_province_level_daily
  WHERE province = 'Hải Phòng'
) AS hp
"""
    expected = """SELECT
  (hn.max_aqi - hp.max_aqi) AS aqi_difference
FROM (
  SELECT MAX(max_aqi_vn) AS max_aqi
  FROM fct_air_quality_province_level_daily
  WHERE province = 'Hà Nội'
) AS hn
CROSS JOIN (
  SELECT MAX(max_aqi_vn) AS max_aqi
  FROM fct_air_quality_province_level_daily
  WHERE province = 'Hải Phòng'
) AS hp"""
    assert extract_sql_statement(response) == expected
