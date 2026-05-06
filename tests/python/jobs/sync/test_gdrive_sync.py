"""Tests for Google Drive to ClickHouse sync value formatting."""

from datetime import datetime, timezone

from python_jobs.jobs.sync.gdrive_sync import format_value, resolve_table_for_file


def test_format_datetime_with_microseconds_and_timezone():
    assert (
        format_value("2026-04-22 20:06:45.923553+00:00", "DateTime")
        == "'2026-04-22 20:06:45'"
    )


def test_format_datetime_z_suffix():
    assert (
        format_value("2026-04-22T20:06:45Z", "Nullable(DateTime)")
        == "'2026-04-22 20:06:45'"
    )


def test_format_datetime_object_as_utc():
    value = datetime(2026, 4, 22, 20, 6, 45, tzinfo=timezone.utc)
    assert format_value(value, "DateTime") == "'2026-04-22 20:06:45'"


def test_format_uint8_boolean_strings():
    assert format_value("False", "UInt8") == "0"
    assert format_value("true", "UInt8") == "1"
    assert format_value("0", "UInt8") == "0"
    assert format_value("1", "UInt8") == "1"


def test_format_bool_without_type():
    assert format_value(False) == "0"
    assert format_value(True) == "1"


def test_resolve_table_for_nested_openweather_measurement_chunk():
    assert (
        resolve_table_for_file("openweather/measurements", "openweather_meas_20260506_1610_0001.csv")
        == "raw_openweather_measurements"
    )


def test_resolve_table_for_flat_openweather_measurement_chunk():
    assert (
        resolve_table_for_file("", "openweather_meas_20260506_1610_0001.csv")
        == "raw_openweather_measurements"
    )


def test_resolve_table_for_flat_openweather_weather_chunk():
    assert (
        resolve_table_for_file("", "openweather_weat_20260506_1621_0001.csv")
        == "raw_openweather_meteorology"
    )
