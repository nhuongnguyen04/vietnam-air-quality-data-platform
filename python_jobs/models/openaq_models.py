"""
OpenAQ API Data Models - Pydantic models for OpenAQ API responses.

This module provides data models for:
- Parameters
- Locations
- Sensors
- Measurements

Author: Air Quality Data Platform
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class OpenAQParameter(BaseModel):
    """OpenAQ parameter model."""
    id: int
    name: str
    units: str
    display_name: Optional[str] = None
    description: Optional[str] = None


class OpenAQParametersResponse(BaseModel):
    """Response model for /v3/parameters endpoint."""
    meta: Dict[str, Any]
    results: List[OpenAQParameter]


class OpenAQCountry(BaseModel):
    """OpenAQ country information."""
    id: int
    code: str
    name: str


class OpenAQProvider(BaseModel):
    """OpenAQ provider information."""
    id: int
    name: str


class OpenAQOwner(BaseModel):
    """OpenAQ owner information."""
    id: int
    name: str


class OpenAQLocation(BaseModel):
    """OpenAQ location model."""
    id: int = Field(alias="location_id")
    name: Optional[str] = None
    locality: Optional[str] = None
    timezone: Optional[str] = None
    country_id: Optional[int] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    owner_id: Optional[int] = None
    owner_name: Optional[str] = None
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    is_mobile: Optional[bool] = False
    is_monitor: Optional[bool] = True
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    raw_sensors: Optional[str] = None
    datetime_first: Optional[str] = None
    datetime_last: Optional[str] = None
    
    class Config:
        populate_by_name = True


class OpenAQLocationsResponse(BaseModel):
    """Response model for /v3/locations endpoint."""
    meta: Dict[str, Any]
    results: List[OpenAQLocation]


class OpenAQCoverage(BaseModel):
    """OpenAQ coverage information."""
    expected_count: Optional[int] = None
    expected_interval: Optional[str] = None
    observed_count: Optional[int] = None
    observed_interval: Optional[str] = None
    percent_complete: Optional[float] = None
    percent_coverage: Optional[float] = None
    datetime_from_utc: Optional[datetime] = None
    datetime_to_utc: Optional[datetime] = None


class OpenAQPeriod(BaseModel):
    """OpenAQ measurement period."""
    label: Optional[str] = None
    interval: Optional[str] = None
    datetime_from_utc: Optional[datetime] = None
    datetime_from_local: Optional[str] = None
    datetime_to_utc: Optional[datetime] = None
    datetime_to_local: Optional[str] = None


class OpenAQCoordinates(BaseModel):
    """OpenAQ coordinates."""
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class OpenAQLatest(BaseModel):
    """OpenAQ latest measurement for a sensor."""
    datetime_utc: Optional[datetime] = None
    datetime_local: Optional[str] = None
    value: Optional[float] = None
    coordinates: Optional[OpenAQCoordinates] = None


class OpenAQSensor(BaseModel):
    """OpenAQ sensor model."""
    id: int = Field(alias="sensor_id")
    location_id: Optional[int] = None
    parameter_id: Optional[int] = None
    name: Optional[str] = None
    datetime_first_utc: Optional[datetime] = None
    datetime_first_local: Optional[str] = None
    datetime_last_utc: Optional[datetime] = None
    datetime_last_local: Optional[str] = None
    coverage: Optional[OpenAQCoverage] = None
    latest: Optional[OpenAQLatest] = None
    
    class Config:
        populate_by_name = True


class OpenAQSensorsResponse(BaseModel):
    """Response model for /v3/locations/{id}/sensors endpoint."""
    meta: Dict[str, Any]
    results: List[OpenAQSensor]


class OpenAQMeasurement(BaseModel):
    """OpenAQ measurement model."""
    # Foreign keys
    location_id: Optional[int] = None
    sensor_id: Optional[int] = None
    parameter_id: Optional[int] = None
    
    # Core measurement data
    value: Optional[float] = None
    
    # Period information
    period_label: Optional[str] = None
    period_interval: Optional[str] = None
    period_datetime_from_utc: Optional[datetime] = None
    period_datetime_from_local: Optional[str] = None
    period_datetime_to_utc: Optional[datetime] = None
    period_datetime_to_local: Optional[str] = None
    
    # Coverage information
    coverage_expected_count: Optional[int] = None
    coverage_expected_interval: Optional[str] = None
    coverage_observed_count: Optional[int] = None
    coverage_observed_interval: Optional[str] = None
    coverage_percent_complete: Optional[float] = None
    coverage_percent_coverage: Optional[float] = None
    coverage_datetime_from_utc: Optional[datetime] = None
    coverage_datetime_to_utc: Optional[datetime] = None
    
    # Flag information
    flag_has_flags: Optional[bool] = False
    
    # Coordinates
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # Summary
    summary: Optional[str] = None
    
    # Raw payload for audit
    raw_payload: Optional[str] = None


class OpenAQMeasurementsResponse(BaseModel):
    """Response model for /v3/sensors/{id}/measurements endpoint."""
    meta: Dict[str, Any]
    results: List[Dict[str, Any]]  # Raw results for flexibility


def format_datetime(dt_str: Optional[str]) -> Optional[str]:
    """Format ISO 8601 datetime string to ClickHouse format (YYYY-MM-DD HH:MM:SS)."""
    if not dt_str:
        return None
    return dt_str.replace("T", " ").replace("Z", "")


def transform_parameter(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw parameter API response to database format."""
    return {
        "parameter_id": raw.get("id"),
        "name": raw.get("name"),
        "display_name": raw.get("displayName"),
        "units": raw.get("units"),
        "description": raw.get("description"),
        "raw_payload": str(raw)
    }


def transform_location(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw location API response to database format."""
    country = raw.get("country") or {}
    owner = raw.get("owner") or {}
    provider = raw.get("provider") or {}
    coordinates = raw.get("coordinates") or {}
    dt_first = raw.get("datetimeFirst") or {}
    dt_last = raw.get("datetimeLast") or {}
    
    return {
        "location_id": raw.get("id"),
        "name": raw.get("name"),
        "locality": raw.get("locality"),
        "timezone": raw.get("timezone"),
        "country_id": country.get("id"),
        "country_code": country.get("code"),
        "country_name": country.get("name"),
        "owner_id": owner.get("id"),
        "owner_name": owner.get("name"),
        "provider_id": provider.get("id"),
        "provider_name": provider.get("name"),
        "is_mobile": raw.get("isMobile", False),
        "is_monitor": raw.get("isMonitor", True),
        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),
        "raw_sensors": str(raw.get("sensors", [])),
        "datetime_first": format_datetime(dt_first.get("utc")),
        "datetime_last": format_datetime(dt_last.get("utc")),
        "raw_payload": str(raw)
    }


def transform_sensor(raw: Dict[str, Any], location_id: int) -> Dict[str, Any]:
    """Transform raw sensor API response to database format."""
    parameter = raw.get("parameter") or {}
    dt_first = raw.get("datetimeFirst") or {}
    dt_last = raw.get("datetimeLast") or {}
    coverage = raw.get("coverage") or {}
    coverage_dt = coverage.get("datetimeFrom") or {}
    coverage_dt_to = coverage.get("datetimeTo") or {}
    latest = raw.get("latest") or {}
    latest_dt = latest.get("datetime") or {}
    latest_coords = latest.get("coordinates") or {}
    
    return {
        "sensor_id": raw.get("id"),
        "location_id": location_id,
        "parameter_id": parameter.get("id"),
        "name": parameter.get("name"),
        "datetime_first_utc": format_datetime(dt_first.get("utc")),
        "datetime_first_local": format_datetime(dt_first.get("local")),
        "datetime_last_utc": format_datetime(dt_last.get("utc")),
        "datetime_last_local": format_datetime(dt_last.get("local")),
        "coverage_expected_count": coverage.get("expectedCount"),
        "coverage_expected_interval": coverage.get("expectedInterval"),
        "coverage_observed_count": coverage.get("observedCount"),
        "coverage_observed_interval": coverage.get("observedInterval"),
        "coverage_percent_complete": coverage.get("percentComplete"),
        "coverage_percent_coverage": coverage.get("percentCoverage"),
        "coverage_datetime_from_utc": format_datetime(coverage_dt.get("utc")),
        "coverage_datetime_to_utc": format_datetime(coverage_dt_to.get("utc")),
        "latest_datetime_utc": format_datetime(latest_dt.get("utc")),
        "latest_datetime_local": format_datetime(latest_dt.get("local")),
        "latest_value": latest.get("value"),
        "latest_latitude": latest_coords.get("latitude"),
        "latest_longitude": latest_coords.get("longitude"),
        "raw_payload": str(raw)
    }


def transform_measurement(
    raw: Dict[str, Any],
    location_id: int,
    sensor_id: int,
    parameter_id: int
) -> Dict[str, Any]:
    """Transform raw measurement API response to database format."""
    period = raw.get("period") or {}
    period_dt_from = period.get("datetimeFrom") or {}
    period_dt_to = period.get("datetimeTo") or {}
    
    coverage = raw.get("coverage") or {}
    coverage_dt_from = coverage.get("datetimeFrom") or {}
    coverage_dt_to = coverage.get("datetimeTo") or {}
    
    coordinates = raw.get("coordinates") or {}
    flag_info = raw.get("flagInfo") or {}
    
    return {
        "location_id": location_id,
        "sensor_id": sensor_id,
        "parameter_id": parameter_id,
        "value": raw.get("value"),
        "period_label": period.get("label"),
        "period_interval": period.get("interval"),
        "period_datetime_from_utc": format_datetime(period_dt_from.get("utc")),
        "period_datetime_from_local": format_datetime(period_dt_from.get("local")),
        "period_datetime_to_utc": format_datetime(period_dt_to.get("utc")),
        "period_datetime_to_local": format_datetime(period_dt_to.get("local")),
        "coverage_expected_count": coverage.get("expectedCount"),
        "coverage_expected_interval": coverage.get("expectedInterval"),
        "coverage_observed_count": coverage.get("observedCount"),
        "coverage_observed_interval": coverage.get("observedInterval"),
        "coverage_percent_complete": coverage.get("percentComplete"),
        "coverage_percent_coverage": coverage.get("percentCoverage"),
        "coverage_datetime_from_utc": format_datetime(coverage_dt_from.get("utc")),
        "coverage_datetime_to_utc": format_datetime(coverage_dt_to.get("utc")),
        "flag_has_flags": flag_info.get("hasFlags", False),
        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),
        "summary": raw.get("summary"),
        "raw_payload": str(raw)
    }

