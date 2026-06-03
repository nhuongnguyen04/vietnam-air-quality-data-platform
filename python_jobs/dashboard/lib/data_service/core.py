import streamlit as st
from lib.aqi_utils import AQI_VN_SQL_EXPR

def escape_value(val) -> str:
    """Safely escape backslashes and single quotes in string values to prevent SQL injection."""
    if val is None:
        return ""
    if not isinstance(val, str):
        val = str(val)
    return val.replace("\\", "\\\\").replace("'", "\\'")

# Mapping of temporal grains to dbt ANALYTICS models (dm_*)
TIME_GRAIN_TABLE = {
    "Giờ": "dm_air_quality_overview_hourly",
    "Ngày": "dm_air_quality_overview_daily",
    "Tháng": "dm_air_quality_overview_monthly",
}

def get_source_mix(source: str) -> str:
    """Map source dropdown value to the unified source_mix database value."""
    if source == "openweather":
        return "modeled"
    return "observed"

def get_source_table(spatial_grain: str, time_grain: str, source: str = "blended") -> str:
    """Return the analytical table name for the given selection and source."""
    return TIME_GRAIN_TABLE.get(time_grain, "dm_air_quality_overview_daily")

def get_pollutant_col(pollutant: str, standard: str = "VN_AQI") -> str:
    """Map pollutant filter to database column name."""
    if pollutant == "aqi":
        return "avg_aqi_vn" if standard == "VN_AQI" else "avg_aqi_us"
    mapping = {
        "pm25": "pm25_avg",
        "pm10": "pm10_avg",
        "co": "co_avg",
        "no2": "no2_avg",
        "so2": "so2_avg",
        "o3": "o3_avg"
    }
    return mapping.get(pollutant, "avg_aqi_vn")

def get_pollutant_cols(pollutant: str, standard: str = "VN_AQI") -> tuple[str, str]:
    """Map pollutant filter to (avg_col, max_col) names.
    Safely handles the fact that dm_* tables only store max columns for AQI.
    """
    avg_col = get_pollutant_col(pollutant, standard)
    if pollutant == "aqi":
        max_col = avg_col.replace("avg", "max")
    else:
        max_col = avg_col
    return avg_col, max_col

SOURCE_MIX_LABELS = {
    "observed": {"vi": "Quan trắc", "en": "Observed"},
    "mixed": {"vi": "Kết hợp", "en": "Mixed"},
    "modeled": {"vi": "Mô hình", "en": "Modeled"},
}

CONFIDENCE_LABELS = {
    "high": {"vi": "Tin cậy cao", "en": "High confidence"},
    "medium": {"vi": "Tin cậy vừa", "en": "Medium confidence"},
    "low": {"vi": "Ước tính mô hình", "en": "Modeled estimate"},
}

CONFIDENCE_COLORS = {
    "high": "#16a34a",
    "medium": "#f59e0b",
    "low": "#ef4444",
}

POLLUTANT_LABELS = {
    "aqi": "AQI",
    "pm25": "PM2.5",
    "pm10": "PM10",
    "no2": "NO2",
    "so2": "SO2",
    "o3": "O3",
    "co": "CO",
}

def build_where_clause(
    spatial_scope: str,
    spatial_value: str,
    date_range=None,
    date_col: str = "date",
    time_unit: str = "day",
    source_mix: str = None,
    source: str = None,
):
    """Construct dynamic WHERE clause based on hierarchical filters."""
    clauses = []
    if source_mix:
        clauses.append(f"source_mix = '{escape_value(source_mix)}'")
    if source:
        clauses.append(f"source = '{escape_value(source)}'")

    if spatial_scope == "Vùng" and spatial_value:
        clauses.append(f"region_3 = '{escape_value(spatial_value)}'")
    elif spatial_scope == "Khu vực" and spatial_value:
        clauses.append(f"region_8 = '{escape_value(spatial_value)}'")
    elif spatial_scope in ["Tỉnh", "Phường"] and spatial_value:
        clauses.append(f"province = '{escape_value(spatial_value)}'")

    if date_range:
        if time_unit == "hour":
            formatted = []
            for d in date_range:
                if hasattr(d, "strftime"):
                    formatted.append(d.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    formatted.append(escape_value(d))
            if len(formatted) == 2:
                clauses.append(
                    f"datetime_hour BETWEEN toDateTime('{formatted[0]}') "
                    f"AND toDateTime('{formatted[1]}')"
                )
            elif len(formatted) == 1:
                clauses.append(f"datetime_hour = toDateTime('{formatted[0]}')")
        else:
            formatted = []
            for d in date_range:
                if hasattr(d, "strftime"):
                    formatted.append(d.strftime("%Y-%m-%d"))
                else:
                    formatted.append(escape_value(d)[:10])
            if len(formatted) == 2:
                start_date, end_date = formatted
                clauses.append(f"{date_col} BETWEEN '{start_date}' AND '{end_date}'")
            elif len(formatted) == 1:
                clauses.append(f"{date_col} = '{formatted[0]}'")

    return " AND ".join(clauses) if clauses else "1=1"

def build_date_comparison_ranges(date_range, time_grain: str = "Ngày"):
    """Calculate date ranges for comparison (current vs previous period)."""
    from datetime import datetime, timedelta
    import pandas as pd
    
    if not date_range or len(date_range) < 1:
        latest_date = datetime.now().date()
        date_range = [latest_date - timedelta(days=7), latest_date]
        
    d1 = date_range[0]
    d2 = date_range[1] if len(date_range) > 1 else date_range[0]
    
    if hasattr(d1, "date"):
        d1 = d1.date()
    if hasattr(d2, "date"):
        d2 = d2.date()
        
    days_diff = (d2 - d1).days + 1
    days_cap = min(days_diff, 30)
    
    curr_start = d2 - timedelta(days=days_cap - 1)
    curr_end = d2
    prev_start = curr_start - timedelta(days=days_cap)
    prev_end = curr_start - timedelta(days=1)
    
    if time_grain == "Giờ":
        curr_start_str = curr_start.strftime("%Y-%m-%d 00:00:00")
        curr_end_str = curr_end.strftime("%Y-%m-%d 23:59:59")
        prev_start_str = prev_start.strftime("%Y-%m-%d 00:00:00")
        prev_end_str = prev_end.strftime("%Y-%m-%d 23:59:59")
        between_expr_curr = f"datetime_hour BETWEEN toDateTime('{curr_start_str}') AND toDateTime('{curr_end_str}')"
        between_expr_prev = f"datetime_hour BETWEEN toDateTime('{prev_start_str}') AND toDateTime('{prev_end_str}')"
    else:
        curr_start_str = curr_start.strftime("%Y-%m-%d")
        curr_end_str = curr_end.strftime("%Y-%m-%d")
        prev_start_str = prev_start.strftime("%Y-%m-%d")
        prev_end_str = prev_end.strftime("%Y-%m-%d")
        between_expr_curr = f"date BETWEEN '{curr_start_str}' AND '{curr_end_str}'"
        between_expr_prev = f"date BETWEEN '{prev_start_str}' AND '{prev_end_str}'"
        
    return {
        "curr_start": curr_start,
        "curr_end": curr_end,
        "prev_start": prev_start,
        "prev_end": prev_end,
        "between_expr_curr": between_expr_curr,
        "between_expr_prev": between_expr_prev,
    }
