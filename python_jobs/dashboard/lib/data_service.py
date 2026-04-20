"""Data service for hierarchical filtering and optimal source selection.
Redirected to Analytics-First Layer (dm_* tables).
"""
import streamlit as st
from .clickhouse_client import query_df
import pandas as pd

# Mapping of spatio-temporal grains to dbt ANALYTICS models (dm_*)
SOURCE_MATRIX = {
    # Provincial level dashboards use province summary or overview
    ("Toàn quốc", "Giờ"): "dm_air_quality_overview_hourly",
    ("Toàn quốc", "Ngày"): "dm_air_quality_overview_daily",
    ("Toàn quốc", "Tháng"): "dm_air_quality_overview_monthly",
    
    ("Tỉnh", "Giờ"): "dm_air_quality_overview_hourly",
    ("Tỉnh", "Ngày"): "dm_air_quality_overview_daily",
    
    # Ward level dashboards
    ("Phường", "Giờ"): "dm_air_quality_overview_hourly",
    ("Phường", "Ngày"): "dm_air_quality_overview_daily",
}

def get_source_table(spatial_grain: str, time_grain: str) -> str:
    """Return the analytical table name for the given selection."""
    return SOURCE_MATRIX.get((spatial_grain, time_grain), "dm_air_quality_overview_daily")

@st.cache_data(ttl=3600)
def get_hierarchy_metadata():
    """Fetch regions and provinces from the centralized Dimension table."""
    q = """
    SELECT DISTINCT
        region_3,
        region_8,
        province
    FROM air_quality.dim_administrative_units
    ORDER BY region_3, region_8, province
    """
    return query_df(q)

def get_ward_list(province: str):
    """Fetch list of wards from the centralized Dimension table."""
    q = f"SELECT DISTINCT ward_code, ward_name FROM air_quality.dim_administrative_units WHERE province = '{province}' ORDER BY ward_name"
    return query_df(q)

def get_pollutant_col(pollutant: str, standard: str = "TCVN") -> str:
    """Map pollutant filter to database column name."""
    if pollutant == "aqi":
        # Handle "WHO 2021" or other non-TCVN labels from app.py
        return "avg_aqi_vn" if standard == "TCVN" else "avg_aqi_us"
    
    # For now, return the _avg concentrations. 
    # In future, we might want _aqi versions for each pollutant.
    mapping = {
        "pm25": "pm25_avg",
        "pm10": "pm10_avg",
        "co": "co_avg",
        "no2": "no2_avg",
        "so2": "so2_avg",
        "o3": "o3_avg"
    }
    return mapping.get(pollutant, "avg_aqi_vn")

def get_pollutant_cols(pollutant: str, standard: str = "TCVN") -> tuple[str, str]:
    """Map pollutant filter to (avg_col, max_col) names.
    Safely handles the fact that dm_* tables only store max columns for AQI.
    """
    avg_col = get_pollutant_col(pollutant, standard)
    
    if pollutant == "aqi":
        # AQI has both avg and max columns in dm_* tables
        max_col = avg_col.replace("avg", "max")
    else:
        # Specific pollutants (PM25, etc) only have averages in dm_* summary tables
        max_col = avg_col
        
    return avg_col, max_col


def build_where_clause(spatial_scope: str, spatial_value: str, date_range=None, date_col="date"):
    """Construct dynamic WHERE clause based on hierarchical filters."""
    clauses = []
    
    if spatial_scope == "Vùng" and spatial_value:
        clauses.append(f"region_3 = '{spatial_value}'")
    elif spatial_scope == "Khu vực" and spatial_value:
        clauses.append(f"region_8 = '{spatial_value}'")
    elif spatial_scope in ["Tỉnh", "Phường"] and spatial_value:
        clauses.append(f"province = '{spatial_value}'")
        
    if date_range:
        # Ensure dates are strings in YYYY-MM-DD format for ClickHouse
        formatted_dates = []
        for d in date_range:
            if hasattr(d, "strftime"):
                formatted_dates.append(d.strftime("%Y-%m-%d"))
            else:
                formatted_dates.append(str(d))

        if len(formatted_dates) == 2:
            start_date, end_date = formatted_dates
            clauses.append(f"{date_col} BETWEEN '{start_date}' AND '{end_date}'")
        elif len(formatted_dates) == 1:
            clauses.append(f"{date_col} = '{formatted_dates[0]}'")
        
    return " AND ".join(clauses) if clauses else "1=1"
