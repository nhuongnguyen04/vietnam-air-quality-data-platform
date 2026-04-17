"""Data service for hierarchical filtering and optimal source selection."""
import streamlit as st
from .clickhouse_client import query_df
import pandas as pd

# Mapping of spatio-temporal grains to dbt source tables
SOURCE_MATRIX = {
    ("Toàn quốc", "Giờ"): "fct_air_quality_province_level_hourly",
    ("Toàn quốc", "Ngày"): "fct_air_quality_province_level_daily",
    ("Toàn quốc", "Tháng"): "fct_air_quality_province_level_monthly",
    ("Vùng", "Giờ"): "fct_air_quality_province_level_hourly",
    ("Vùng", "Ngày"): "fct_air_quality_province_level_daily",
    ("Vùng", "Tháng"): "fct_air_quality_province_level_monthly",
    ("Tỉnh", "Giờ"): "fct_air_quality_province_level_hourly",
    ("Tỉnh", "Ngày"): "fct_air_quality_province_level_daily",
    ("Tỉnh", "Tháng"): "fct_air_quality_province_level_monthly",
    ("Phường", "Giờ"): "fct_air_quality_ward_level_hourly",
    ("Phường", "Ngày"): "fct_air_quality_ward_level_daily",
    ("Phường", "Tháng"): "fct_air_quality_ward_level_monthly",
}

def get_source_table(spatial_grain: str, time_grain: str) -> str:
    """Return the optimal table name for the given selection."""
    return SOURCE_MATRIX.get((spatial_grain, time_grain), "fct_air_quality_province_level_daily")

@st.cache_data(ttl=3600)
def get_hierarchy_metadata():
    """Fetch complete list of regions, sub-regions, and provinces for filters."""
    q = """
    SELECT DISTINCT
        region_3,
        region_8,
        province
    FROM air_quality.fct_air_quality_province_level_daily
    ORDER BY region_3, region_8, province
    """
    return query_df(q)

def get_ward_list(province: str):
    """Fetch list of wards for a specific province."""
    q = f"SELECT DISTINCT ward_code, ward_name FROM air_quality.stg_core__administrative_units WHERE province = '{province}' ORDER BY ward_name"
    return query_df(q)

def build_where_clause(spatial_scope: str, spatial_value: str, date_range=None):
    """Construct dynamic WHERE clause based on hierarchical filters."""
    clauses = []
    
    if spatial_scope == "Vùng" and spatial_value:
        clauses.append(f"region_3 = '{spatial_value}'")
    elif spatial_scope == "Khu vực" and spatial_value:
        clauses.append(f"region_8 = '{spatial_value}'")
    elif spatial_scope in ["Tỉnh", "Phường"] and spatial_value:
        # province is always available at these levels
        clauses.append(f"province = '{spatial_value}'")
        
    if date_range:
        if len(date_range) == 2:
            start_date, end_date = date_range
            clauses.append(f"toStartOfDay(date) BETWEEN '{start_date}' AND '{end_date}'")
        elif len(date_range) == 1:
            clauses.append(f"toStartOfDay(date) = '{date_range[0]}'")
        
    return " AND ".join(clauses) if clauses else "1=1"
