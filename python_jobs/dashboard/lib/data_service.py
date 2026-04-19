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

def build_where_clause(spatial_scope: str, spatial_value: str, date_range=None):
    """Construct dynamic WHERE clause based on hierarchical filters."""
    clauses = []
    
    if spatial_scope == "Vùng" and spatial_value:
        clauses.append(f"region_3 = '{spatial_value}'")
    elif spatial_scope == "Khu vực" and spatial_value:
        clauses.append(f"region_8 = '{spatial_value}'")
    elif spatial_scope in ["Tỉnh", "Phường"] and spatial_value:
        clauses.append(f"province = '{spatial_value}'")
        
    if date_range:
        if len(date_range) == 2:
            start_date, end_date = date_range
            clauses.append(f"toStartOfDay(date) BETWEEN '{start_date}' AND '{end_date}'")
        elif len(date_range) == 1:
            clauses.append(f"toStartOfDay(date) = '{date_range[0]}'")
        
    return " AND ".join(clauses) if clauses else "1=1"
