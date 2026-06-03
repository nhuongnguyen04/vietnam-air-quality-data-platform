import streamlit as st
import pandas as pd
from lib.clickhouse_client import query_df
from .core import build_where_clause

@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(dates, grain, scope, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"

    q = f"""
    SELECT
        toHour(toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh')) as hour_val,
        avg(avg_congestion) as avg_congestion,
        avg({target_col}) as avg_p
    FROM air_quality.dm_traffic_hourly_trend
    WHERE {where_clause}
    GROUP BY hour_val
    ORDER BY hour_val
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_stats(grain: str, scope: str | None = None, dates=None):
    where_clause = build_where_clause(grain, scope, dates)
    q = f"""
    SELECT
        avg(pm25_daily_avg) as avg_pm25,
        avg(congestion_daily_avg) as avg_congestion,
        avg(pm25_congestion_uplift) as avg_pm25_uplift,
        avg(traffic_pollution_impact_score) as avg_comovement_score,
        avg(avg_traffic_coverage_ratio) as avg_traffic_coverage_ratio,
        sum(total_hours) as observed_hours,
        sum(low_congestion_hours) as low_congestion_hours,
        sum(high_congestion_hours) as high_congestion_hours,
        countIf(pm25_congestion_uplift IS NOT NULL) as uplift_sample_days
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause}
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_ranking_data(grain: str, scope: str | None = None, dates=None, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"

    if grain in ["Tỉnh", "Phường"] and scope:
        ward_where_clause = build_where_clause(grain, scope, dates, date_col="t.date").replace("province =", "a.province =")
        q = f"""
        WITH ward_traffic AS (
            SELECT
                ward_code,
                toStartOfHour(timestamp_utc) as datetime_hour,
                toDate(timestamp_utc) as date,
                avg(value) as avg_congestion
            FROM air_quality.stg_tomtom__flow
            GROUP BY ward_code, datetime_hour, date
        )
        SELECT
            any(a.ward_name) as label_col,
            any(case
                when a.province IN ('Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
                when a.province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
                else 'Rural'
            end) as location_type,
            avg(h.{target_col} * t.avg_congestion) as impact_score
        FROM air_quality.dim_administrative_units a
        INNER JOIN ward_traffic t
            ON a.ward_code = t.ward_code
        INNER JOIN air_quality.dm_traffic_hourly_trend h
            ON a.province = h.province
            AND t.datetime_hour = h.datetime_hour
        WHERE {ward_where_clause}
            AND a.province != ''
            AND a.ward_code != ''
            AND h.{target_col} IS NOT NULL
            AND t.avg_congestion IS NOT NULL
            AND t.avg_congestion > 0
        GROUP BY a.ward_code
        ORDER BY impact_score DESC
        LIMIT 12
        """
        return query_df(q)

    q = f"""
    SELECT
        province as label_col,
        any(location_type) as location_type,
        avg({target_col}_daily_avg * congestion_daily_avg) as impact_score
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause} AND province != ''
    GROUP BY label_col
    ORDER BY impact_score DESC
    LIMIT 12
    """
    return query_df(q)
