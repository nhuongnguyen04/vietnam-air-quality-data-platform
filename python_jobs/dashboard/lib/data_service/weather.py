import streamlit as st
import pandas as pd
from lib.clickhouse_client import query_df
from .core import build_where_clause, escape_value

@st.cache_data(ttl=300)
def get_weather_summary_stats(grain: str, scope: str | None = None, dates=None, p_stag="p_stag", p_disp="p_disp", source_mix: str = 'observed'):
    where_clause = build_where_clause(grain, scope, dates)
    q = f"""
    WITH stats_cte AS (
        SELECT
            sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
            sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp,
            avg(stagnant_air_probability) as stagnant_prob,
            avg(wind_daily_avg) as avg_wind
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
    )
    SELECT
        *,
        (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
    FROM stats_cte
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_ranking_data(grain: str, scope: str | None = None, dates=None, p_stag="p_stag", p_disp="p_disp", p_col="pm25_daily_avg", source_mix: str = 'observed'):
    where_clause = build_where_clause(grain, scope, dates)

    if grain == "Phường":
        q = f"""
        WITH rank_cte AS (
            SELECT
                w.ward_code as label_key,
                any(coalesce(nullIf(d.ward_name, ''), w.ward_code)) as label_col,
                avg({p_col} / nullif(wind_daily_avg, 0)) as risk_index,
                sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
                sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp
            FROM air_quality.dm_weather_pollution_correlation_daily w
            LEFT JOIN air_quality.dim_administrative_units d ON w.ward_code = d.ward_code
            WHERE {where_clause} AND w.ward_code != '' AND w.source_mix = '{escape_value(source_mix)}'
            GROUP BY label_key
        )
        SELECT
            *,
            (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
        FROM rank_cte
        ORDER BY risk_index DESC
        LIMIT 10
        """
    else:
        q = f"""
        WITH rank_cte AS (
            SELECT
                province as label_col,
                avg({p_col} / nullif(wind_daily_avg, 0)) as risk_index,
                sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
                sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp
            FROM air_quality.dm_weather_pollution_correlation_daily
            WHERE {where_clause} AND province != '' AND source_mix = '{escape_value(source_mix)}'
            GROUP BY label_col
        )
        SELECT
            *,
            (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
        FROM rank_cte
        ORDER BY risk_index DESC
        LIMIT 10
        """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_trend_data(grain: str, scope: str | None = None, dates=None, col="pm25", time_grain="Ngày", source_mix: str = 'observed'):
    target_col = col if col in ["pm25", "pm10", "aqi_vn", "aqi_us"] else "pm25"

    if time_grain == "Giờ":
        where_clause = build_where_clause(grain, scope, dates, time_unit="hour")
        q = f"""
        SELECT
            toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as time_key,
            avg({target_col}) as avg_val,
            avg(wind_speed) as avg_wind,
            avg(humidity) as avg_hum,
            avg(temperature) as avg_temp
        FROM air_quality.dm_weather_hourly_trend
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    elif time_grain == "Ngày":
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(date as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(wind_daily_avg) as avg_wind,
            avg(humidity_daily_avg) as avg_hum,
            avg(temp_daily_avg) as avg_temp
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    else:
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(toStartOfMonth(date) as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(wind_daily_avg) as avg_wind,
            avg(humidity_daily_avg) as avg_hum,
            avg(temp_daily_avg) as avg_temp
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_correlation_data(grain: str, scope: str | None = None, dates=None, col="pm25", time_grain="Ngày", source_mix: str = 'observed'):
    target_col = col if col in ["pm25", "pm10", "aqi_vn", "aqi_us"] else "pm25"

    if time_grain == "Giờ":
        where_clause = build_where_clause(grain, scope, dates, time_unit="hour")
        q = f"""
        SELECT
            toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as time_key,
            avg({target_col}) as avg_val,
            avg(temperature) as avg_temp,
            avg(humidity) as avg_hum,
            avg(wind_speed) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_hourly_trend
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    elif time_grain == "Ngày":
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(date as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(temp_daily_avg) as avg_temp,
            avg(humidity_daily_avg) as avg_hum,
            avg(wind_daily_avg) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    else:
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(toStartOfMonth(date) as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(temp_daily_avg) as avg_temp,
            avg(humidity_daily_avg) as avg_hum,
            avg(wind_daily_avg) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND source_mix = '{escape_value(source_mix)}'
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    return query_df(q)
