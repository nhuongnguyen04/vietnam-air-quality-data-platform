import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df
from lib.aqi_utils import get_aqi_category, get_aqi_color, get_aqi_color_name

st.header("📈 Historical AQI Trend")

st.caption("Historical AQI trends from actual measurements — no forecast data required.")

# Sidebar filters
st.sidebar.header("Filters")
cities = st.sidebar.multiselect(
    "Cities",
    options=["Hanoi", "Ho Chi Minh City", "Da Nang", "Hai Phong", "Can Tho", "Others"],
    default=["Hanoi", "Ho Chi Minh City", "Da Nang"],
)
days = st.sidebar.select_slider("Period", options=[7, 30, 90], value=30)
refresh = st.sidebar.button("🔄 Refresh")


@st.cache_data(ttl=300)
def get_trend_data(days: int):
    q = f"""
    SELECT
        date,
        avg(avg_aqi) as avg_aqi,
        min(min_aqi) as min_aqi,
        max(max_aqi) as max_aqi
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL {days} DAY
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_city_trend(days: int):
    q = f"""
    SELECT
        date,
        city,
        avg(avg_aqi) as avg_aqi
    FROM air_quality.mart_air_quality__dashboard
    WHERE date >= today() - INTERVAL {days} DAY
      AND city IS NOT NULL AND city != ''
    GROUP BY date, city
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_top_cities(days: int):
    q = f"""
    SELECT
        city,
        round(avg(avg_aqi), 1) as avg_aqi,
        round(max(max_aqi), 1) as max_aqi,
        count(*) as days
    FROM air_quality.mart_air_quality__dashboard
    WHERE date >= today() - INTERVAL {days} DAY
      AND city IS NOT NULL AND city != ''
    GROUP BY city
    ORDER BY avg_aqi DESC
    LIMIT 10
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_category_distribution(days: int):
    q = f"""
    SELECT
        case
            when avg_aqi <= 50 then 'Good (0-50)'
            when avg_aqi <= 100 then 'Moderate (51-100)'
            when avg_aqi <= 150 then 'Unhealthy for Sensitive (101-150)'
            when avg_aqi <= 200 then 'Unhealthy (151-200)'
            when avg_aqi <= 300 then 'Very Unhealthy (201-300)'
            else 'Hazardous (>300)'
        end as aqi_category,
        count(*) as days
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL {days} DAY
    GROUP BY aqi_category
    ORDER BY days DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_overall_stats(days: int):
    q = f"""
    SELECT
        count(distinct date) as total_days,
        round(avg(avg_aqi), 1) as overall_avg,
        round(min(min_aqi), 1) as overall_min,
        round(max(max_aqi), 1) as overall_max
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL {days} DAY
    """
    return query_df(q)


try:
    stats = get_overall_stats(days)
    if not stats.empty:
        row = stats.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Days Tracked", int(row.total_days))
        col2.metric(f"Avg AQI ({days}d)", f"{row.overall_avg:.0f}")
        col3.metric("Min AQI", f"{row.overall_min:.0f}")
        col4.metric("Max AQI", f"{row.overall_max:.0f}")

    st.subheader(f"AQI Trend — Last {days} Days")
    trend = get_trend_data(days)
    if not trend.empty:
        st.line_chart(trend.set_index("date"), height=300)
    else:
        st.info("No trend data available for selected period.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Cities by Avg AQI")
        top = get_top_cities(days)
        if not top.empty:
            st.bar_chart(top.set_index("city")["avg_aqi"], horizontal=True, height=400)
        else:
            st.info("No city data available.")

    with col2:
        st.subheader("AQI Category Distribution")
        dist = get_category_distribution(days)
        if not dist.empty:
            st.bar_chart(dist.set_index("aqi_category")["days"], height=400)
        else:
            st.info("No distribution data available.")

except Exception as e:
    st.error(f"Query failed: {e}")
    st.info("Make sure ClickHouse is running and dbt has materialized the mart tables.")
