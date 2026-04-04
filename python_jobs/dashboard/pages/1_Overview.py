import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df
from lib.aqi_utils import get_aqi_category, get_aqi_color, get_aqi_color_name

st.header("📊 AQI Overview")

# Sidebar filters
st.sidebar.header("Filters")
cities = st.sidebar.multiselect(
    "Cities",
    options=["Hanoi", "Ho Chi Minh City", "Da Nang", "Hai Phong", "Can Tho", "Others"],
    default=["Hanoi", "Ho Chi Minh City", "Da Nang"],
)
days = st.sidebar.slider("Days", min_value=7, max_value=90, value=30)
refresh = st.sidebar.button("🔄 Refresh")

# Cache TTL = 5 minutes
@st.cache_data(ttl=300)
def get_summary_stats():
    q = """
    SELECT
        count(distinct unified_station_id) as total_stations,
        avg(avg_aqi) as overall_avg_aqi,
        sum(if(avg_aqi <= 50, 1, 0)) as good_days,
        sum(if(avg_aqi > 50, 1, 0)) as bad_days
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL 30 DAY
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_trend_data(days: int):
    q = f"""
    SELECT
        date,
        avg(aqi_value) as avg_aqi
    FROM air_quality.mart_analytics__trends
    WHERE date >= today() - INTERVAL {days} DAY
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_city_aqi(days: int):
    q = f"""
    SELECT
        city,
        avg(avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi
    FROM air_quality.mart_analytics__geographic
    WHERE date >= today() - INTERVAL {days} DAY
      AND city IS NOT NULL AND city != ''
    GROUP BY city
    ORDER BY avg_aqi DESC
    LIMIT 10
    """
    return query_df(q)


try:
    # Metric cards
    stats = get_summary_stats()
    if not stats.empty:
        row = stats.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Stations", int(row.total_stations))
        col2.metric("Avg AQI (30d)", f"{row.overall_avg_aqi:.0f}")
        col3.metric("Good Days", int(row.good_days))
        col4.metric("Bad Days", int(row.bad_days))

    # Trend chart
    st.subheader(f"AQI Trend — Last {days} Days")
    trend = get_trend_data(days)
    if not trend.empty:
        st.line_chart(trend.set_index("date")["avg_aqi"], height=300)
    else:
        st.info("No trend data available for selected period.")

    # City comparison
    st.subheader("Top 10 Cities by Average AQI")
    city_aqi = get_city_aqi(days)
    if not city_aqi.empty:
        st.bar_chart(city_aqi.set_index("city")["avg_aqi"], horizontal=True, height=400)
    else:
        st.info("No city data available.")

except Exception as e:
    st.error(f"Query failed: {e}")
    st.info("Make sure ClickHouse is running and dbt has materialized the mart tables.")
