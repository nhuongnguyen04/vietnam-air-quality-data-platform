import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df
from lib.aqi_utils import get_aqi_category

st.header("🧪 Pollutant Analysis")

pollutants = st.multiselect(
    "Pollutants",
    options=["pm25", "pm10", "o3", "no2", "co", "so2"],
    default=["pm25", "pm10", "o3"],
    format_func=lambda x: {"pm25": "PM2.5", "pm10": "PM10", "o3": "Ozone (O\u2083)", "no2": "NO\u2082", "co": "CO", "so2": "SO\u2082"}.get(x, x),
)

@st.cache_data(ttl=300)
def get_exceedance_trend(pollutant_list):
    if not pollutant_list:
        return pd.DataFrame()
    pollutant_filter = "', '".join(pollutant_list)
    q = f"""
    SELECT
        date,
        pollutant,
        exceedance_rate
    FROM air_quality.mart_kpis__pollutant_concentrations
    WHERE date >= today() - INTERVAL 30 DAY
      AND pollutant IN ('{pollutant_filter}')
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_dominant_pollutants():
    q = """
    SELECT
        dominant_pollutant,
        count(*) as days
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL 30 DAY
      AND dominant_pollutant IS NOT NULL
    GROUP BY dominant_pollutant
    ORDER BY days DESC
    LIMIT 6
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_aqi_distribution():
    q = """
    SELECT
        case
            when avg_aqi <= 50 then 'Good'
            when avg_aqi <= 100 then 'Moderate'
            when avg_aqi <= 150 then 'Unhealthy for Sensitive Groups'
            when avg_aqi <= 200 then 'Unhealthy'
            when avg_aqi <= 300 then 'Very Unhealthy'
            else 'Hazardous'
        end as category,
        count(*) as days
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL 30 DAY
    GROUP BY category
    ORDER BY days DESC
    """
    return query_df(q)


try:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Pollutant Exceedance Rate (30d)")
        exc = get_exceedance_trend(pollutants)
        if not exc.empty:
            pivot = exc.pivot(index="date", columns="pollutant", values="exceedance_rate")
            st.bar_chart(pivot, height=300)
        else:
            st.info("No pollutant concentration data available.")

    with col2:
        st.subheader("Dominant Pollutants")
        dom = get_dominant_pollutants()
        if not dom.empty:
            st.bar_chart(dom.set_index("dominant_pollutant"), horizontal=True, height=300)
        else:
            st.info("No dominant pollutant data.")

    st.subheader("AQI Category Distribution (30d)")
    dist = get_aqi_distribution()
    if not dist.empty:
        st.bar_chart(dist.set_index("category"), horizontal=True, height=300)
    else:
        st.info("No AQI distribution data.")

except Exception as e:
    st.error(f"Query failed: {e}")
