import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df

st.header("🔗 Source Comparison")

st.info(
    "Comparing AQI readings from multiple data sources: "
    "AQI.in (~540 Vietnam monitoring stations) and OpenWeather (62 Vietnam provinces)."
)

sources = st.multiselect(
    "Data Sources",
    options=["aqiin", "openweather"],
    default=["aqiin", "openweather"],
    format_func=lambda x: {"aqiin": "AQI.in", "openweather": "OpenWeather"}.get(x, x),
)

@st.cache_data(ttl=300)
def get_source_trends(source_list):
    if not source_list:
        return pd.DataFrame()
    source_filter = "', '".join(source_list)
    q = f"""
    SELECT
        date,
        source,
        avg(avg_aqi) as avg_aqi
    FROM air_quality.mart_air_quality__daily_summary
    WHERE date >= today() - INTERVAL 30 DAY
      AND source IN ('{source_filter}')
    GROUP BY date, source
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_stats(source_list):
    if not source_list:
        return pd.DataFrame()
    source_filter = "', '".join(source_list)
    q = f"""
    SELECT
        source,
        round(avg(val), 1) as avg_aqi,
        count(*) as total_days,
        sum(if(val <= 50, 1, 0)) as good_days
    FROM (
        SELECT source, avg_aqi as val
        FROM air_quality.mart_air_quality__daily_summary
        WHERE source IN ('{source_filter}')
    )
    GROUP BY source
    """
    return query_df(q)


try:
    st.subheader("AQI by Source (30d)")
    trends = get_source_trends(sources)
    if not trends.empty:
        pivot = trends.pivot(index="date", columns="source", values="avg_aqi")
        st.line_chart(pivot, height=300)
    else:
        st.info("No source data available for selected sources.")

    st.subheader("Source Statistics")
    stats = get_source_stats(sources)
    if not stats.empty:
        stats_display = stats.rename(columns={
            "source": "Source",
            "avg_aqi": "Avg AQI",
            "total_days": "Total Days",
            "good_days": "Good Days",
        })
        stats_display["Good %"] = (
            stats_display["Good Days"] / stats_display["Total Days"] * 100
        ).round(1).astype(str) + "%"
        st.dataframe(stats_display.set_index("Source"), use_container_width=True)
    else:
        st.info("No source statistics available.")

except Exception as e:
    st.error(f"Query failed: {e}")
