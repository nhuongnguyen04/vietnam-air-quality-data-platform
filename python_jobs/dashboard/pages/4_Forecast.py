import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df

st.header("📈 Forecast Accuracy")

st.caption(
    "Compares AQICN forecast AQI against actual measured AQI. "
    "Requires raw_aqicn_forecast data ingestion to populate."
)

@st.cache_data(ttl=300)
def get_forecast_metrics():
    q = """
    SELECT
        round(avg(mae), 2) as mae,
        round(avg(rmse), 2) as rmse,
        round(avg(forecast_accuracy_score), 1) as avg_accuracy
    FROM air_quality.mart_analytics__forecast_accuracy
    WHERE forecast_date >= today() - INTERVAL 30 DAY
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_forecast_trend():
    q = """
    SELECT
        forecast_date as date,
        avg(actual_value) as actual_aqi
    FROM air_quality.mart_analytics__forecast_accuracy
    WHERE forecast_date >= today() - INTERVAL 30 DAY
    GROUP BY forecast_date
    ORDER BY forecast_date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_forecast_by_station():
    q = """
    SELECT
        station_id,
        round(avg(mae), 2) as mae,
        round(avg(rmse), 2) as rmse,
        round(avg(forecast_accuracy_score), 1) as accuracy_score
    FROM air_quality.mart_analytics__forecast_accuracy
    WHERE forecast_date >= today() - INTERVAL 30 DAY
    GROUP BY station_id
    ORDER BY accuracy_score DESC
    LIMIT 20
    """
    return query_df(q)


try:
    metrics = get_forecast_metrics()
    if not metrics.empty:
        row = metrics.iloc[0]
        col1, col2, col3 = st.columns(3)
        col1.metric("Mean Absolute Error (MAE)", f"{row.mae:.1f}")
        col2.metric("Root Mean Square Error (RMSE)", f"{row.rmse:.1f}")
        col3.metric("Avg Accuracy Score", f"{row.avg_accuracy:.0f}%")
    else:
        st.warning(
            "No forecast data available. Run AQICN forecast ingestion first:\n"
            "`python python_jobs/jobs/aqicn/ingest_forecast.py --mode incremental`"
        )

    st.subheader("Actual AQI Over Time (30d)")
    trend = get_forecast_trend()
    if not trend.empty:
        st.line_chart(trend.set_index("date"), height=300)
    else:
        st.info("No forecast trend data available.")

    st.subheader("Accuracy by Station")
    by_station = get_forecast_by_station()
    if not by_station.empty:
        st.dataframe(by_station.rename(columns={
            "station_id": "Station",
            "mae": "MAE",
            "rmse": "RMSE",
            "accuracy_score": "Accuracy %",
        }).set_index("Station"), use_container_width=True)
    else:
        st.info("No per-station forecast accuracy data available.")

except Exception as e:
    st.error(f"Query failed: {e}")
