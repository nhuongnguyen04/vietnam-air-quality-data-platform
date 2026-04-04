import streamlit as st
import pandas as pd
import sys
sys.path.insert(0, '..')
from lib.clickhouse_client import query_df

st.header("🚨 AQI Alerts")

st.caption(
    "Air quality threshold breaches from AQICN monitoring stations. "
    "High = AQI > 150, Very High = AQI > 200."
)

@st.cache_data(ttl=300)
def get_recent_alerts():
    q = """
    SELECT
        a.datetime_hour as triggered_at,
        a.station_id,
        a.source,
        a.dominant_pollutant as pollutant,
        a.normalized_aqi as aqi,
        a.threshold_breached,
        a.sensor_quality_tier,
        s.city,
        s.province
    FROM air_quality.mart_air_quality__alerts a
    LEFT JOIN air_quality.mart_air_quality__stations s
        ON a.station_id = s.station_id
    WHERE a.datetime_hour >= now() - INTERVAL 7 DAY
    ORDER BY a.datetime_hour DESC
    LIMIT 100
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_alert_counts():
    q = """
    SELECT
        threshold_breached,
        count(*) as count
    FROM air_quality.mart_air_quality__alerts
    WHERE datetime_hour >= now() - INTERVAL 7 DAY
    GROUP BY threshold_breached
    ORDER BY count DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_alert_timeline():
    q = """
    SELECT
        toDate(datetime_hour) as date,
        count(*) as alert_count
    FROM air_quality.mart_air_quality__alerts
    WHERE datetime_hour >= now() - INTERVAL 30 DAY
    GROUP BY toDate(datetime_hour)
    ORDER BY date
    """
    return query_df(q)


try:
    alerts = get_recent_alerts()

    if not alerts.empty:
        # Add alert level label
        alerts = alerts.copy()
        alerts["alert_level"] = alerts["threshold_breached"].map({
            "150": "High",
            "200": "Very High",
        })

        st.dataframe(
            alerts[[
                "triggered_at", "station_id", "city", "province",
                "pollutant", "aqi", "alert_level", "source",
            ]].rename(columns={
                "triggered_at": "Time",
                "station_id": "Station",
                "city": "City",
                "province": "Province",
                "pollutant": "Pollutant",
                "aqi": "AQI",
                "alert_level": "Level",
                "source": "Source",
            }),
            use_container_width=True,
        )
    else:
        st.success("No alerts in the last 7 days — AQI levels are normal.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Alerts by Threshold (7d)")
        counts = get_alert_counts()
        if not counts.empty:
            labels = counts.copy()
            labels["threshold_breached"] = labels["threshold_breached"].map({
                "150": "High (>150)",
                "200": "Very High (>200)",
            })
            st.bar_chart(labels.set_index("threshold_breached"), height=250)
        else:
            st.info("No alert data.")

    with col2:
        st.subheader("Alert Frequency (30d)")
        timeline = get_alert_timeline()
        if not timeline.empty:
            st.line_chart(timeline.set_index("date"), height=250)
        else:
            st.info("No alert timeline data.")

except Exception as e:
    st.error(f"Query failed: {e}")
