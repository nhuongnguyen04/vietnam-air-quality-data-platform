import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("weather_title", lang))

@st.cache_data(ttl=300)
def get_weather_correlation():
    # Only select rows where weather data exists (temp > 0 or humidity > 0 as proxies)
    q = """
    SELECT
        datetime_hour,
        province,
        avg(temp) as avg_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind,
        avg(pm25) as avg_pm25
    FROM air_quality.dm_aqi_weather_traffic_unified
    WHERE datetime_hour >= now() - INTERVAL 7 DAY
      AND (temp > 0 OR humidity > 0)
    GROUP BY datetime_hour, province
    ORDER BY datetime_hour
    """
    return query_df(q)

df = get_weather_correlation()

if not df.empty:
    # Top Metrics
    c1, c2, c3 = st.columns(3)
    
    avg_temp = df.avg_temp.mean()
    avg_hum = df.avg_humidity.mean()
    avg_wind = df.avg_wind.mean()

    with c1:
        temp_label = "Avg Temp" if lang == "en" else "Nhiệt độ TB"
        render_metric_card(temp_label, f"{avg_temp:.1f}°C", icon="device_thermostat")
    with c2:
        hum_label = "Humidity" if lang == "en" else "Độ ẩm TB"
        render_metric_card(hum_label, f"{avg_hum:.1f}%", icon="humidity_percentage")
    with c3:
        wind_label = "Wind Speed" if lang == "en" else "Tốc độ Gió"
        render_metric_card(wind_label, f"{avg_wind:.1f} m/s", icon="air")

    st.markdown("---")
    
    # Section 1: Wind Dispersal Analysis
    wind_title = "Wind Speed vs PM2.5 (Dispersal Effect)" if lang == "en" else "Tác động của Gió: Phân tán ô nhiễm"
    st.subheader(wind_title)
    
    # Calculate correlation for wind
    wind_corr = df[['avg_wind', 'avg_pm25']].corr().iloc[0,1]
    
    fig_wind = px.scatter(
        df, x="avg_wind", y="avg_pm25", color="province",
        trendline="ols",
        labels={"avg_wind": "Wind Speed (m/s)", "avg_pm25": "PM2.5"}
    )
    fig_wind.update_layout(get_plotly_layout())
    st.plotly_chart(fig_wind, use_container_width=True)
    
    st.info(
        "Giảm ô nhiễm nhờ gió:" if lang == "vi" else "Pollution dispersal by wind:",
        icon="ℹ️"
    )
    st.write(f"Correlation: **{wind_corr:.2f}**")

    st.markdown("---")

    # Section 2: Humidity Stagnation Analysis
    hum_title = "Humidity vs PM2.5 (Trap Effect)" if lang == "en" else "Tác động của Độ ẩm: Tụ khói bụi"
    st.subheader(hum_title)
    
    hum_corr = df[['avg_humidity', 'avg_pm25']].corr().iloc[0,1]
    
    fig_hum = px.scatter(
        df, x="avg_humidity", y="avg_pm25", color="province",
        trendline="ols",
        labels={"avg_humidity": "Humidity (%)", "avg_pm25": "PM2.5"}
    )
    fig_hum.update_layout(get_plotly_layout())
    st.plotly_chart(fig_hum, use_container_width=True)
    
    st.info(
        "Bẫy ô nhiễm do độ ẩm cao:" if lang == "vi" else "Pollution trapping due to high humidity:",
        icon="⚠️"
    )
    st.write(f"Correlation: **{hum_corr:.2f}**")

else:
    st.info(t("status_no_data", lang))
