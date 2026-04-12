import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("traffic_title", lang))

@st.cache_data(ttl=300)
def get_traffic_correlation():
    q = """
    SELECT
        datetime_hour,
        province,
        avg(congestion_index) as avg_congestion,
        avg(pm25) as avg_pm25,
        avg(co) as avg_co
    FROM air_quality.dm_aqi_weather_traffic_unified
    WHERE datetime_hour >= now() - INTERVAL 7 DAY
    GROUP BY datetime_hour, province
    ORDER BY datetime_hour
    """
    return query_df(q)

df = get_traffic_correlation()

if not df.empty:
    # Top Metrics
    c1, c2, c3 = st.columns(3)
    
    # Robust correlation calculation to avoid NAN
    valid_corr_df = df.dropna(subset=['avg_congestion', 'avg_pm25'])
    if len(valid_corr_df) > 1:
        corr = valid_corr_df[['avg_congestion', 'avg_pm25']].corr().iloc[0,1]
    else:
        corr = 0.0

    with c1:
        render_metric_card(t("nav_traffic", lang) if lang=="en" else "Mật độ Giao thông", f"{df.avg_congestion.mean():.1%}", icon="traffic")
    with col2 if 'col2' in locals() else c2:
        render_metric_card(t("metric_dominant", lang) if lang=="en" else "Tương quan Ô nhiễm", f"{corr:.2f}", icon="insights")
    with col3 if 'col3' in locals() else c3:
        render_metric_card(t("metric_worst", lang) if lang=="en" else "PM2.5 Đỉnh điểm", f"{df.avg_pm25.max():.1f}", icon="error")

    st.markdown("---")
    
    # Chart 1: Time Series Correlation
    st.subheader(t("chart_aqi_dist", lang) if lang=="en" else "Tương quan theo giờ: Giao thông & PM2.5")
    fig = px.line(df, x="datetime_hour", y=["avg_congestion", "avg_pm25"], 
                 labels={"value": "Level", "datetime_hour": "Time"})
    fig.update_layout(get_plotly_layout())
    st.plotly_chart(fig, use_container_width=True)

    # Chart 2: Scatter Plot
    st.subheader(t("chart_top_polluted", lang) if lang=="en" else "Phân tích mức độ tương quan")
    fig_scatter = px.scatter(df, x="avg_congestion", y="avg_pm25", color="province", 
                            trendline="ols")
    fig_scatter.update_layout(get_plotly_layout())
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.warning("No traffic data found in ClickHouse.")
