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
    
    # Robust correlation calculation to avoid NAN (check variance)
    valid_corr_df = df.dropna(subset=['avg_congestion', 'avg_pm25'])
    has_variance = len(valid_corr_df) > 5 and valid_corr_df['avg_congestion'].std() > 0 and valid_corr_df['avg_pm25'].std() > 0
    
    if has_variance:
        corr = valid_corr_df[['avg_congestion', 'avg_pm25']].corr().iloc[0,1]
    else:
        corr = 0.0

    # Traffic Density (%)
    avg_traffic = df.avg_congestion.mean()
    traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"

    with c1:
        render_metric_card(t("nav_traffic", lang), traffic_display, icon="traffic")
    with c2:
        corr_label = "Correlation" if lang == "en" else "Tương quan"
        render_metric_card(corr_label, f"{corr:.2f}" if has_variance else "N/A", icon="insights")
    with c3:
        peak_label = "Peak PM2.5" if lang == "en" else "PM2.5 Đỉnh điểm"
        render_metric_card(peak_label, f"{df.avg_pm25.max():.1f}", icon="error")

    st.markdown("---")
    
    # Chart 1: Time Series Correlation
    timeseries_label = "Hourly Trend: Traffic & PM2.5" if lang == "en" else "Tương quan theo giờ: Giao thông & PM2.5"
    st.subheader(timeseries_label)
    fig = px.line(df, x="datetime_hour", y=["avg_congestion", "avg_pm25"], 
                 labels={"value": "Level", "datetime_hour": "Time"})
    fig.update_layout(get_plotly_layout())
    st.plotly_chart(fig, use_container_width=True)

    # Chart 2: Scatter Plot
    analysis_label = "Correlation Depth Analysis" if lang == "en" else "Phân tích mức độ tương quan"
    st.subheader(analysis_label)
    
    # Only show trendline if we have enough data to avoid px error
    fig_scatter = px.scatter(
        df, x="avg_congestion", y="avg_pm25", color="province",
        trendline="ols" if has_variance else None,
        labels={"avg_congestion": "Congestion", "avg_pm25": "PM2.5"}
    )
    fig_scatter.update_layout(get_plotly_layout())
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info(t("status_no_data", lang))
