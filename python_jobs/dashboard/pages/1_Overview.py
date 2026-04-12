import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang
theme = st.session_state.get("theme", "light")

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_national_summary():
    """Fetch aggregated national AQI metrics."""
    q = """
    SELECT
        avg(current_aqi_us) as national_avg,
        max(current_aqi_us) as max_aqi,
        count(DISTINCT province) as province_count,
        topK(1)(main_pollutant)[1] as dominant_pollutant
    FROM air_quality.dm_aqi_current_status
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_map_data():
    """Fetch geo-coded AQI data (using current status as base)."""
    # Note: Using current status for map to ensure consistency with metrics
    q = """
    SELECT
        province,
        district,
        current_aqi_us,
        pm25,
        main_pollutant
    FROM air_quality.dm_aqi_current_status
    """
    # Note: If geo data is missing in current_status, we might need a join. 
    # For now, assuming the table has what we need or we fallback.
    return query_df(q)

# ── UI Header ─────────────────────────────────────────────────────────────────
st.title(t("overview_title", lang))
st.markdown(f"{t('current_outlook', lang)}: **{st.session_state.standard} {t('standards', lang)}**")

# ── Row 1: KPI Cards ──────────────────────────────────────────────────────────
summary = get_national_summary()
col1, col2, col3, col4 = st.columns(4)

if not summary.empty:
    row = summary.iloc[0]
    with col1:
        render_metric_card(t("metric_national_avg", lang), f"{int(row.national_avg or 0)}", icon="insights")
    with col2:
        render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
    with col3:
        render_metric_card(t("metric_worst", lang), f"{int(row.max_aqi or 0)}", icon="error")
    with col4:
        render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")

# ── Row 2: Real-time Map ─────────────────────────────────────────────────────
st.subheader(t("map_title", lang))

# Re-fetching map data from verified unified table for coordinates
@st.cache_data(ttl=300)
def get_geo_map_data():
    q = """
    SELECT
        province,
        station_name,
        station_latitude as lat,
        station_longitude as lon,
        aqi_us as current_aqi_us,
        pm25
    FROM air_quality.dm_aqi_weather_traffic_unified
    WHERE datetime_hour = (SELECT max(datetime_hour) FROM air_quality.dm_aqi_weather_traffic_unified)
    """
    return query_df(q)

map_df = get_geo_map_data()

if not map_df.empty:
    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="current_aqi_us",
        size="pm25",
        hover_name="station_name",
        hover_data=["province", "current_aqi_us"],
        color_continuous_scale="RdYlGn_r",
        range_color=[0, 250],
        zoom=5,
        center={"lat": 16.0, "lon": 108.0},
    )
    
    map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
    fig_map.update_layout(
        mapbox_style=map_style,
        margin={"r":20, "t":30, "l":20, "b":20}, 
        height=600,
        coloraxis_showscale=True
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("No real-time station data available for the map.")

st.divider()

# ── Row 3: Analytics ──────────────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader(t("chart_aqi_dist", lang))
    q_dist = """
    SELECT 
        CASE 
            WHEN current_aqi_us <= 50 THEN 'Good'
            WHEN current_aqi_us <= 100 THEN 'Moderate'
            WHEN current_aqi_us <= 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN current_aqi_us <= 200 THEN 'Unhealthy'
            WHEN current_aqi_us <= 300 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END as aqi_category,
        count(*) as count 
    FROM air_quality.dm_aqi_current_status 
    GROUP BY aqi_category
    """
    df_dist = query_df(q_dist)
    if not df_dist.empty:
        category_colors = {
            'Good': '#00E400',
            'Moderate': '#FFFF00',
            'Unhealthy for Sensitive Groups': '#FF7E00',
            'Unhealthy': '#FF0000',
            'Very Unhealthy': '#8F3F97',
            'Hazardous': '#7E0023'
        }
        fig_pie = px.pie(df_dist, values='count', names='aqi_category', 
                        color='aqi_category',
                        color_discrete_map=category_colors)
        fig_pie.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_pie, use_container_width=True)

with c2:
    st.subheader(t("chart_top_polluted", lang))
    q_top = "SELECT province, current_aqi_us FROM air_quality.dm_aqi_current_status ORDER BY current_aqi_us DESC LIMIT 10"
    df_top = query_df(q_top)
    if not df_top.empty:
        fig_bar = px.bar(df_top, x='province', y='current_aqi_us', 
                        color='current_aqi_us',
                        color_continuous_scale='Reds',
                        labels={'current_aqi_us': 'AQI', 'province': 'Province'})
        fig_bar.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_bar, use_container_width=True)
