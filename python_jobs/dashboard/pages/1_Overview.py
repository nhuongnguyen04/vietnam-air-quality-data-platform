import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

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
    """Fetch geo-coded AQI data for the map."""
    q = """
    SELECT
        province,
        station_name,
        station_latitude,
        station_longitude,
        aqi_us,
        pm25,
        is_stagnant_air_risk
    FROM air_quality.dm_aqi_weather_traffic_unified
    WHERE datetime_hour = (SELECT max(datetime_hour) FROM air_quality.dm_aqi_weather_traffic_unified)
    """
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
        render_metric_card(t("metric_national_avg", lang), f"{int(row.national_avg)}", icon="insights")
    with col2:
        render_metric_card(t("metric_dominant", lang), row.dominant_pollutant.upper(), icon="biotech")
    with col3:
        render_metric_card(t("metric_worst", lang), f"{int(row.max_aqi)}", icon="error")
    with col4:
        render_metric_card(t("metric_active", lang), f"{int(row.province_count)}", icon="location_on")

# ── Row 2: Interactive Map ────────────────────────────────────────────────────
st.subheader(t("map_title", lang))
map_df = get_map_data()

if not map_df.empty:
    fig_map = px.scatter_mapbox(
        map_df,
        lat="station_latitude",
        lon="station_longitude",
        color="aqi_us",
        size="pm25",
        hover_name="station_name",
        hover_data=["province", "aqi_us"],
        color_continuous_scale="RdYlGn_r",
        range_color=[0, 300],
        zoom=5,
        center={"lat": 16.0, "lon": 108.0},
    )
    
    map_style = "carto-darkmatter" if st.session_state.theme == "dark" else "carto-positron"
    fig_map.update_layout(
        mapbox_style=map_style,
        margin={"r":0,"t":0,"l":0,"b":0},
        height=500,
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ── Row 3: Secondary Analytics ────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader(t("chart_aqi_dist", lang))
    # Logic for distribution
    q_dist = "SELECT aqi_category, count(*) as count FROM air_quality.dm_aqi_current_status GROUP BY aqi_category"
    df_dist = query_df(q_dist)
    if not df_dist.empty:
        fig_pie = px.pie(df_dist, values='count', names='aqi_category', 
                        color_discrete_sequence=px.colors.sequential.RdBu)
        fig_pie.update_layout(get_plotly_layout(height=400))
        st.plotly_chart(fig_pie, use_container_width=True)

with c2:
    st.subheader(t("chart_top_polluted", lang))
    q_top = "SELECT province, max(aqi_us) as max_aqi FROM air_quality.dm_aqi_current_status GROUP BY province ORDER BY max_aqi DESC LIMIT 10"
    df_top = query_df(q_top)
    if not df_top.empty:
        fig_bar = px.bar(df_top, x="max_aqi", y="province", orientation='h', color="max_aqi",
                        color_continuous_scale="Reds")
        fig_bar.update_layout(get_plotly_layout(height=400))
        st.plotly_chart(fig_bar, use_container_width=True)
