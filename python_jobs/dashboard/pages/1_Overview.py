import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, render_city_metric, get_plotly_layout
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart, EPA_BREAKPOINTS, get_aqi_category, EPA_COLORS
from lib.i18n import t

# ── Translation & Standard Helpers ─────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")
theme = st.session_state.get("theme", "light")
standard = st.session_state.get("standard", "TCVN")

def get_aqi_column():
    return "current_aqi_vn" if standard == "TCVN" else "current_aqi_us"

def get_map_aqi_column():
    return "aqi_vn" if standard == "TCVN" else "aqi_us"

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_national_summary(col):
    """Fetch aggregated national AQI metrics."""
    q = f"""
    SELECT
        avg({col}) as national_avg,
        max({col}) as max_aqi,
        count(DISTINCT province) as province_count,
        topK(1)(main_pollutant)[1] as dominant_pollutant
    FROM air_quality.dm_aqi_current_status FINAL
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_map_data(col):
    """Fetch geo-coded AQI data (using current status as base)."""
    q = f"""
    SELECT
        province,
        district,
        {col} as current_aqi,
        pm25,
        main_pollutant
    FROM air_quality.dm_aqi_current_status FINAL
    """
    return query_df(q)

# ── UI Header ─────────────────────────────────────────────────────────────────
st.title(t("overview_title", lang))
st.markdown(f"{t('current_outlook', lang)}: **{st.session_state.standard} {t('standards', lang)}**")

# ── Row 1: KPI Cards ──────────────────────────────────────────────────────────
aqi_col = get_aqi_column()
summary = get_national_summary(aqi_col)
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

# ── Row 1.5: Major Cities Highlights (Pinned) ────────────────────────────────
st.markdown("### " + ("Hiện trạng tại các đô thị lớn" if lang == "vi" else "Major Cities Outlook"))
mc_col1, mc_col2, mc_col3, mc_col4, mc_col5 = st.columns(5)

@st.cache_data(ttl=300)
def get_major_cities_status(col):
    q = f"""
    SELECT 
        province, 
        avg({col}) as avg_aqi,
        max({col}) as max_aqi
    FROM air_quality.dm_aqi_current_status FINAL
    WHERE province IN ('TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ')
    GROUP BY province
    """
    return query_df(q)

mc_df = get_major_cities_status(aqi_col)

# Mapping từ province_key trong DB → display name
CITY_DB_NAMES = {
    'Hà Nội':       'TP.Hà Nội',
    'Hải Phòng':    'TP.Hải Phòng',
    'Đà Nẵng':      'TP.Đà Nẵng',
    'TP. Hồ Chí Minh': 'TP.Hồ Chí Minh',
    'Cần Thơ':      'TP.Cần Thơ',
}

label_avg = "Trung bình" if lang == "vi" else "Avg"
label_hotspot = "Điểm nóng" if lang == "vi" else "Hotspot"

if not mc_df.empty:
    cities = ['Hà Nội', 'Hải Phòng', 'Đà Nẵng', 'TP. Hồ Chí Minh', 'Cần Thơ']
    cols = [mc_col1, mc_col2, mc_col3, mc_col4, mc_col5]
    for city, col_widget in zip(cities, cols):
        # Lookup bằng tên DB (có prefix TP.) nhưng hiển thị display name
        db_key = CITY_DB_NAMES.get(city, city)
        city_data = mc_df[mc_df['province'] == db_key]
        if not city_data.empty:
            avg_val = int(city_data.iloc[0].avg_aqi)
            max_val = int(city_data.iloc[0].max_aqi)
            with col_widget:
                render_city_metric(city, avg_val, max_val, label_avg, label_hotspot)
        else:
            with col_widget:
                render_city_metric(city, "N/A", "-", label_avg, label_hotspot)

# ── Row 2: Real-time Map ─────────────────────────────────────────────────────
st.subheader(t("map_title", lang))

# Re-fetching map data from verified unified table for coordinates
@st.cache_data(ttl=300)
def get_geo_map_data(col):
    q = f"""
    SELECT
        province,
        station_name,
        station_latitude as lat,
        station_longitude as lon,
        {col} as current_aqi,
        pm25
    FROM air_quality.dm_aqi_weather_traffic_unified
    WHERE datetime_hour = (SELECT max(datetime_hour) FROM air_quality.dm_aqi_weather_traffic_unified)
    """
    return query_df(q)

map_aqi_col = get_map_aqi_column()
map_df = get_geo_map_data(map_aqi_col)

if not map_df.empty:
    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="current_aqi",
        size="pm25",
        hover_name="station_name",
        hover_data=["province", "current_aqi"],
        color_continuous_scale=get_epa_continuous_scale(),
        range_color=[0, 300],
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
    st.plotly_chart(render_empty_chart("Không có dữ liệu trạm thời gian thực cho bản đồ."), use_container_width=True)

st.divider()

# ── Row 3: Analytics ──────────────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader(t("chart_aqi_dist", lang))
    q_dist = f"""
    SELECT 
        CASE 
            WHEN {aqi_col} <= 50 THEN 'Good'
            WHEN {aqi_col} <= 100 THEN 'Moderate'
            WHEN {aqi_col} <= 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN {aqi_col} <= 200 THEN 'Unhealthy'
            WHEN {aqi_col} <= 300 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END as aqi_category,
        count(*) as count 
    FROM air_quality.dm_aqi_current_status FINAL
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
        fig_pie.update_layout(get_plotly_layout(height=480))
        st.plotly_chart(fig_pie, use_container_width=True)

with c2:
    st.subheader(t("chart_top_polluted", lang))
    
    @st.cache_data(ttl=300)
    def get_top_polluted(col):
        q = f"""
        SELECT 
            province, 
            max({col}) as province_aqi 
        FROM air_quality.dm_aqi_current_status FINAL
        WHERE province != '' AND province IS NOT NULL
        GROUP BY province 
        ORDER BY province_aqi DESC 
        LIMIT 10
        """
        return query_df(q)

    df_top = get_top_polluted(aqi_col)
    if not df_top.empty:
        # Sort values for horizontal bar: top on top
        df_top = df_top.sort_values('province_aqi', ascending=True)
        # Add category for discrete coloring
        df_top['aqi_category'] = df_top['province_aqi'].apply(get_aqi_category)
        
        y_label = f"Hotspot ({standard}) AQI"
        fig_bar = px.bar(df_top, y='province', x='province_aqi', 
                        color='aqi_category',
                        orientation='h',
                        color_discrete_map=EPA_COLORS,
                        labels={'province_aqi': y_label, 'province': '', 'aqi_category': t('category', lang)})
        
        # Add background color bands for health categories
        for lo, hi, label, color in EPA_BREAKPOINTS:
            if lo > df_top['province_aqi'].max() + 50: break
            fig_bar.add_vrect(
                x0=lo, x1=hi, 
                fillcolor=color, opacity=0.05, # More subtle background
                layer="below", line_width=0
            )

        fig_bar.update_layout(get_plotly_layout(height=480))
        fig_bar.update_layout(coloraxis_showscale=False, showlegend=True) 
        st.plotly_chart(fig_bar, use_container_width=True)
