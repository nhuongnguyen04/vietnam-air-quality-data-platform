import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.data_service import get_source_table, build_where_clause, get_pollutant_col, get_pollutant_cols
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart, get_aqi_category, EPA_COLORS
from lib.i18n import t
from lib.filters import render_sidebar_filters

# ── Translation & Standard Helpers ─────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")
theme = st.session_state.get("theme", "light")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_grain = filters["time_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters["pollutant"]
standard = filters["standard"]

# ── UI Layout Definitions ─────────────────────────────────────────────────────
st.title(t("overview_title", lang))

# Pre-define KPI row early so content fills from top
metric_row = st.columns(4)

# Determine Source View (Analytics Layer)
table_name = get_source_table(spatial_grain, time_grain)
# Use unified helper for column mapping
display_col, max_col = get_pollutant_cols(pollutant, standard)

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_national_summary(table, col, scope, dates):
    """Fetch aggregated summaries synchronized with current filters."""
    where_clause = build_where_clause(None, scope, dates)

    # Check for max column availability (Analytics tables have pre-aggregated max only for AQI)
    # The helper get_pollutant_cols already handled the max_col mapping safely.
    # However, if we are in hourly view, we just use the original column as max.
    m_col = col if table.endswith("_hourly") else max_col
    
    q = f"""
    SELECT
        avg({col}) as avg_val,
        max(if({m_col} is not null, {m_col}, {col})) as max_val,
        count(distinct if({col} is not null, province, null)) as province_count,
        topK(1)(main_pollutant)[1] as dominant_pollutant
    FROM air_quality.{table}
    WHERE {where_clause}
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_chart_data(table, col, grain, scope, dates):
    """Fetch data for maps and charts with proper hierarchical aggregation."""
    where_clause = build_where_clause(grain, scope, dates)
    
    # Switch grain based on display level: "Tỉnh" or "Phường" triggers ward-level drill down
    if grain in ["Tỉnh", "Phường"]:
        q = f"""
        SELECT
            ward_code,
            ward_name,
            province,
            latitude,
            longitude,
            avg({col}) as display_val,
            topK(1)(main_pollutant)[1] as main_pollutant
        FROM air_quality.{table}
        WHERE {where_clause}
        GROUP BY ward_code, ward_name, province, latitude, longitude
        """
    else:
        # National/Regional view: aggregate to province centroids
        q = f"""
        SELECT
            province,
            avg(latitude) as latitude,
            avg(longitude) as longitude,
            avg({col}) as display_val,
            topK(1)(main_pollutant)[1] as main_pollutant
        FROM air_quality.{table}
        WHERE {where_clause}
        GROUP BY province
        """
    return query_df(q)

@st.cache_data(ttl=300)
def get_aqi_distribution(table, col, scope, dates):
    """Calculate AQI category distribution directly in SQL."""
    where_clause = build_where_clause(None, scope, dates)
    
    q = f"""
    SELECT
        CASE
            WHEN {col} <= 50 THEN 'aqi_good'
            WHEN {col} <= 100 THEN 'aqi_moderate'
            WHEN {col} <= 150 THEN 'aqi_unhealthy_sg'
            WHEN {col} <= 200 THEN 'aqi_unhealthy'
            WHEN {col} <= 300 THEN 'aqi_very_unhealthy'
            ELSE 'aqi_hazardous'
        END as aqi_category_key,
        count(*) as count
    FROM air_quality.{table}
    WHERE {where_clause} AND {col} IS NOT NULL
    GROUP BY aqi_category_key
    """
    df = query_df(q)
    if not df.empty:
        df['aqi_category'] = df['aqi_category_key'].apply(lambda x: t(x, lang))
    return df

# ── Fill KPI Cards (Synchronized with Filters) ────────────────────────────────
summary = get_national_summary(table_name, display_col, scope_val, date_range)
if not summary.empty:
    row = summary.iloc[0]
    
    # Label formatting based on pollutant
    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
    
    with metric_row[0]:
        render_metric_card(f"{t('metric_national_avg', lang)} ({val_label})", f"{int(row.avg_val or 0)}", icon="insights")
    with metric_row[1]:
        render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
    with metric_row[2]:
        render_metric_card(f"{t('metric_worst', lang)} ({val_label})", f"{int(row.max_val or 0)}", icon="error")
    with metric_row[3]:
        render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")

# ── Row 2: Map ────────────────────────────────────────────────────────────────
st.subheader(f"{t('map_title', lang)} - {spatial_grain} ({val_label})")
map_df = get_chart_data(table_name, display_col, spatial_grain, scope_val, date_range)

# Dynamic labeling based on grain
label_col = "ward_name" if spatial_grain in ["Tỉnh", "Phường"] else "province"

# Dynamic range and scale for map
if pollutant == "aqi":
    color_scale = get_epa_continuous_scale()
    range_val = [0, 300]
else:
    # Concentration scale (could be improved with custom scales per pollutant)
    color_scale = "Viridis" if theme == "light" else "Plasma"
    range_val = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]

if not map_df.empty:
    map_lat, map_lon = map_df.latitude.mean(), map_df.longitude.mean()
    zoom_level = 10 if spatial_grain in ["Tỉnh", "Phường"] else 5

    fig_map = px.scatter_mapbox(
        map_df, lat="latitude", lon="longitude", color="display_val",
        hover_name=label_col, color_continuous_scale=color_scale,
        range_color=range_val, zoom=zoom_level, center={"lat": map_lat, "lon": map_lon},
        size="display_val", size_max=30,
        labels={"display_val": val_label, "province": t("province", lang), "ward_name": t("location", lang)}
    )
    map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
    fig_map.update_layout(mapbox_style=map_style, height=600, margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu cho vùng này."), use_container_width=True)

# ── Row 3: Charts ─────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    if pollutant == "aqi":
        st.subheader(t("chart_aqi_dist", lang))
        df_dist = get_aqi_distribution(table_name, display_col, scope_val, date_range)
        if not df_dist.empty:
            # Create color map with translated keys if necessary, 
            # but EPA_COLORS uses English keys. We should map translated labels to colors.
            color_map = {t(k, lang): v for k, v in EPA_COLORS.items()}
            fig_pie = px.pie(df_dist, values='count', names='aqi_category', 
                            color='aqi_category', color_discrete_map=color_map)
            fig_pie.update_layout(get_plotly_layout(height=400))
            st.plotly_chart(fig_pie, use_container_width=True)
    else:
        dist_title = f"{t('chart_label_dist', lang)} {val_label}" if lang=="en" else f"Phân bố {val_label}"
        st.subheader(dist_title)
        if not map_df.empty:
            fig_hist = px.histogram(map_df, x="display_val", marginal="box", 
                                   labels={"display_val": val_label, "count": t("chart_label_count", lang)})
            fig_hist.update_layout(get_plotly_layout(height=400))
            st.plotly_chart(fig_hist, use_container_width=True)

with c2:
    st.subheader(f"{t('chart_top_polluted', lang)} ({val_label})")
    if not map_df.empty:
        df_top = map_df.sort_values('display_val', ascending=False).head(10)
        df_top = df_top.sort_values('display_val', ascending=True)
        fig_bar = px.bar(df_top, y=label_col, x='display_val', orientation='h',
                        color='display_val', color_continuous_scale=color_scale,
                        range_color=range_val, 
                        labels={"display_val": val_label, "province": t("province", lang), "ward_name": t("location", lang)})
        fig_bar.update_layout(get_plotly_layout(height=400))
        st.plotly_chart(fig_bar, use_container_width=True)
