import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.data_service import get_source_table, get_hierarchy_metadata, build_where_clause
from lib.style import render_metric_card, render_city_metric, get_plotly_layout
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart, EPA_BREAKPOINTS, get_aqi_category, EPA_COLORS
from lib.i18n import t

# ── Translation & Standard Helpers ─────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")
theme = st.session_state.get("theme", "light")
standard = st.session_state.get("standard", "TCVN")

def get_aqi_col_name(standard):
    """Standardized naming regardless of table grain."""
    return "avg_aqi_vn" if standard == "TCVN" else "avg_aqi_us"

# ── UI Layout Definitions ─────────────────────────────────────────────────────
st.title(t("overview_title", lang))

# Pre-define KPI row early so content fills from top
metric_row = st.columns(4)

# ── Filter Bar ────────────────────────────────────────────────────────────────
hierarchy_df = get_hierarchy_metadata()

c_f1, c_f2, c_f3, c_f4 = st.columns(4)

with c_f1:
    time_grain = st.selectbox(
        "Độ phân giải thời gian" if lang == "vi" else "Time Grain",
        ["Giờ", "Ngày", "Tháng"],
        index=1
    )

with c_f2:
    spatial_grain = st.selectbox(
        "Cấp độ hiển thị" if lang == "vi" else "Spatial Grain",
        ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh", "Phường"],
        index=0
    )

# Scope selector — always rendered, disabled when "Toàn quốc"
scope_disabled = spatial_grain == "Toàn quốc"
scope_disabled_label = "📍 Chọn cấp độ hiển thị trước" if scope_disabled else None

scope_choices = {
    "Vùng":    ("Chọn miền", sorted(hierarchy_df['region_3'].unique())),
    "Khu vực": ("Chọn khu vực", sorted(hierarchy_df['region_8'].unique())),
    "Tỉnh":    ("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique())),
    "Phường":  ("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique())),
}.get(spatial_grain, ("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique())))

scope_label, scope_options = scope_choices

with c_f3:
    scope_val = st.selectbox(
        scope_label,
        scope_options,
        disabled=scope_disabled,
        label_visibility="visible"
    )
    if scope_disabled:
        scope_val = None  # "Toàn quốc": không filter theo vùng

with c_f4:
    date_range = st.date_input(
        "Khoảng thời gian" if lang == "vi" else "Date Range",
        value=[pd.to_datetime('today') - pd.Timedelta(days=7), pd.to_datetime('today')],
        max_value=pd.to_datetime('today')
    )

# Determine Source View (Analytics Layer)
table_name = get_source_table(spatial_grain, time_grain)
aqi_col = get_aqi_col_name(standard)

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_national_summary(col, grain, dates):
    """Fetch aggregated summaries from the optimized National Summary mart."""
    # Mapping grain to time_grain in table
    tg = {"Giờ": "hourly", "Ngày": "daily", "Tháng": "monthly"}.get(grain, "daily")
    
    q = f"""
    SELECT
        avg_aqi_vn as avg_val_vn,
        avg_aqi_us as avg_val_us,
        max_aqi_vn as max_val_vn,
        max_aqi_us as max_val_us,
        active_provinces as province_count,
        dominant_pollutant
    FROM air_quality.dm_air_quality_national_summary
    WHERE time_grain = '{tg}'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_chart_data(table, col, grain, scope, dates):
    """Fetch data for maps and charts with proper hierarchical aggregation."""
    where_clause = build_where_clause(grain, scope, dates)
    
    # If the user has selected a specific province/scope OR the grain is set to Phường, 
    # we show the individual ward-level points.
    if grain == "Phường" or scope:
        q = f"""
        SELECT
            ward_code,
            ward_name,
            province,
            latitude,
            longitude,
            avg({col}) as display_aqi,
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
            avg({col}) as display_aqi,
            topK(1)(main_pollutant)[1] as main_pollutant
        FROM air_quality.{table}
        WHERE {where_clause}
        GROUP BY province
        """
    return query_df(q)

# ── Fill KPI Cards (Using Optimized National Mart) ──────────────────────────────
summary = get_national_summary(aqi_col, time_grain, date_range)
if not summary.empty:
    row = summary.iloc[0]
    avg_val = row.avg_val_vn if standard == "TCVN" else row.avg_val_us
    max_val = row.max_val_vn if standard == "TCVN" else row.max_val_us
    
    with metric_row[0]:
        render_metric_card(t("metric_national_avg", lang), f"{int(avg_val or 0)}", icon="insights")
    with metric_row[1]:
        render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
    with metric_row[2]:
        render_metric_card(t("metric_worst", lang), f"{int(max_val or 0)}", icon="error")
    with metric_row[3]:
        render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")

# ── Row 2: Map ────────────────────────────────────────────────────────────────
st.subheader(f"{t('map_title', lang)} - {spatial_grain}")
map_df = get_chart_data(table_name, aqi_col, spatial_grain, scope_val, date_range)

if not map_df.empty:
    map_lat, map_lon = map_df.latitude.mean(), map_df.longitude.mean()
    zoom_level = 10 if spatial_grain in ["Tỉnh", "Phường"] else 5

    fig_map = px.scatter_mapbox(
        map_df, lat="latitude", lon="longitude", color="display_aqi",
        hover_name="province", color_continuous_scale=get_epa_continuous_scale(),
        range_color=[0, 300], zoom=zoom_level, center={"lat": map_lat, "lon": map_lon},
        size="display_aqi", size_max=20,
    )
    map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
    fig_map.update_layout(mapbox_style=map_style, height=600, margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu cho vùng này."), use_container_width=True)

# ── Row 3: Charts ─────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    st.subheader(t("chart_aqi_dist", lang))
    if not map_df.empty:
        map_df['aqi_category'] = map_df['display_aqi'].apply(get_aqi_category)
        df_dist = map_df.groupby('aqi_category').size().reset_index(name='count')
        fig_pie = px.pie(df_dist, values='count', names='aqi_category', 
                        color='aqi_category', color_discrete_map=EPA_COLORS)
        fig_pie.update_layout(get_plotly_layout(height=400))
        st.plotly_chart(fig_pie, use_container_width=True)

with c2:
    st.subheader(t("chart_top_polluted", lang))
    if not map_df.empty:
        df_top = map_df.sort_values('display_aqi', ascending=False).head(10)
        df_top = df_top.sort_values('display_aqi', ascending=True)
        fig_bar = px.bar(df_top, y='province', x='display_aqi', orientation='h',
                        color='display_aqi', color_continuous_scale=get_epa_continuous_scale())
        fig_bar.update_layout(get_plotly_layout(height=400))
        st.plotly_chart(fig_bar, use_container_width=True)
