"""
Trang Tổng quan (Overview) cung cấp cái nhìn toàn diện về chất lượng không khí tại Việt Nam.
Bao gồm các chỉ số KPI chính (AQI trung bình, chất ô nhiễm chính, điểm nóng ô nhiễm)
và bản đồ phân bố không gian theo Tỉnh/Thành phố hoặc Phường/Xã.
Tất cả giá trị (KPI, bản đồ, biểu đồ) đều tuân theo bộ lọc sidebar.
"""
import plotly.express as px
import streamlit as st
from lib.aqi_utils import (
    get_aqi_category,
    get_aqi_color_range,
    get_aqi_color_scale,
    get_aqi_colorbar_config,
    get_aqi_discrete_colors,
    render_empty_chart,
)
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause, get_pollutant_cols, get_source_table
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.style import get_plotly_layout, render_metric_card

# ── Translation & Standard Helpers ─────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")
theme = st.session_state.get("theme", "light")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_grain    = filters["time_grain"]
time_unit     = filters["time_unit"]       # "hour" or "day"
scope_val     = filters["scope_val"]
date_range    = filters["date_range"]
pollutant     = filters["pollutant"]
standard      = filters["standard"]

# ── UI Layout Definitions ─────────────────────────────────────────────────────
st.title(t("overview_title", lang))

# ── Real-Time Alert Banner (always uses dm_aqi_current_status — same as Grafana) ──
@st.cache_data(ttl=120)
def get_current_aqi_status():
    """Fetch current AQI per province — same table as Grafana alert rules."""
    q = """
    SELECT
        province,
        MAX(current_aqi_vn) AS current_aqi,
        argMax(main_pollutant, latest_hour) AS main_pollutant,
        MAX(latest_hour) AS as_of_hour
    FROM air_quality.dm_aqi_current_status
    WHERE latest_hour >= NOW() - INTERVAL 3 HOUR
      AND province != ''
      AND current_aqi_vn IS NOT NULL
    GROUP BY province
    ORDER BY current_aqi DESC
    """
    return query_df(q)

current_df = get_current_aqi_status()

if not current_df.empty:
    alert_provinces = current_df[current_df["current_aqi"] > 150]
    if not alert_provinces.empty:
        alert_lines = " | ".join(
            f"📍 **{row.province}**: {int(row.current_aqi)}"
            for _, row in alert_provinces.head(8).iterrows()
        )
        st.warning(
            f"⚠️ **Cảnh báo chất lượng không khí (AQI > 150) — dữ liệu giờ gần nhất:**\n\n{alert_lines}\n\n"
            "_Đây là dữ liệu thực-time (cùng nguồn với cảnh báo Telegram). "
            "Các chỉ số bên dưới hiển thị theo khoảng thời gian bạn chọn._"
        )
    else:
        st.success("✅ **Hiện tại không có tỉnh nào vượt ngưỡng AQI 150** (cập nhật theo giờ)")

    with st.expander("📊 Bảng AQI hiện tại theo tỉnh (cùng dữ liệu với cảnh báo Telegram)", expanded=False):
        display_current = current_df.copy()
        display_current["AQI hiện tại"] = display_current["current_aqi"].apply(lambda x: int(x))
        display_current["Mức độ"] = display_current["current_aqi"].apply(lambda x: {
            "Good": "🟢 Tốt",
            "Moderate": "🟡 Trung bình",
            "Unhealthy for Sensitive Groups": "🟠 Kém (nhạy cảm)",
            "Unhealthy": "🔴 Xấu",
            "Very Unhealthy": "🟣 Rất xấu",
            "Hazardous": "⚫ Nguy hại",
        }.get(get_aqi_category(x), "❓"))
        display_current["Cập nhật lúc"] = display_current["as_of_hour"].apply(
            lambda x: x.strftime("%H:%M %d/%m") if hasattr(x, "strftime") else str(x)
        )
        st.dataframe(
            display_current[["province", "AQI hiện tại", "Mức độ", "main_pollutant", "Cập nhật lúc"]]
            .rename(columns={"province": "Tỉnh/thành", "main_pollutant": "Ô nhiễm chính"}),
            hide_index=True,
            use_container_width=True,
        )
        st.caption("⏱ Cảnh báo Telegram kích hoạt khi AQI > 150 trong 10 phút liên tiếp.")

st.markdown("---")

# ── Pre-define KPI row ─────────────────────────────────────────────────────────
metric_row = st.columns(4)

# Determine Source Table (based on spatial + time grain)
table_name = get_source_table(spatial_grain, time_grain)
# Column mapping for selected pollutant + standard
display_col, max_col = get_pollutant_cols(pollutant, standard)
# Label for axes / cards
val_label = "AQI" if pollutant == "aqi" else pollutant.upper()

# ── Filter description label ───────────────────────────────────────────────────
if time_unit == "hour" and date_range and len(date_range) == 2:
    dr_start, dr_end = date_range
    filter_label = (
        f"{dr_start.strftime('%H:%M %d/%m/%Y')} → {dr_end.strftime('%H:%M %d/%m/%Y')}"
        if hasattr(dr_start, "strftime") else ""
    )
else:
    filter_label = (
        f"{date_range[0]} → {date_range[1]}"
        if date_range and len(date_range) == 2 else ""
    )

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_national_summary(table, col, m_col, scope, dates, tunit):
    """KPI summary — follows the active filter (time_unit aware)."""
    where_clause = build_where_clause(None, scope, dates, time_unit=tunit)
    q = f"""
    SELECT
        avg({col}) as avg_val,
        max(if({m_col} is not null, {m_col}, {col})) as max_val,
        count(distinct if({col} is not null, province, null)) as province_count,
        topK(1)(main_pollutant)[1] as dominant_pollutant
    FROM air_quality.{table}
    WHERE {where_clause}
      AND {col} IS NOT NULL
      AND province IS NOT NULL
      AND province != ''
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_chart_data(table, col, grain, scope, dates, tunit):
    """Map & bar chart data — ward-level for Tỉnh/Phường, province-level otherwise."""
    where_clause = build_where_clause(grain, scope, dates, time_unit=tunit)

    if grain in ["Tỉnh", "Phường"]:
        # Ward-level: show individual ward dots on the map
        # Note: aliases use lat/lon (not latitude/longitude) to avoid ClickHouse
        # ILLEGAL_AGGREGATION error when WHERE clause references same column name.
        q = f"""
        SELECT
            ward_code,
            ward_name,
            province,
            any(latitude)  AS lat,
            any(longitude) AS lon,
            avg({col}) as display_val,
            topK(1)(main_pollutant)[1] as main_pollutant
        FROM air_quality.{table}
        WHERE {where_clause}
          AND {col} IS NOT NULL
          AND province IS NOT NULL
          AND province != ''
          AND ward_name IS NOT NULL
          AND ward_name != ''
          AND latitude  != 0
          AND longitude != 0
        GROUP BY ward_code, ward_name, province
        """
    else:
        # National / Region: aggregate to province centroids
        q = f"""
        SELECT
            province,
            avg(latitude)  AS lat,
            avg(longitude) AS lon,
            avg({col}) as display_val,
            topK(1)(main_pollutant)[1] as main_pollutant
        FROM air_quality.{table}
        WHERE {where_clause}
          AND {col} IS NOT NULL
          AND province IS NOT NULL
          AND province != ''
          AND latitude  != 0
          AND longitude != 0
        GROUP BY province
        """
    df = query_df(q)
    # Restore expected column names for downstream map rendering
    if not df.empty and "lat" in df.columns:
        df = df.rename(columns={"lat": "latitude", "lon": "longitude"})
    return df



@st.cache_data(ttl=300)
def get_aqi_distribution(table, col, scope, dates, tunit):
    """AQI category distribution — follows the active filter."""
    where_clause = build_where_clause(None, scope, dates, time_unit=tunit)
    q = f"""
    SELECT
        CASE
            WHEN {col} <= 50  THEN 'aqi_good'
            WHEN {col} <= 100 THEN 'aqi_moderate'
            WHEN {col} <= 150 THEN 'aqi_unhealthy_sg'
            WHEN {col} <= 200 THEN 'aqi_unhealthy'
            WHEN {col} <= 300 THEN 'aqi_very_unhealthy'
            ELSE 'aqi_hazardous'
        END as aqi_category_key,
        count(*) as count
    FROM air_quality.{table}
    WHERE {where_clause}
      AND {col} IS NOT NULL
      AND province IS NOT NULL
      AND province != ''
    GROUP BY aqi_category_key
    """
    df = query_df(q)
    if not df.empty:
        df["aqi_category"] = df["aqi_category_key"].apply(lambda x: t(x, lang))
    return df


# ── hourly table has no max_aqi column — fall back to avg for max card ────────
m_col = display_col if table_name.endswith("_hourly") else max_col

# ── KPI Cards ─────────────────────────────────────────────────────────────────
summary = get_national_summary(table_name, display_col, m_col, scope_val, date_range, time_unit)
if not summary.empty:
    row = summary.iloc[0]
    with metric_row[0]:
        render_metric_card(f"{t('metric_national_avg', lang)} ({val_label})", f"{int(row.avg_val or 0)}", icon="insights")
    with metric_row[1]:
        render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
    with metric_row[2]:
        render_metric_card(f"{t('metric_worst', lang)} ({val_label})", f"{int(row.max_val or 0)}", icon="error")
    with metric_row[3]:
        render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")

# ── Contextual Annotations ─────────────────────────────────────────────────
# Seasonal context: May–September monsoon reduces PM2.5
if date_range and len(date_range) == 2:
    _months = set()
    if hasattr(date_range[0], 'month'):
        _months.add(date_range[0].month)
    if hasattr(date_range[1], 'month'):
        _months.add(date_range[1].month)
    if _months & {5, 6, 7, 8, 9}:
        _seasonal = (
            "🌧 **Bối cảnh mùa vụ:** Tháng 5–9 là mùa mưa — PM2.5 giảm nhờ "
            "rửa bụi (wash-out) và gió mùa Tây Nam tăng phát tán ô nhiễm."
        ) if lang == "vi" else (
            "🌧 **Seasonal context:** May–Sep is monsoon season — PM2.5 drops due to "
            "rain wash-out and southwest monsoon enhancing pollutant dispersion."
        )
        st.caption(_seasonal)

# ── Map ───────────────────────────────────────────────────────────────────────
map_title = f"{t('map_title', lang)} — {spatial_grain} ({val_label})"
if filter_label:
    map_title += f"  ·  {filter_label}"
st.subheader(map_title)

map_df = get_chart_data(table_name, display_col, spatial_grain, scope_val, date_range, time_unit)

# Label column for hover
label_col = "ward_name" if spatial_grain in ["Tỉnh", "Phường"] else "province"

# Color scale
if pollutant == "aqi":
    color_scale = get_aqi_color_scale(standard)
    range_val   = get_aqi_color_range(standard)
else:
    color_scale = "Viridis" if theme == "light" else "Plasma"
    range_val   = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]

if not map_df.empty:
    map_lat   = map_df.latitude.mean()
    map_lon   = map_df.longitude.mean()
    zoom_level = 8 if spatial_grain in ["Tỉnh", "Phường"] else 5

    fig_map = px.scatter_map(
        map_df,
        lat="latitude", lon="longitude",
        color="display_val",
        hover_name=label_col,
        hover_data={"province": True, "display_val": ":.1f", "latitude": False, "longitude": False}
        if spatial_grain in ["Tỉnh", "Phường"] else
        {"display_val": ":.1f", "latitude": False, "longitude": False},
        color_continuous_scale=color_scale,
        range_color=range_val,
        zoom=zoom_level,
        center={"lat": map_lat, "lon": map_lon},
        size="display_val",
        size_max=25,
        labels={
            "display_val": val_label,
            "province":    t("province", lang),
            "ward_name":   t("location", lang),
        },
    )
    map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
    fig_map.update_layout(
        mapbox_style=map_style,
        height=600,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    if pollutant == "aqi":
        fig_map.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
    st.plotly_chart(fig_map, width="stretch")
else:
    st.plotly_chart(
        render_empty_chart(t("no_data", lang) if lang == "en" else "Không có dữ liệu cho vùng này."),
        width="stretch",
    )

# ── Row 3: Charts ─────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    if pollutant == "aqi":
        st.subheader(t("chart_aqi_dist", lang))
        df_dist = get_aqi_distribution(table_name, display_col, scope_val, date_range, time_unit)
        if not df_dist.empty:
            aqi_colors = get_aqi_discrete_colors(standard)
            color_map = {
                t("aqi_good", lang):           aqi_colors["Good"],
                t("aqi_moderate", lang):        aqi_colors["Moderate"],
                t("aqi_unhealthy_sg", lang):    aqi_colors["Unhealthy for Sensitive Groups"],
                t("aqi_unhealthy", lang):       aqi_colors["Unhealthy"],
                t("aqi_very_unhealthy", lang):  aqi_colors["Very Unhealthy"],
                t("aqi_hazardous", lang):       aqi_colors["Hazardous"],
            }
            fig_pie = px.pie(
                df_dist, values="count", names="aqi_category",
                color="aqi_category", color_discrete_map=color_map,
            )
            fig_pie.update_layout(get_plotly_layout(height=400))
            st.plotly_chart(fig_pie, width="stretch")
    else:
        dist_title = (
            f"{t('chart_label_dist', lang)} {val_label}" if lang == "en"
            else f"Phân bố {val_label}"
        )
        st.subheader(dist_title)
        if not map_df.empty:
            fig_hist = px.histogram(
                map_df, x="display_val", marginal="box",
                labels={"display_val": val_label, "count": t("chart_label_count", lang)},
            )
            fig_hist.update_layout(get_plotly_layout(height=400))
            st.plotly_chart(fig_hist, width="stretch")

with c2:
    st.subheader(f"{t('chart_top_polluted', lang)} ({val_label})")
    if not map_df.empty:
        df_top = map_df.sort_values("display_val", ascending=False).head(10)
        df_top = df_top.sort_values("display_val", ascending=True)
        fig_bar = px.bar(
            df_top,
            y=label_col,
            x="display_val",
            orientation="h",
            color="display_val",
            color_continuous_scale=color_scale,
            range_color=range_val,
            labels={
                "display_val": val_label,
                "province":    t("province", lang),
                "ward_name":   t("location", lang),
            },
        )
        fig_bar.update_layout(get_plotly_layout(height=400))
        if pollutant == "aqi":
            fig_bar.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
        st.plotly_chart(fig_bar, width="stretch")
