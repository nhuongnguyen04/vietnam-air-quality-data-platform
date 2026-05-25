"""
Trang Tổng quan (Overview) cung cấp cái nhìn toàn diện về chất lượng không khí tại Việt Nam.
Bao gồm các chỉ số KPI chính (AQI trung bình, chất ô nhiễm chính, điểm nóng ô nhiễm)
và bản đồ phân bố không gian theo Tỉnh/Thành phố hoặc Phường/Xã.
Tất cả giá trị (KPI, bản đồ, biểu đồ) đều tuân theo bộ lọc sidebar.
"""
import pandas as pd
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
from lib.data_service import (
    build_where_clause,
    get_pollutant_cols,
    get_source_table,
    get_source_mix,
    localize_confidence_level,
    localize_source_mix,
    get_source_coverage,
    get_source_correlation,
)
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
        current_aqi,
        main_pollutant,
        confidence_score,
        if(confidence_score >= 0.8, 'high', if(confidence_score >= 0.5, 'medium', 'low')) AS confidence_level,
        source_mix,
        aqiin_observation_count,
        openweather_observation_count,
        as_of_hour
    FROM (
        SELECT
            province,
            MAX(current_aqi_vn) AS current_aqi,
            argMax(main_pollutant, latest_hour) AS main_pollutant,
            avg(confidence_score) AS confidence_score,
            topK(1)(source_mix)[1] AS source_mix,
            sum(aqiin_observation_count) AS aqiin_observation_count,
            sum(openweather_observation_count) AS openweather_observation_count,
            MAX(latest_hour) AS as_of_hour
        FROM air_quality.dm_aqi_current_status
        WHERE latest_hour >= NOW() - INTERVAL 3 HOUR
          AND province != ''
          AND current_aqi_vn IS NOT NULL
        GROUP BY province
    )
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
        display_current["Nguồn"] = display_current["source_mix"].apply(lambda x: localize_source_mix(x, lang))
        display_current["Độ tin cậy"] = display_current["confidence_level"].apply(
            lambda x: localize_confidence_level(x, lang)
        )
        st.dataframe(
            display_current[[
                "province", "AQI hiện tại", "Mức độ", "main_pollutant",
                "Nguồn", "Độ tin cậy", "Cập nhật lúc"
            ]]
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
def get_national_summary(table, col, m_col, grain, scope, dates, tunit, source_name="blended"):
    """KPI summary — follows the active filter (time_unit aware)."""
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(None, scope, dates, time_unit=tunit, source_mix=source_mix)
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
def get_chart_data(table, col, grain, scope, dates, tunit, source_name="blended"):
    """Map & bar chart data — ward-level for Tỉnh/Phường, province-level otherwise."""
    if source_name == "aqiin":
        # Raw / Staging level query for un-aggregated stations
        is_aqi = "aqi" in col
        if is_aqi:
            pollutant_name = "aqi"
            standard_name = "VN_AQI" if "vn" in col else "US_AQI"
        else:
            pollutant_name = col.split("_")[0]  # e.g., "pm25" from "pm25_avg"
            standard_name = "VN_AQI"

        # Construct date filter
        if dates and len(dates) == 2:
            start_val = dates[0].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[0], "hour") else f"{dates[0]} 00:00:00"
            end_val = dates[1].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[1], "hour") else f"{dates[1]} 23:59:59"
            date_filter = f"timestamp_utc BETWEEN toDateTime('{start_val}') AND toDateTime('{end_val}')"
        elif dates and len(dates) == 1:
            val = dates[0].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[0], "hour") else f"{dates[0]} 00:00:00"
            date_filter = f"timestamp_utc >= toDateTime('{val}')"
        else:
            date_filter = "1=1"

        # Construct spatial filter
        spatial_filters = []
        if grain == "Vùng" and scope:
            spatial_filters.append(f"d.region_3 = '{scope}'")
        elif grain == "Khu vực" and scope:
            spatial_filters.append(f"d.region_8 = '{scope}'")
        elif grain in ["Tỉnh", "Phường"] and scope:
            spatial_filters.append(f"s.province = '{scope}'")
            
        spatial_filter_str = " AND ".join(spatial_filters) if spatial_filters else "1=1"

        if pollutant_name == "aqi":
            if standard_name == "VN_AQI":
                # Compute VN AQI on the fly using standard formulas from calculate_aqi_vn macro
                aqi_vn_expr = """
                CASE
                    WHEN LOWER(parameter) = 'pm25' THEN
                        CASE
                            WHEN value <= 25 THEN ((50.0 - 0.0) / (25.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 50 THEN ((100.0 - 51.0) / (50.0 - 26.0)) * (value - 26.0) + 51.0
                            WHEN value <= 80 THEN ((150.0 - 101.0) / (80.0 - 51.0)) * (value - 51.0) + 101.0
                            WHEN value <= 150 THEN ((200.0 - 151.0) / (150.0 - 81.0)) * (value - 81.0) + 151.0
                            WHEN value <= 250 THEN ((300.0 - 201.0) / (250.0 - 151.0)) * (value - 151.0) + 201.0
                            WHEN value <= 350 THEN ((400.0 - 301.0) / (350.0 - 251.0)) * (value - 251.0) + 301.0
                            WHEN value <= 500 THEN ((500.0 - 401.0) / (500.0 - 351.0)) * (value - 351.0) + 401.0
                            ELSE NULL
                        END
                    WHEN LOWER(parameter) = 'pm10' THEN
                        CASE
                            WHEN value <= 50 THEN ((50.0 - 0.0) / (50.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 150 THEN ((100.0 - 51.0) / (150.0 - 51.0)) * (value - 51.0) + 51.0
                            WHEN value <= 250 THEN ((150.0 - 101.0) / (250.0 - 151.0)) * (value - 151.0) + 101.0
                            WHEN value <= 350 THEN ((200.0 - 151.0) / (350.0 - 251.0)) * (value - 251.0) + 151.0
                            WHEN value <= 420 THEN ((300.0 - 201.0) / (420.0 - 351.0)) * (value - 351.0) + 201.0
                            WHEN value <= 500 THEN ((400.0 - 301.0) / (500.0 - 421.0)) * (value - 421.0) + 301.0
                            WHEN value <= 600 THEN ((500.0 - 401.0) / (600.0 - 501.0)) * (value - 501.0) + 401.0
                            ELSE NULL
                        END
                    WHEN LOWER(parameter) = 'o3' THEN
                        CASE
                            WHEN value <= 160 THEN ((50.0 - 0.0) / (160.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 200 THEN ((100.0 - 51.0) / (200.0 - 161.0)) * (value - 161.0) + 51.0
                            WHEN value <= 240 THEN ((150.0 - 101.0) / (240.0 - 201.0)) * (value - 201.0) + 101.0
                            WHEN value <= 280 THEN ((200.0 - 151.0) / (280.0 - 241.0)) * (value - 241.0) + 151.0
                            WHEN value <= 400 THEN ((300.0 - 201.0) / (400.0 - 281.0)) * (value - 281.0) + 201.0
                            WHEN value <= 500 THEN ((400.0 - 301.0) / (500.0 - 401.0)) * (value - 401.0) + 301.0
                            WHEN value <= 600 THEN ((500.0 - 401.0) / (600.0 - 501.0)) * (value - 501.0) + 401.0
                            ELSE NULL
                        END
                    WHEN LOWER(parameter) = 'so2' THEN
                        CASE
                            WHEN value <= 125 THEN ((50.0 - 0.0) / (125.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 350 THEN ((100.0 - 51.0) / (350.0 - 126.0)) * (value - 126.0) + 51.0
                            WHEN value <= 550 THEN ((150.0 - 101.0) / (550.0 - 351.0)) * (value - 351.0) + 101.0
                            WHEN value <= 800 THEN ((200.0 - 151.0) / (800.0 - 551.0)) * (value - 551.0) + 151.0
                            WHEN value <= 1600 THEN ((300.0 - 201.0) / (1600.0 - 801.0)) * (value - 801.0) + 201.0
                            WHEN value <= 2100 THEN ((400.0 - 301.0) / (2100.0 - 1601.0)) * (value - 1601.0) + 301.0
                            WHEN value <= 2630 THEN ((500.0 - 401.0) / (2630.0 - 2101.0)) * (value - 2101.0) + 401.0
                            ELSE NULL
                        END
                    WHEN LOWER(parameter) = 'no2' THEN
                        CASE
                            WHEN value <= 40 THEN ((50.0 - 0.0) / (40.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 80 THEN ((100.0 - 51.0) / (80.0 - 41.0)) * (value - 41.0) + 51.0
                            WHEN value <= 180 THEN ((150.0 - 101.0) / (180.0 - 81.0)) * (value - 81.0) + 101.0
                            WHEN value <= 280 THEN ((200.0 - 151.0) / (280.0 - 181.0)) * (value - 181.0) + 151.0
                            WHEN value <= 565 THEN ((300.0 - 201.0) / (565.0 - 281.0)) * (value - 281.0) + 201.0
                            WHEN value <= 750 THEN ((400.0 - 301.0) / (750.0 - 566.0)) * (value - 566.0) + 301.0
                            WHEN value <= 940 THEN ((500.0 - 401.0) / (940.0 - 751.0)) * (value - 751.0) + 401.0
                            ELSE NULL
                        END
                    WHEN LOWER(parameter) = 'co' THEN
                        CASE
                            WHEN value <= 10000 THEN ((50.0 - 0.0) / (10000.0 - 0.0)) * (value - 0.0) + 0.0
                            WHEN value <= 30000 THEN ((100.0 - 51.0) / (30000.0 - 10001.0)) * (value - 10001.0) + 51.0
                            WHEN value <= 45000 THEN ((150.0 - 101.0) / (45000.0 - 30001.0)) * (value - 30001.0) + 101.0
                            WHEN value <= 60000 THEN ((200.0 - 151.0) / (60000.0 - 45001.0)) * (value - 45001.0) + 151.0
                            WHEN value <= 90000 THEN ((300.0 - 201.0) / (90000.0 - 60001.0)) * (value - 60001.0) + 201.0
                            WHEN value <= 120000 THEN ((400.0 - 301.0) / (120000.0 - 90001.0)) * (value - 90001.0) + 301.0
                            WHEN value <= 150000 THEN ((500.0 - 401.0) / (150000.0 - 120001.0)) * (value - 120001.0) + 401.0
                            ELSE NULL
                        END
                    ELSE NULL
                END
                """
                overall_aqi_expr = f"max({aqi_vn_expr})"
                main_poll_expr = f"argMax(parameter, {aqi_vn_expr})"
            else:
                # US AQI
                overall_aqi_expr = "any(aqi_reported)"
                main_poll_expr = "argMax(parameter, value)"

            q = f"""
            SELECT
                s.ward_code AS ward_code,
                s.station_name AS ward_name,
                any(coalesce(nullIf(d.ward_name, ''), s.station_name)) AS actual_ward_name,
                s.province AS province,
                any(s.latitude) AS lat,
                any(s.longitude) AS lon,
                avg(t.overall_aqi) AS display_val,
                topK(1)(t.main_pollutant)[1] AS main_pollutant,
                1.0 AS confidence_score,
                'high' AS confidence_level,
                'observed' AS source_mix,
                sum(t.obs_count) AS aqiin_observation_count,
                0 AS openweather_observation_count
            FROM (
                SELECT
                    station_name,
                    timestamp_utc,
                    {overall_aqi_expr} AS overall_aqi,
                    {main_poll_expr} AS main_pollutant,
                    count() AS obs_count
                FROM air_quality.stg_aqiin__measurements
                WHERE parameter IN ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
                  AND {date_filter}
                GROUP BY station_name, timestamp_utc
            ) t
            JOIN air_quality.stg_core__stations s ON t.station_name = s.station_name
            LEFT JOIN air_quality.dim_administrative_units d ON s.ward_code = d.ward_code
            WHERE {spatial_filter_str}
              AND s.latitude != 0
              AND s.longitude != 0
            GROUP BY s.ward_code, s.station_name, s.province
            """
        else:
            q = f"""
            SELECT
                s.ward_code AS ward_code,
                s.station_name AS ward_name,
                any(coalesce(nullIf(d.ward_name, ''), s.station_name)) AS actual_ward_name,
                s.province AS province,
                any(s.latitude) AS lat,
                any(s.longitude) AS lon,
                avg(m.value) AS display_val,
                '{pollutant_name}' AS main_pollutant,
                1.0 AS confidence_score,
                'high' AS confidence_level,
                'observed' AS source_mix,
                count(distinct m.timestamp_utc) AS aqiin_observation_count,
                0 AS openweather_observation_count
            FROM air_quality.stg_aqiin__measurements m
            JOIN air_quality.stg_core__stations s ON m.station_name = s.station_name
            LEFT JOIN air_quality.dim_administrative_units d ON m.ward_code = d.ward_code
            WHERE m.parameter = '{pollutant_name}'
              AND {date_filter}
              AND {spatial_filter_str}
              AND s.latitude != 0
              AND s.longitude != 0
            GROUP BY s.ward_code, s.station_name, s.province
            """
    else:
        source_mix = get_source_mix(source_name)
        where_clause = build_where_clause(grain, scope, dates, time_unit=tunit, source_mix=source_mix)

        if grain in ["Tỉnh", "Phường"]:
            # Ward-level: show individual ward dots on the map
            # Note: aliases use lat/lon (not latitude/longitude) to avoid ClickHouse
            # ILLEGAL_AGGREGATION error when WHERE clause references same column name.
            # confidence_level uses a subquery so avg(confidence_score) is already a
            # plain scalar in the outer SELECT — avoids ClickHouse ILLEGAL_AGGREGATION.
            q = f"""
            SELECT
                ward_code,
                ward_name,
                province,
                lat,
                lon,
                display_val,
                main_pollutant,
                confidence_score,
                if(confidence_score >= 0.8, 'high', if(confidence_score >= 0.5, 'medium', 'low')) as confidence_level,
                source_mix_val as source_mix,
                aqiin_observation_count,
                openweather_observation_count
            FROM (
                SELECT
                    ward_code,
                    ward_name,
                    province,
                    any(latitude)  AS lat,
                    any(longitude) AS lon,
                    avg({col}) as display_val,
                    topK(1)(main_pollutant)[1] as main_pollutant,
                    avg(confidence_score) as confidence_score,
                    topK(1)(source_mix)[1] as source_mix_val,
                    sum(aqiin_observation_count) as aqiin_observation_count,
                    sum(openweather_observation_count) as openweather_observation_count
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
            )
            """
        else:
            # National / Region: aggregate to province centroids
            # confidence_level uses a subquery so avg(confidence_score) is already a
            # plain scalar in the outer SELECT — avoids ClickHouse ILLEGAL_AGGREGATION.
            q = f"""
            SELECT
                province,
                lat,
                lon,
                display_val,
                main_pollutant,
                confidence_score,
                if(confidence_score >= 0.8, 'high', if(confidence_score >= 0.5, 'medium', 'low')) as confidence_level,
                source_mix_val as source_mix,
                aqiin_observation_count,
                openweather_observation_count
            FROM (
                SELECT
                    province,
                    avg(latitude)  AS lat,
                    avg(longitude) AS lon,
                    avg({col}) as display_val,
                    topK(1)(main_pollutant)[1] as main_pollutant,
                    avg(confidence_score) as confidence_score,
                    topK(1)(source_mix)[1] as source_mix_val,
                    sum(aqiin_observation_count) as aqiin_observation_count,
                    sum(openweather_observation_count) as openweather_observation_count
                FROM air_quality.{table}
                WHERE {where_clause}
                  AND {col} IS NOT NULL
                  AND province IS NOT NULL
                  AND province != ''
                  AND latitude  != 0
                  AND longitude != 0
                GROUP BY province
            )
            """
    df = query_df(q)
    # Restore expected column names for downstream map rendering
    if not df.empty and "lat" in df.columns:
        df = df.rename(columns={"lat": "latitude", "lon": "longitude"})
    return df



@st.cache_data(ttl=300)
def get_aqi_distribution(table, col, grain, scope, dates, tunit, source_name="blended"):
    """AQI category distribution — follows the active filter."""
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(None, scope, dates, time_unit=tunit, source_mix=source_mix)
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


# ── Render Source Dashboard Helper ───────────────────────────────────────────
def render_source_dashboard(source_name: str):
    # Determine Source Table (based on spatial + time grain)
    table_name = get_source_table(spatial_grain, time_grain, source_name)
    # Column mapping for selected pollutant + standard
    display_col, max_col = get_pollutant_cols(pollutant, standard)
    # Label for axes / cards
    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()

    # 1. Coverage Warning / Badge (if Ground)
    if source_name == "aqiin":
        prov_val = scope_val if spatial_grain in ["Tỉnh", "Phường"] else None
        cov_df = get_source_coverage(prov_val)
        if not cov_df.empty:
            if prov_val:
                row = cov_df.iloc[0]
                cov_pct = row["aqiin_coverage_pct"]
                total_w = row["total_ward_count"]
                aqi_w = row["aqiin_ward_count"]
                
                if cov_pct < 50:
                    st.warning(
                        f"⚠️ **Chất lượng bao phủ thấp:** Chỉ có **{cov_pct:.1f}%** số phường/xã "
                        f"({int(aqi_w)}/{int(total_w)}) tại **{prov_val}** có trạm quan trắc mặt đất hoạt động. "
                        f"Dữ liệu bản đồ có thể có khoảng trống không gian lớn."
                        if lang == "vi" else
                        f"⚠️ **Low Spatial Coverage:** Only **{cov_pct:.1f}%** of wards "
                        f"({int(aqi_w)}/{int(total_w)}) in **{prov_val}** have active ground monitors. "
                        f"Map visualization may contain significant spatial gaps."
                    )
                else:
                    st.success(
                        f"✅ **Độ bao phủ mặt đất tốt:** **{cov_pct:.1f}%** số phường/xã "
                        f"({int(aqi_w)}/{int(total_w)}) tại **{prov_val}** có trạm quan trắc hoạt động."
                        if lang == "vi" else
                        f"✅ **Good Ground Coverage:** **{cov_pct:.1f}%** of wards "
                        f"({int(aqi_w)}/{int(total_w)}) in **{prov_val}** have active ground monitors."
                    )
            else:
                total_aqiin_wards = cov_df["aqiin_ward_count"].sum()
                total_wards = cov_df["total_ward_count"].sum()
                cov_pct = (total_aqiin_wards * 100.0 / total_wards) if total_wards > 0 else 0
                
                if cov_pct < 30:
                    st.warning(
                        f"⚠️ **Bao phủ trạm mặt đất hạn chế:** Toàn quốc chỉ có **{cov_pct:.1f}%** số phường/xã "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) có trạm quan trắc mặt đất hoạt động. "
                        f"Khuyến nghị tham khảo thêm tab **🛰 Mô hình vệ tinh** để bổ sung vùng thiếu dữ liệu."
                        if lang == "vi" else
                        f"⚠️ **Limited Ground Monitor Coverage:** Only **{cov_pct:.1f}%** of wards "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) nationwide have active ground monitors. "
                        f"We recommend checking the **🛰 Satellite Model** tab for full spatial coverage."
                    )
                else:
                    st.success(
                        f"✅ **Tình trạng bao phủ mặt đất:** **{cov_pct:.1f}%** số phường/xã toàn quốc "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) có trạm quan trắc hoạt động."
                        if lang == "vi" else
                        f"✅ **Ground Monitor Coverage:** **{cov_pct:.1f}%** of wards "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) nationwide have active ground monitors."
                    )
    elif source_name == "openweather":
        st.info(
            "🛰️ **Mô hình Vệ tinh & SILAM:** Cung cấp độ bao phủ địa lý đầy đủ (100% xã/phường) dựa trên "
            "dữ liệu lưới ô ~25km từ Viện Khí tượng Phần Lan (FMI). Lưu ý: do là mô hình kết hợp, "
            "chỉ số AQI/nồng độ chất ô nhiễm thường có xu hướng thấp hơn (underestimate) thực tế đo tại mặt đất từ 1.5 đến 2.5 lần."
            if lang == "vi" else
            "🛰️ **Satellite & SILAM Model:** Provides complete geographic coverage (100% of wards) based on "
            "~25km grid resolution from the Finnish Meteorological Institute (FMI). Note: due to the model blending nature, "
            "AQI/concentrations typically tend to be underestimated by 1.5x to 2.5x compared to ground-truth monitors."
        )

    # ── KPI Cards ─────────────────────────────────────────────────────────────────
    metric_row = st.columns(4)
    m_col = display_col if table_name.endswith("_hourly") else max_col
    summary = get_national_summary(table_name, display_col, m_col, spatial_grain, scope_val, date_range, time_unit, source_name)
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
    else:
        for idx in range(4):
            with metric_row[idx]:
                render_metric_card(t("metric_national_avg", lang) if idx == 0 else "...", "N/A", icon="insights")

    # ── Contextual Annotations ─────────────────────────────────────────────────
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

    map_df = get_chart_data(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name)
    label_col = "ward_name" if (spatial_grain in ["Tỉnh", "Phường"] or source_name == "aqiin") else "province"

    if pollutant == "aqi":
        color_scale = get_aqi_color_scale(standard)
        range_val   = get_aqi_color_range(standard)
    else:
        color_scale = "Viridis" if theme == "light" else "Plasma"
        range_val   = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]

    if not map_df.empty:
        map_lat   = map_df.latitude.mean()
        map_lon   = map_df.longitude.mean()
        zoom_level = 8 if (spatial_grain in ["Tỉnh", "Phường"] or (source_name == "aqiin" and scope_val)) else 5

        tooltip_data = {
            "province": True,
            "display_val": ":.1f",
            "latitude": False,
            "longitude": False,
        }
        if "confidence_score" in map_df.columns:
            tooltip_data["confidence_score"] = ":.2f"
        if "source_mix" in map_df.columns:
            tooltip_data["source_mix"] = True

        fig_map = px.scatter_map(
            map_df,
            lat="latitude", lon="longitude",
            color="display_val",
            hover_name=label_col,
            hover_data=tooltip_data,
            color_continuous_scale=color_scale,
            range_color=range_val,
            zoom=zoom_level,
            center={"lat": map_lat, "lon": map_lon},
            size="display_val",
            size_max=30,
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
            df_dist = get_aqi_distribution(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name)
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
            if source_name == "aqiin":
                if spatial_grain not in ["Tỉnh", "Phường"]:
                    # Group and aggregate station-level map_df by province
                    agg_dict = {"display_val": "mean"}
                    if "confidence_score" in map_df.columns:
                        agg_dict["confidence_score"] = "mean"
                    if "confidence_level" in map_df.columns:
                        agg_dict["confidence_level"] = "first"
                    if "source_mix" in map_df.columns:
                        agg_dict["source_mix"] = "first"
                    
                    rank_df = map_df.groupby("province").agg(agg_dict).reset_index()
                    bar_y_col = "province"
                else:
                    # Group and aggregate station-level map_df by ward_name (ward level)
                    group_col = "actual_ward_name" if "actual_ward_name" in map_df.columns else "ward_name"
                    agg_dict = {"display_val": "mean"}
                    if "confidence_score" in map_df.columns:
                        agg_dict["confidence_score"] = "mean"
                    if "confidence_level" in map_df.columns:
                        agg_dict["confidence_level"] = "first"
                    if "source_mix" in map_df.columns:
                        agg_dict["source_mix"] = "first"
                    
                    rank_df = map_df.groupby(["province", group_col]).agg(agg_dict).reset_index()
                    bar_y_col = group_col
            else:
                rank_df = map_df.copy()
                bar_y_col = label_col
                
                # Check confidence score for non-aqiin if applicable
                if "confidence_score" in rank_df.columns:
                    high_conf = rank_df[rank_df["confidence_score"] >= 0.38]
                    if len(high_conf) >= 5:
                        rank_df = high_conf
                        st.caption(
                            "Ranking ưu tiên tỉnh có đóng góp quan trắc AQI.in đáng kể; vùng thuần mô hình được tách khỏi top mặc định."
                            if lang == "vi"
                            else "Ranking prioritizes provinces with meaningful AQI.in monitor contribution; model-only areas are separated from the default top list."
                        )
                    else:
                        st.caption(
                            "Không đủ điểm tin cậy vừa trở lên; chart đang hiển thị cả ước tính mô hình."
                            if lang == "vi"
                            else "Not enough medium-or-better confidence records; chart includes modeled estimates."
                        )

            df_top = rank_df.sort_values("display_val", ascending=False).head(10)
            if "confidence_level" in df_top.columns:
                df_top["confidence_label"] = df_top["confidence_level"].apply(
                    lambda x: localize_confidence_level(x, lang)
                )
            if "source_mix" in df_top.columns:
                df_top["source_label"] = df_top["source_mix"].apply(lambda x: localize_source_mix(x, lang))
            
            df_top = df_top.sort_values("display_val", ascending=True)
            
            hover_options = {}
            if "confidence_level" in df_top.columns:
                hover_options["confidence_label"] = True
            if "source_mix" in df_top.columns:
                hover_options["source_label"] = True
            if "confidence_score" in df_top.columns:
                hover_options["confidence_score"] = ":.2f"

            fig_bar = px.bar(
                df_top,
                y=bar_y_col,
                x="display_val",
                orientation="h",
                color="display_val",
                color_continuous_scale=color_scale,
                range_color=range_val,
                labels={
                    "display_val": val_label,
                    "province":    t("province", lang),
                    "ward_name":   t("location", lang),
                    "actual_ward_name": t("location", lang),
                    "confidence_label": "Độ tin cậy" if lang == "vi" else "Confidence",
                    "source_label": "Nguồn" if lang == "vi" else "Source",
                },
                hover_data=hover_options,
            )
            fig_bar.update_layout(get_plotly_layout(height=400))
            if pollutant == "aqi":
                fig_bar.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
            st.plotly_chart(fig_bar, width="stretch")

# ── 3-Tab Layout Implementation ──────────────────────────────────────────────
tab_ground, tab_sat, tab_corr = st.tabs([
    "📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
    "🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
    "📊 Tương quan & Độ tin cậy" if lang == "vi" else "📊 Correlation & Reliability"
])

with tab_ground:
    render_source_dashboard("aqiin")

with tab_sat:
    render_source_dashboard("openweather")

with tab_corr:
    st.subheader("📊 Tương quan & Độ tin cậy giữa các nguồn")
    st.markdown(
        "Phân tích so sánh dữ liệu thu thập trực tiếp từ các trạm quan trắc mặt đất (AQI.in) "
        "và mô hình lưới vệ tinh SILAM (OpenWeather)."
        if lang == "vi" else
        "Comparative analysis of direct observations from ground monitors (AQI.in) "
        "and SILAM satellite grid model (OpenWeather)."
    )
    
    corr_df = get_source_correlation(
        province=scope_val if spatial_grain in ["Tỉnh", "Phường"] else None,
        start_date=date_range[0].strftime("%Y-%m-%d") if date_range and len(date_range) >= 1 else None,
        end_date=date_range[1].strftime("%Y-%m-%d") if date_range and len(date_range) >= 2 else None,
    )
    
    if corr_df.empty:
        st.plotly_chart(
            render_empty_chart("Không có dữ liệu tương quan cho phạm vi và thời gian này." if lang == "vi" else "No correlation data available."),
            width="stretch"
        )
    else:
        both_sources_df = corr_df[corr_df["aqiin_aqi"].notnull() & corr_df["ow_aqi"].notnull()]
        
        # Aggregated Metrics
        both_sources_count = both_sources_df["province"].nunique()
        total_provinces = corr_df["province"].nunique()
        avg_bias = both_sources_df["aqi_bias"].mean()
        avg_mae = both_sources_df["aqi_mae"].mean()
        
        agree_rows = both_sources_df[both_sources_df["category_agreement"].isin(["both_good", "both_unhealthy"])]
        agree_pct = (len(agree_rows) * 100.0 / len(both_sources_df)) if len(both_sources_df) > 0 else 0
        
        # Reliability Score Card Row
        c_corr1, c_corr2, c_corr3, c_corr4 = st.columns(4)
        with c_corr1:
            if scope_val and spatial_grain in ["Tỉnh", "Phường"]:
                has_ground = "Có trạm hoạt động" if not both_sources_df.empty else "Không trạm mặt đất"
                render_metric_card("Trạm mặt đất" if lang == "vi" else "Ground Monitors", has_ground, icon="sensors")
            else:
                render_metric_card("Tỉnh có cả 2 nguồn" if lang == "vi" else "Provinces w/ Both", f"{both_sources_count}/{total_provinces}", icon="layers")
        with c_corr2:
            bias_text = f"{avg_bias:+.1f} AQI" if not pd.isna(avg_bias) else "N/A"
            render_metric_card("Độ lệch TB (Bias)" if lang == "vi" else "Avg Bias", bias_text, icon="compare_arrows")
        with c_corr3:
            mae_text = f"{avg_mae:.1f} AQI" if not pd.isna(avg_mae) else "N/A"
            render_metric_card("Sai số tuyệt đối (MAE)" if lang == "vi" else "MAE", mae_text, icon="summarize")
        with c_corr4:
            agree_text = f"{agree_pct:.0f}%" if not pd.isna(agree_pct) and len(both_sources_df) > 0 else "N/A"
            render_metric_card("Đồng thuận Category" if lang == "vi" else "Category Agreement", agree_text, icon="fact_check")
            
        # Charts Row 1: Timeline comparisons & Scatter Plot
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.subheader("📈 Xu hướng thời gian theo Nguồn" if lang == "vi" else "📈 Temporal Trend by Source")
            timeline_df = corr_df.groupby("date")[["aqiin_aqi", "ow_aqi"]].mean().reset_index()
            
            fig_timeline = px.line(
                timeline_df, x="date", y=["aqiin_aqi", "ow_aqi"],
                labels={
                    "value": "AQI VN",
                    "variable": "Nguồn" if lang == "vi" else "Source",
                    "date": "Ngày" if lang == "vi" else "Date"
                },
                color_discrete_map={"aqiin_aqi": "#2563eb", "ow_aqi": "#f97316"}
            )
            fig_timeline.data[0].name = "📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors"
            fig_timeline.data[1].name = "🛰️ Vệ tinh" if lang == "vi" else "🛰️ SILAM Satellite"
            fig_timeline.update_layout(get_plotly_layout(height=400), hovermode="x unified")
            st.plotly_chart(fig_timeline, width="stretch")
            
        with col_chart2:
            st.subheader("🎯 Tương quan PM2.5 (Mặt đất vs Vệ tinh)" if lang == "vi" else "🎯 PM2.5 Correlation (Ground vs Sat)")
            if both_sources_df.empty:
                st.plotly_chart(render_empty_chart("Không đủ dữ liệu song hành để vẽ đồ thị tương quan."), width="stretch")
            else:
                r_val = both_sources_df["aqiin_pm25"].corr(both_sources_df["ow_pm25"])
                
                fig_scatter = px.scatter(
                    both_sources_df, x="aqiin_pm25", y="ow_pm25",
                    hover_name="province", hover_data=["date", "aqiin_aqi", "ow_aqi"],
                    labels={
                        "aqiin_pm25": "Nồng độ PM2.5 Trạm mặt đất (µg/m³)" if lang == "vi" else "Ground PM2.5 (µg/m³)",
                        "ow_pm25": "Nồng độ PM2.5 Vệ tinh (µg/m³)" if lang == "vi" else "SILAM PM2.5 (µg/m³)"
                    },
                    color="aqi_bias",
                    color_continuous_scale="RdBu_r"
                )
                fig_scatter.update_layout(get_plotly_layout(height=400))
                st.plotly_chart(fig_scatter, width="stretch")
                if not pd.isna(r_val):
                    st.markdown(f"📊 **Hệ số tương quan tuyến tính Pearson (r):** `{r_val:.2f}` (r gần 1 biểu hiện độ tương quan mạnh).")
                    
        # Charts Row 2: Bias by Province & Ground Spatial Coverage
        col_chart3, col_chart4 = st.columns(2)
        with col_chart3:
            st.subheader("📍 Độ lệch AQI trung bình theo Tỉnh thành" if lang == "vi" else "📍 Avg Bias by Province/City")
            if both_sources_df.empty:
                st.plotly_chart(render_empty_chart("Không có dữ liệu trạm đo song hành."), width="stretch")
            else:
                bias_df = both_sources_df.groupby("province")["aqi_bias"].mean().reset_index()
                bias_df = bias_df.sort_values("aqi_bias", ascending=False).head(15)
                
                fig_bias = px.bar(
                    bias_df, x="aqi_bias", y="province", orientation="h",
                    color="aqi_bias", color_continuous_scale="RdBu_r",
                    labels={
                        "aqi_bias": "Độ lệch AQI (Mặt đất - Vệ tinh)" if lang == "vi" else "AQI Bias (Ground - Sat)",
                        "province": "Tỉnh thành" if lang == "vi" else "Province"
                    }
                )
                fig_bias.update_layout(get_plotly_layout(height=400))
                st.plotly_chart(fig_bias, width="stretch")
                st.caption("Giá trị dương (+) nghĩa là trạm quan trắc mặt đất ghi nhận AQI cao hơn mô hình vệ tinh.")
                
        with col_chart4:
            st.subheader("📶 Tỷ lệ bao phủ trạm mặt đất (%)" if lang == "vi" else "📶 Ground Station Coverage (%)")
            cov_df = get_source_coverage()
            if cov_df.empty:
                st.plotly_chart(render_empty_chart("Không có dữ liệu bao phủ."), width="stretch")
            else:
                cov_df_plot = cov_df.sort_values("aqiin_coverage_pct", ascending=True).head(15)
                
                fig_cov = px.bar(
                    cov_df_plot, x="aqiin_coverage_pct", y="province", orientation="h",
                    color="aqiin_coverage_pct", color_continuous_scale="Blues",
                    labels={
                        "aqiin_coverage_pct": "% Phường/Xã có trạm quan trắc" if lang == "vi" else "Wards with Monitors %",
                        "province": "Tỉnh thành" if lang == "vi" else "Province"
                    }
                )
                fig_cov.update_layout(get_plotly_layout(height=400))
                st.plotly_chart(fig_cov, width="stretch")
                st.caption("Biểu đồ thể hiện mức độ bao phủ trạm mặt đất ở các tỉnh có tỷ lệ thấp nhất.")
