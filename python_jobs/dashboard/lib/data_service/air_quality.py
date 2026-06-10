import streamlit as st
import pandas as pd
from lib.clickhouse_client import query_df
from lib.i18n import t
from lib.aqi_utils import AQI_VN_SQL_EXPR

from .core import (
    escape_value,
    build_where_clause,
    get_source_mix,
    get_pollutant_cols,
    build_date_comparison_ranges,
    SOURCE_MIX_LABELS,
    CONFIDENCE_LABELS,
    POLLUTANT_LABELS,
)

@st.cache_data(ttl=3600)
def get_hierarchy_metadata():
    """Fetch regions and provinces from the centralized Dimension table."""
    q = """
    SELECT DISTINCT
        region_3,
        region_8,
        province
    FROM air_quality.dim_administrative_units
    ORDER BY region_3, region_8, province
    """
    return query_df(q)

def get_ward_list(province: str):
    """Fetch list of wards from the centralized Dimension table."""
    escaped_province = escape_value(province)
    q = f"SELECT DISTINCT ward_code, ward_name FROM air_quality.dim_administrative_units WHERE province = '{escaped_province}' ORDER BY ward_name"
    return query_df(q)

def localize_source_mix(source_mix: str | None, lang: str = "vi") -> str:
    """Return a localized source-mix label for dashboard display."""
    if not source_mix:
        return "N/A"
    return SOURCE_MIX_LABELS.get(source_mix, {}).get(lang, source_mix)

def localize_confidence_level(confidence_level: str | None, lang: str = "vi") -> str:
    """Return a localized confidence label for dashboard display."""
    if not confidence_level:
        return "N/A"
    return CONFIDENCE_LABELS.get(confidence_level, {}).get(lang, confidence_level)

@st.cache_data(ttl=600)
def get_source_coverage(province: str = None) -> list:
    """Fetch ground station coverage per province or nation-wide from dm_source_coverage."""
    where_clause = f"WHERE province = '{escape_value(province)}'" if province else ""
    q = f"""
    SELECT
        province,
        total_ward_count,
        aqiin_ward_count,
        ow_ward_count,
        aqiin_coverage_pct,
        aqiin_latest_hour,
        ow_latest_hour
    FROM air_quality.dm_source_coverage
    {where_clause}
    ORDER BY aqiin_coverage_pct DESC
    """
    return query_df(q)

@st.cache_data(ttl=600)
def get_source_correlation(province: str = None, start_date=None, end_date=None) -> list:
    """Fetch correlation, bias and MAE metrics from dm_source_correlation_daily."""
    clauses = []
    if province:
        clauses.append(f"province = '{escape_value(province)}'")
    if start_date:
        if hasattr(start_date, "strftime"):
            start_str = start_date.strftime("%Y-%m-%d")
        else:
            start_str = escape_value(start_date)
        clauses.append(f"date >= '{start_str}'")
    if end_date:
        if hasattr(end_date, "strftime"):
            end_str = end_date.strftime("%Y-%m-%d")
        else:
            end_str = escape_value(end_date)
        clauses.append(f"date <= '{end_str}'")

    where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
    q = f"""
    SELECT
        date,
        province,
        aqiin_aqi,
        ow_aqi,
        aqiin_pm25,
        ow_pm25,
        aqi_bias,
        aqi_mae,
        aqi_pct_diff,
        category_agreement,
        aqiin_wards,
        ow_wards,
        has_ground_data,
        ground_coverage_pct
    FROM air_quality.dm_source_correlation_daily
    {where_clause}
    ORDER BY date, province
    """
    return query_df(q)

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

def get_national_summary(table, col, m_col, grain, scope, dates, tunit, source_name="blended"):
    """KPI summary — follows the active filter (time_unit aware)."""
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(None, scope, dates, time_unit=tunit, source_mix=source_mix)
    q = f"""
    SELECT
        avg({col}) as avg_val,
        max(if({m_col} is not null, {m_col}, {col})) as max_val,
        argMax(province, if({m_col} is not null, {m_col}, {col})) as max_val_province,
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
        is_aqi = "aqi" in col
        if is_aqi:
            pollutant_name = "aqi"
            standard_name = "VN_AQI" if "vn" in col else "US_AQI"
        else:
            pollutant_name = col.split("_")[0]
            standard_name = "VN_AQI"

        if dates and len(dates) == 2:
            start_val = dates[0].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[0], "hour") else f"{dates[0]} 00:00:00"
            end_val = dates[1].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[1], "hour") else f"{dates[1]} 23:59:59"
            date_filter = f"timestamp_utc BETWEEN toDateTime('{escape_value(start_val)}') AND toDateTime('{escape_value(end_val)}')"
        elif dates and len(dates) == 1:
            val = dates[0].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[0], "hour") else f"{dates[0]} 00:00:00"
            date_filter = f"timestamp_utc >= toDateTime('{escape_value(val)}')"
        else:
            date_filter = "1=1"

        spatial_filters = []
        if grain == "Vùng" and scope:
            spatial_filters.append(f"d.region_3 = '{escape_value(scope)}'")
        elif grain == "Khu vực" and scope:
            spatial_filters.append(f"d.region_8 = '{escape_value(scope)}'")
        elif grain in ["Tỉnh", "Phường"] and scope:
            spatial_filters.append(f"s.province = '{escape_value(scope)}'")

        spatial_filter_str = " AND ".join(spatial_filters) if spatial_filters else "1=1"

        if pollutant_name == "aqi":
            if standard_name == "VN_AQI":
                aqi_vn_expr = AQI_VN_SQL_EXPR
                overall_aqi_expr = f"max({aqi_vn_expr})"
                main_poll_expr = f"argMax(parameter, {aqi_vn_expr})"
            else:
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
                FROM air_quality.int_observed__processed
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
                '{escape_value(pollutant_name)}' AS main_pollutant,
                1.0 AS confidence_score,
                'high' AS confidence_level,
                'observed' AS source_mix,
                count(distinct m.timestamp_utc) AS aqiin_observation_count,
                0 AS openweather_observation_count
            FROM air_quality.int_observed__processed m
            JOIN air_quality.stg_core__stations s ON m.station_name = s.station_name
            LEFT JOIN air_quality.dim_administrative_units d ON s.ward_code = d.ward_code
            WHERE m.parameter = '{escape_value(pollutant_name)}'
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
    if not df.empty and "lat" in df.columns:
        df = df.rename(columns={"lat": "latitude", "lon": "longitude"})
    return df

@st.cache_data(ttl=300)
def get_aqi_distribution(table, col, grain, scope, dates, tunit, source_name="blended", lang="vi"):
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

def generate_insights(filters: dict, lang: str = "vi", theme: str = "light") -> list:
    """Generate dynamic insights based on active filters and ClickHouse data."""
    date_range = filters.get("date_range")
    time_grain = filters.get("time_grain", "Ngày")
    pollutant = filters.get("pollutant", "aqi")
    standard = filters.get("standard", "VN_AQI")
    
    ranges = build_date_comparison_ranges(date_range, time_grain)
    between_expr_curr = ranges["between_expr_curr"]
    between_expr_prev = ranges["between_expr_prev"]
    
    avg_col, _ = get_pollutant_cols(pollutant, standard)
    
    source_name = st.session_state.get("overview_source", "ground")
    source_mix = "observed" if source_name == "ground" else "modeled" if source_name == "satellite" else None
    
    source_clause = ""
    if source_mix:
        source_clause = f"AND source_mix = '{escape_value(source_mix)}'"
        
    table_name = "dm_air_quality_overview_hourly" if time_grain == "Giờ" else "dm_air_quality_overview_daily"
    p_label = POLLUTANT_LABELS.get(pollutant, pollutant.upper())

    # --- INSIGHT 1: Worst Trend ---
    insight1 = {
        "type": "worst_trend",
        "title": "XU HƯỚNG XẤU" if lang == "vi" else "WORST TREND",
        "icon": "📈",
        "icon_color": "#ef4444",
        "title_color": "#f87171" if theme == "dark" else "#ef4444",
        "message": (
            "Chất lượng không khí toàn quốc có xu hướng cải thiện hoặc ổn định so với giai đoạn trước."
            if lang == "vi" else
            "National air quality shows a stable or improving trend compared to the previous period."
        )
    }
    
    try:
        q_trend = f"""
        SELECT 
            province,
            avg(if({between_expr_prev}, {avg_col}, null)) as prev_avg,
            avg(if({between_expr_curr}, {avg_col}, null)) as curr_avg
        FROM air_quality.{table_name}
        WHERE province != '' AND {avg_col} IS NOT NULL {source_clause}
        GROUP BY province
        HAVING prev_avg > 0 AND curr_avg > prev_avg
        ORDER BY (curr_avg - prev_avg) / prev_avg DESC
        LIMIT 1
        """
        df_trend = query_df(q_trend)
        if not df_trend.empty:
            row = df_trend.iloc[0]
            pct = ((row.curr_avg - row.prev_avg) / row.prev_avg) * 100
            prov = row.province
            if lang == "vi":
                insight1["message"] = f"{prov} có xu hướng ô nhiễm tăng mạnh nhất — {p_label} tăng {pct:.0f}% so với giai đoạn trước."
            else:
                insight1["message"] = f"{prov} shows the steepest deteriorating trend — {p_label} increased by {pct:.0f}% compared to the previous period."
    except Exception:
        pass

    # --- INSIGHT 2: Weather stagnant ---
    insight2 = {
        "type": "weather",
        "title": "THỜI TIẾT" if lang == "vi" else "WEATHER",
        "icon": "💨",
        "icon_color": "#60a5fa",
        "title_color": "#93c5fd" if theme == "dark" else "#3b82f6",
        "message": (
            "Tốc độ gió trung bình ôn hòa góp phần phân tán tốt chất ô nhiễm trên toàn quốc."
            if lang == "vi" else
            "Moderate average wind speeds contributed to good pollutant dispersion nationwide."
        )
    }
    
    try:
        q_weather = f"""
        SELECT 
            province,
            avg(wind_daily_avg) as avg_wind,
            avg(stagnant_hours) as avg_stagnant_hours,
            avg(pm25_daily_avg) as avg_pm25
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE date BETWEEN '{escape_value(ranges["curr_start"].strftime("%Y-%m-%d"))}' AND '{escape_value(ranges["curr_end"].strftime("%Y-%m-%d"))}' AND province != ''
        GROUP BY province
        ORDER BY avg_stagnant_hours DESC, avg_pm25 DESC
        LIMIT 1
        """
        df_weather = query_df(q_weather)
        if not df_weather.empty:
            row = df_weather.iloc[0]
            prov = row.province
            wind = row.avg_wind
            hours = row.avg_stagnant_hours
            if hours > 1.0:
                if lang == "vi":
                    insight2["message"] = f"Gió yếu (TB {wind:.1f} m/s) tại {prov} Góp phần tích tụ {hours:.1f} giờ lặng gió gần đây, làm tăng nồng độ bụi."
                else:
                    insight2["message"] = f"Light winds (avg {wind:.1f} m/s) in {prov} contributed to {hours:.1f} hours of stagnant air recently, building up dust."
    except Exception:
        pass

    # --- INSIGHT 3: Best performing ---
    insight3 = {
        "type": "best",
        "title": "TỐT NHẤT" if lang == "vi" else "BEST PERFORMING",
        "icon": "✅",
        "icon_color": "#34d399",
        "title_color": "#6ee7b7" if theme == "dark" else "#10b981",
        "message": (
            "Không có dữ liệu xếp hạng chất lượng không khí tốt."
            if lang == "vi" else
            "No data available for best performing regions."
        )
    }
    
    try:
        q_best = f"""
        SELECT 
            province,
            avg({avg_col}) as avg_val
        FROM air_quality.{table_name}
        WHERE {between_expr_curr} AND province != '' AND {avg_col} IS NOT NULL {source_clause}
        GROUP BY province
        ORDER BY avg_val ASC
        LIMIT 2
        """
        df_best = query_df(q_best)
        if not df_best.empty:
            provs = ", ".join(df_best["province"].tolist())
            lowest_val = df_best.iloc[0].avg_val
            if lang == "vi":
                insight3["message"] = f"{provs} duy trì chất lượng không khí tốt nhất với {p_label} trung bình chỉ từ {lowest_val:.0f}."
            else:
                insight3["message"] = f"{provs} recorded the cleanest air quality, with average {p_label} as low as {lowest_val:.0f}."
    except Exception:
        pass

    return [insight1, insight2, insight3]
