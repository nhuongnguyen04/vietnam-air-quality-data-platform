"""Data service for hierarchical filtering and optimal source selection.
Redirected to Analytics-First Layer (dm_* tables).
"""
import streamlit as st

from .aqi_utils import AQI_VN_SQL_EXPR
from .clickhouse_client import query_df
from .i18n import t


def escape_value(val) -> str:
    """Safely escape backslashes and single quotes in string values to prevent SQL injection.
    
    Args:
        val: Any value to be escaped for a SQL single-quoted literal.
        
    Returns:
        The safely escaped string value.
    """
    if val is None:
        return ""
    if not isinstance(val, str):
        val = str(val)
    # Escape backslash first, then single quotes
    return val.replace("\\", "\\\\").replace("'", "\\'")



# Mapping of temporal grains to dbt ANALYTICS models (dm_*)
TIME_GRAIN_TABLE = {
    "Giờ": "dm_air_quality_overview_hourly",
    "Ngày": "dm_air_quality_overview_daily",
    "Tháng": "dm_air_quality_overview_monthly",
}

def get_source_mix(source: str) -> str:
    """Map source dropdown value to the unified source_mix database value."""
    if source == "openweather":
        return "modeled"
    return "observed"

def get_source_table(spatial_grain: str, time_grain: str, source: str = "blended") -> str:
    """Return the analytical table name for the given selection and source."""
    return TIME_GRAIN_TABLE.get(time_grain, "dm_air_quality_overview_daily")



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

def get_pollutant_col(pollutant: str, standard: str = "VN_AQI") -> str:
    """Map pollutant filter to database column name."""
    if pollutant == "aqi":
        # Handle "WHO 2021" or other non-VN_AQI labels from app.py
        return "avg_aqi_vn" if standard == "VN_AQI" else "avg_aqi_us"

    # For now, return the _avg concentrations.
    # In future, we might want _aqi versions for each pollutant.
    mapping = {
        "pm25": "pm25_avg",
        "pm10": "pm10_avg",
        "co": "co_avg",
        "no2": "no2_avg",
        "so2": "so2_avg",
        "o3": "o3_avg"
    }
    return mapping.get(pollutant, "avg_aqi_vn")

def get_pollutant_cols(pollutant: str, standard: str = "VN_AQI") -> tuple[str, str]:
    """Map pollutant filter to (avg_col, max_col) names.
    Safely handles the fact that dm_* tables only store max columns for AQI.
    """
    avg_col = get_pollutant_col(pollutant, standard)

    if pollutant == "aqi":
        # AQI has both avg and max columns in dm_* tables
        max_col = avg_col.replace("avg", "max")
    else:
        # Specific pollutants (PM25, etc) only have averages in dm_* summary tables
        max_col = avg_col

    return avg_col, max_col


SOURCE_MIX_LABELS = {
    "observed": {"vi": "Quan trắc", "en": "Observed"},
    "mixed": {"vi": "Kết hợp", "en": "Mixed"},
    "modeled": {"vi": "Mô hình", "en": "Modeled"},
}

CONFIDENCE_LABELS = {
    "high": {"vi": "Tin cậy cao", "en": "High confidence"},
    "medium": {"vi": "Tin cậy vừa", "en": "Medium confidence"},
    "low": {"vi": "Ước tính mô hình", "en": "Modeled estimate"},
}

CONFIDENCE_COLORS = {
    "high": "#16a34a",
    "medium": "#f59e0b",
    "low": "#ef4444",
}


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


def build_where_clause(
    spatial_scope: str,
    spatial_value: str,
    date_range=None,
    date_col: str = "date",
    time_unit: str = "day",
    source_mix: str = None,
):
    """Construct dynamic WHERE clause based on hierarchical filters.

    Args:
        spatial_scope: "Vùng", "Khu vực", "Tỉnh", "Phường", or None
        spatial_value: the selected scope value
        date_range: list/tuple of 2 date/datetime objects
        date_col: override the date column name (ignored when time_unit="hour")
        time_unit: "hour" uses datetime_hour + toDateTime(); "day" uses date column
        source_mix: optionally filter by source_mix ("observed" or "modeled")
    """
    clauses = []
    if source_mix:
        clauses.append(f"source_mix = '{escape_value(source_mix)}'")

    if spatial_scope == "Vùng" and spatial_value:
        clauses.append(f"region_3 = '{escape_value(spatial_value)}'")
    elif spatial_scope == "Khu vực" and spatial_value:
        clauses.append(f"region_8 = '{escape_value(spatial_value)}'")
    elif spatial_scope in ["Tỉnh", "Phường"] and spatial_value:
        clauses.append(f"province = '{escape_value(spatial_value)}'")

    if date_range:
        if time_unit == "hour":
            # Use datetime_hour column with toDateTime() for precise hourly filtering
            formatted = []
            for d in date_range:
                if hasattr(d, "strftime"):
                    formatted.append(d.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    formatted.append(escape_value(d))
            if len(formatted) == 2:
                clauses.append(
                    f"datetime_hour BETWEEN toDateTime('{formatted[0]}') "
                    f"AND toDateTime('{formatted[1]}')"
                )
            elif len(formatted) == 1:
                clauses.append(f"datetime_hour = toDateTime('{formatted[0]}')")
        else:
            # Day-level filtering — use whichever date_col is appropriate
            formatted = []
            for d in date_range:
                if hasattr(d, "strftime"):
                    formatted.append(d.strftime("%Y-%m-%d"))
                else:
                    formatted.append(escape_value(d)[:10])  # truncate to YYYY-MM-DD
            if len(formatted) == 2:
                start_date, end_date = formatted
                clauses.append(f"{date_col} BETWEEN '{start_date}' AND '{end_date}'")
            elif len(formatted) == 1:
                clauses.append(f"{date_col} = '{formatted[0]}'")

    return " AND ".join(clauses) if clauses else "1=1"


@st.cache_data(ttl=600)
def get_source_coverage(province: str = None) -> list:
    """Fetch ground station coverage per province or nation-wide from dm_source_coverage."""
    escaped_province = escape_value(province)
    where_clause = f"WHERE province = '{escaped_province}'" if province else ""
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
        # start_date might be string or date object
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


# Caching bypassed to ensure instant, 100% reactive KPI card updating across filter changes
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
            date_filter = f"timestamp_utc BETWEEN toDateTime('{escape_value(start_val)}') AND toDateTime('{escape_value(end_val)}')"
        elif dates and len(dates) == 1:
            val = dates[0].strftime("%Y-%m-%d %H:%M:%S") if hasattr(dates[0], "hour") else f"{dates[0]} 00:00:00"
            date_filter = f"timestamp_utc >= toDateTime('{escape_value(val)}')"
        else:
            date_filter = "1=1"

        # Construct spatial filter
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
                # Compute VN AQI on the fly using standard formulas from calculate_aqi_vn macro
                aqi_vn_expr = AQI_VN_SQL_EXPR
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
                '{escape_value(pollutant_name)}' AS main_pollutant,
                1.0 AS confidence_score,
                'high' AS confidence_level,
                'observed' AS source_mix,
                count(distinct m.timestamp_utc) AS aqiin_observation_count,
                0 AS openweather_observation_count
            FROM air_quality.stg_aqiin__measurements m
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
            # Ward-level: show individual ward dots on the map
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


