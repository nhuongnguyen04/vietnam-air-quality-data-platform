from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from .clickhouse_client import query_df
from .data_service import get_hierarchy_metadata
from .i18n import t

# ── Per-grain time preset options ──────────────────────────────────────────────
# Keys for "Giờ" are HOURS; keys for "Ngày"/"Tháng" are DAYS.
GRAIN_TIME_OPTIONS = {
    "Giờ": {
        6:   "6 giờ gần nhất",
        24:  "24 giờ gần nhất",
        48:  "48 giờ gần nhất",
        168: "7 ngày gần nhất",
        0:   "Tùy chọn khoảng giờ",
    },
    "Ngày": {
        7:   "7 ngày gần nhất",
        30:  "30 ngày gần nhất",
        90:  "3 tháng gần nhất",
        365: "1 năm gần nhất",
        0:   "Tùy chọn khoảng ngày",
    },
    "Tháng": {
        30:  "1 tháng gần nhất",
        90:  "3 tháng gần nhất",
        365: "1 năm gần nhất",
        0:   "Tùy chọn khoảng tháng",
    },
}

# Default preset (unit-value) per grain
GRAIN_DEFAULT_PRESET = {
    "Giờ":  24,
    "Ngày": 30,
    "Tháng": 90,
}


@st.cache_data(ttl=300)
def get_latest_available_date():
    """Return the latest date available in the daily dashboard mart."""
    try:
        df = query_df("""
        SELECT max(date) AS latest_date
        FROM air_quality.dm_air_quality_overview_daily
        """)
    except Exception:
        return datetime.now().date()

    if df.empty or pd.isna(df.iloc[0].latest_date):
        return datetime.now().date()

    return pd.Timestamp(df.iloc[0].latest_date).date()


@st.cache_data(ttl=120)
def get_latest_available_datetime():
    """Return the latest datetime_hour available in the hourly mart."""
    try:
        df = query_df("""
        SELECT max(datetime_hour) AS latest_hour
        FROM air_quality.dm_air_quality_overview_hourly
        """)
    except Exception:
        return datetime.now().replace(minute=0, second=0, microsecond=0)

    if df.empty or pd.isna(df.iloc[0].latest_hour):
        return datetime.now().replace(minute=0, second=0, microsecond=0)

    return pd.Timestamp(df.iloc[0].latest_hour).to_pydatetime()


def _valid_preset_for_grain(preset, grain):
    """Return True if the preset key is valid for the given grain."""
    return preset in GRAIN_TIME_OPTIONS.get(grain, {})


def init_filter_state():
    """Initialize session state for filters if not already present."""
    latest_date = get_latest_available_date()
    defaults = {
        "f_spatial_grain": "Toàn quốc",
        "f_scope_val": None,
        "f_time_grain": "Ngày",
        "f_time_preset": 30,
        "f_date_range": [latest_date - timedelta(days=30), latest_date],
        "f_pollutant": "aqi",
        "f_standard": st.session_state.get("standard", "VN_AQI"),
        "lang": st.session_state.get("lang", "vi"),
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def render_sidebar_filters():
    """Render unified sidebar filters and update session state."""
    init_filter_state()
    lang = st.session_state.lang

    st.sidebar.header(t("filter_title", lang))

    # 0. Time Grain (Resolution)
    time_grain_options = ["Giờ", "Ngày", "Tháng"]
    prev_grain = st.session_state.get("f_time_grain", "Ngày")
    tg_idx = time_grain_options.index(prev_grain) if prev_grain in time_grain_options else 1
    new_grain = st.sidebar.selectbox(
        "Độ phân giải thời gian",
        time_grain_options,
        index=tg_idx,
    )

    # If grain changed, reset preset to a sensible default for the new grain
    if new_grain != prev_grain:
        st.session_state.f_time_grain = new_grain
        st.session_state.f_time_preset = GRAIN_DEFAULT_PRESET[new_grain]

    st.session_state.f_time_grain = new_grain
    time_grain = new_grain

    # 1. Spatial Grain
    spatial_options = ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh", "Phường"]
    spatial_idx = spatial_options.index(st.session_state.f_spatial_grain) if st.session_state.f_spatial_grain in spatial_options else 0
    st.session_state.f_spatial_grain = st.sidebar.selectbox(
        t("filter_spatial_grain", lang),
        spatial_options,
        index=spatial_idx,
    )

    # 2. Scope (Province/Region selection)
    hierarchy_df = get_hierarchy_metadata()
    scope_disabled = st.session_state.f_spatial_grain == "Toàn quốc"

    scope_choices = {
        "Vùng":    ("Chọn miền", sorted(hierarchy_df["region_3"].unique())),
        "Khu vực": ("Chọn khu vực", sorted(hierarchy_df["region_8"].unique())),
        "Tỉnh":    ("Chọn tỉnh/thành", sorted(hierarchy_df["province"].unique())),
        "Phường":  ("Chọn tỉnh/thành", sorted(hierarchy_df["province"].unique())),
    }.get(st.session_state.f_spatial_grain, ("Chọn", []))

    scope_label, scope_options = scope_choices

    current_scope = st.session_state.f_scope_val
    if current_scope not in scope_options:
        scope_idx = 0
    else:
        scope_idx = scope_options.index(current_scope)

    if not scope_disabled:
        st.session_state.f_scope_val = st.sidebar.selectbox(
            scope_label, scope_options, index=scope_idx
        )
    else:
        st.session_state.f_scope_val = None

    st.sidebar.markdown("---")

    # 3. Pollutant Filter
    pollutant_map = {
        "aqi":  "AQI Tổng hợp",
        "pm25": "PM2.5",
        "pm10": "PM10",
        "no2":  "NO2",
        "o3":   "O3",
        "so2":  "SO2",
        "co":   "CO",
    }
    poll_keys = list(pollutant_map.keys())
    poll_idx = poll_keys.index(st.session_state.f_pollutant) if st.session_state.f_pollutant in poll_keys else 0
    st.session_state.f_pollutant = st.sidebar.selectbox(
        "Chất ô nhiễm quan tâm" if lang == "vi" else "Target Pollutant",
        poll_keys,
        format_func=lambda x: pollutant_map[x],
        index=poll_idx,
    )

    # 4. Standard is handled globally in app.py
    st.session_state.f_standard = st.session_state.get("standard", "VN_AQI")

    st.sidebar.markdown("---")

    # 5. Time Filter — grain-aware presets
    time_options = GRAIN_TIME_OPTIONS[time_grain]
    preset_keys = list(time_options.keys())

    # Validate/reset current preset for this grain
    current_preset = st.session_state.f_time_preset
    if current_preset not in preset_keys:
        current_preset = GRAIN_DEFAULT_PRESET[time_grain]
        st.session_state.f_time_preset = current_preset

    preset_idx = preset_keys.index(current_preset)

    selected_preset = st.sidebar.selectbox(
        "Khoảng thời gian",
        preset_keys,
        format_func=lambda x: time_options[x],
        index=preset_idx,
    )
    st.session_state.f_time_preset = selected_preset

    # ── Compute date_range based on grain ─────────────────────────────────────
    if time_grain == "Giờ":
        latest_dt = get_latest_available_datetime()
        latest_label = latest_dt.strftime("%H:%M %d/%m/%Y")
        now = datetime.now().replace(minute=0, second=0, microsecond=0)
        if latest_dt < now:
            st.sidebar.warning(f"Dữ liệu mới nhất: {latest_label}")
        else:
            st.sidebar.caption(f"Dữ liệu mới nhất: {latest_label}")

        if selected_preset != 0:
            end_dt = latest_dt
            start_dt = end_dt - timedelta(hours=max(selected_preset - 1, 0))
            st.session_state.f_date_range = [start_dt, end_dt]
        else:
            # Custom: use date_input but convert to datetime at start/end of day
            custom_dates = st.sidebar.date_input(
                "Chọn khoảng ngày",
                value=[latest_dt.date() - timedelta(days=1), latest_dt.date()],
                max_value=latest_dt.date(),
            )
            if isinstance(custom_dates, (list, tuple)) and len(custom_dates) == 2:
                st.session_state.f_date_range = [
                    datetime.combine(custom_dates[0], datetime.min.time()),
                    datetime.combine(custom_dates[1], datetime.max.time().replace(microsecond=0)),
                ]
            else:
                st.session_state.f_date_range = [
                    datetime.combine(custom_dates, datetime.min.time()),
                    datetime.combine(custom_dates, datetime.max.time().replace(microsecond=0)),
                ]

        time_unit = "hour"

    else:
        # Daily / Monthly grain — date-level precision
        latest_date = get_latest_available_date()
        today = datetime.now().date()
        latest_label = latest_date.strftime("%d/%m/%Y")
        if latest_date < today:
            st.sidebar.warning(f"Dữ liệu mới nhất hiện có: {latest_label}")
        else:
            st.sidebar.caption(f"Dữ liệu mới nhất: {latest_label}")

        if selected_preset != 0:
            end_date = latest_date
            start_date = end_date - timedelta(days=max(selected_preset - 1, 0))
            st.session_state.f_date_range = [start_date, end_date]
        else:
            st.session_state.f_date_range = st.sidebar.date_input(
                "Chọn khoảng ngày",
                value=st.session_state.f_date_range,
                max_value=latest_date,
            )

        time_unit = "day"

    return {
        "spatial_grain": st.session_state.f_spatial_grain,
        "time_grain":    time_grain,
        "time_unit":     time_unit,
        "scope_val":     st.session_state.f_scope_val,
        "pollutant":     st.session_state.f_pollutant,
        "standard":      st.session_state.f_standard,
        "date_range":    st.session_state.f_date_range,
        "time_preset":   st.session_state.f_time_preset,
    }
