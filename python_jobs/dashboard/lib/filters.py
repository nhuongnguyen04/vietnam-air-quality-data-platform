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
        6:   {"vi": "6 giờ gần nhất", "en": "Last 6 hours"},
        24:  {"vi": "24 giờ gần nhất", "en": "Last 24 hours"},
        48:  {"vi": "48 giờ gần nhất", "en": "Last 48 hours"},
        168: {"vi": "7 ngày gần nhất", "en": "Last 7 days"},
        0:   {"vi": "Tùy chọn khoảng giờ", "en": "Custom hour range"},
    },
    "Ngày": {
        7:   {"vi": "7 ngày gần nhất", "en": "Last 7 days"},
        30:  {"vi": "30 ngày gần nhất", "en": "Last 30 days"},
        90:  {"vi": "3 tháng gần nhất", "en": "Last 3 months"},
        365: {"vi": "1 năm gần nhất", "en": "Last 1 year"},
        0:   {"vi": "Tùy chọn khoảng ngày", "en": "Custom day range"},
    },
    "Tháng": {
        30:  {"vi": "1 tháng gần nhất", "en": "Last 1 month"},
        90:  {"vi": "3 tháng gần nhất", "en": "Last 3 months"},
        365: {"vi": "1 năm gần nhất", "en": "Last 1 year"},
        0:   {"vi": "Tùy chọn khoảng tháng", "en": "Custom month range"},
    },
}

FILTER_LABELS = {
    "time_resolution": {"vi": "Độ phân giải thời gian", "en": "Time Resolution"},
    "time_range": {"vi": "Khoảng thời gian", "en": "Time Range"},
    "custom_date": {"vi": "Chọn khoảng ngày", "en": "Custom Date Range"},
    "pollutant_of_interest": {"vi": "Chất ô nhiễm quan tâm", "en": "Target Pollutant"},
    "latest_data": {"vi": "Dữ liệu mới nhất:", "en": "Latest data:"},
    "latest_data_avail": {"vi": "Dữ liệu mới nhất hiện có:", "en": "Latest available data:"},
    "select_region": {"vi": "Chọn miền", "en": "Select Region"},
    "select_area": {"vi": "Chọn khu vực", "en": "Select Area"},
    "select_province": {"vi": "Chọn tỉnh/thành", "en": "Select Province"},
    "select_default": {"vi": "Chọn", "en": "Select"},
    "spatial_national": {"vi": "Toàn quốc", "en": "National"},
    "spatial_region": {"vi": "Vùng", "en": "Region"},
    "spatial_area": {"vi": "Khu vực", "en": "Area"},
    "spatial_province": {"vi": "Tỉnh", "en": "Province"},
    "spatial_ward": {"vi": "Phường", "en": "Ward"},
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
        WHERE source_mix = 'observed'
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
        WHERE source_mix = 'observed'
        """)
    except Exception:
        return datetime.now().replace(minute=0, second=0, microsecond=0)

    if df.empty or pd.isna(df.iloc[0].latest_hour):
        return datetime.now().replace(minute=0, second=0, microsecond=0)

    return pd.Timestamp(df.iloc[0].latest_hour).to_pydatetime()

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
    """Render unified sidebar filters with collapsible groups and badges."""
    init_filter_state()
    lang = st.session_state.lang
    latest_date = get_latest_available_date()

    # Active filter count calculation
    active_count = 0
    if st.session_state.f_spatial_grain != "Toàn quốc":
        active_count += 1
    if st.session_state.f_time_grain != "Ngày":
        active_count += 1
    if st.session_state.f_time_preset != 30:
        active_count += 1
    if st.session_state.f_pollutant != "aqi":
        active_count += 1

    # Sidebar Header with active count badge
    badge_html = (
        f'<span style="background-color:#0891B2; color:white; padding:2px 8px; '
        f'border-radius:10px; font-size:0.75rem; font-weight:bold; float:right;">'
        f'{active_count} active</span>'
    ) if active_count > 0 else ""
    
    st.sidebar.markdown(
        f"<div style='display:flex; align-items:center; justify-content:space-between; "
        f"margin-top: 1rem; margin-bottom: 1.5rem;'>"
        f"<h3 style='margin:0; font-family:\"Outfit\",sans-serif; font-size:1.25rem;'>"
        f"⚙️ {t('filter_title', lang)}</h3>{badge_html}</div>", 
        unsafe_allow_html=True
    )

    # 🔄 Reset Filters Button
    if st.sidebar.button("🔄 " + ("Reset Filters" if lang == "en" else "Đặt lại bộ lọc"), use_container_width=True):
        st.session_state.f_spatial_grain = "Toàn quốc"
        st.session_state.f_scope_val = None
        st.session_state.f_time_grain = "Ngày"
        st.session_state.f_time_preset = 30
        st.session_state.f_date_range = [latest_date - timedelta(days=30), latest_date]
        st.session_state.f_pollutant = "aqi"
        st.rerun()

    st.sidebar.markdown("<div style='margin-bottom:0.75rem;'></div>", unsafe_allow_html=True)

    # 📍 Group 1: Geography (Spatial Scope)
    with st.sidebar.expander("📍 " + ("Geography" if lang == "en" else "Phạm vi địa lý"), expanded=True):
        spatial_options = ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh", "Phường"]
        spatial_map = {
            "Toàn quốc": FILTER_LABELS["spatial_national"][lang],
            "Vùng": FILTER_LABELS["spatial_region"][lang],
            "Khu vực": FILTER_LABELS["spatial_area"][lang],
            "Tỉnh": FILTER_LABELS["spatial_province"][lang],
            "Phường": FILTER_LABELS["spatial_ward"][lang],
        }
        spatial_idx = (
            spatial_options.index(st.session_state.f_spatial_grain) 
            if st.session_state.f_spatial_grain in spatial_options else 0
        )
        st.session_state.f_spatial_grain = st.selectbox(
            t("filter_spatial_grain", lang),
            spatial_options,
            format_func=lambda x: spatial_map[x],
            index=spatial_idx,
            key="sb_spatial_grain"
        )

        hierarchy_df = get_hierarchy_metadata()
        scope_disabled = st.session_state.f_spatial_grain == "Toàn quốc"

        scope_choices = {
            "Vùng":    (FILTER_LABELS["select_region"][lang], sorted(hierarchy_df["region_3"].unique())),
            "Khu vực": (FILTER_LABELS["select_area"][lang], sorted(hierarchy_df["region_8"].unique())),
            "Tỉnh":    (FILTER_LABELS["select_province"][lang], sorted(hierarchy_df["province"].unique())),
            "Phường":  (FILTER_LABELS["select_province"][lang], sorted(hierarchy_df["province"].unique())),
        }.get(st.session_state.f_spatial_grain, (FILTER_LABELS["select_default"][lang], []))

        scope_label, scope_options = scope_choices

        current_scope = st.session_state.f_scope_val
        scope_idx = scope_options.index(current_scope) if current_scope in scope_options else 0

        if not scope_disabled:
            st.session_state.f_scope_val = st.selectbox(
                scope_label, scope_options, index=scope_idx, key="sb_scope_val"
            )
        else:
            st.session_state.f_scope_val = None

    # 🔬 Group 2: Target & Pollutant
    with st.sidebar.expander("🔬 " + ("Metrics" if lang == "en" else "Chỉ số & Chất ô nhiễm"), expanded=True):
        pollutant_map = {
            "aqi":  "AQI Tổng hợp" if lang == "vi" else "Composite AQI",
            "pm25": "PM2.5",
            "pm10": "PM10",
            "no2":  "NO2",
            "o3":   "O3",
            "so2":  "SO2",
            "co":   "CO",
        }
        poll_keys = list(pollutant_map.keys())
        poll_idx = poll_keys.index(st.session_state.f_pollutant) if st.session_state.f_pollutant in poll_keys else 0
        st.session_state.f_pollutant = st.selectbox(
            FILTER_LABELS["pollutant_of_interest"][lang],
            poll_keys,
            format_func=lambda x: pollutant_map[x],
            index=poll_idx,
            key="sb_pollutant"
        )
        st.session_state.f_standard = st.session_state.get("standard", "VN_AQI")

    # ⏱ Group 3: Temporal Settings
    with st.sidebar.expander("⏱ " + ("Timeframe" if lang == "en" else "Thời gian"), expanded=True):
        time_grain_options = ["Giờ", "Ngày", "Tháng"]
        tg_map = {
            "Giờ": "Hour" if lang == "en" else "Giờ",
            "Ngày": "Day" if lang == "en" else "Ngày",
            "Tháng": "Month" if lang == "en" else "Tháng"
        }
        prev_grain = st.session_state.get("f_time_grain", "Ngày")
        tg_idx = time_grain_options.index(prev_grain) if prev_grain in time_grain_options else 1
        new_grain = st.selectbox(
            FILTER_LABELS["time_resolution"][lang],
            time_grain_options,
            format_func=lambda x: tg_map[x],
            index=tg_idx,
            key="sb_time_grain"
        )

        if new_grain != prev_grain:
            st.session_state.f_time_grain = new_grain
            st.session_state.f_time_preset = GRAIN_DEFAULT_PRESET[new_grain]

        st.session_state.f_time_grain = new_grain
        time_grain = new_grain

        time_options = GRAIN_TIME_OPTIONS[time_grain]
        preset_keys = list(time_options.keys())

        current_preset = st.session_state.f_time_preset
        if current_preset not in preset_keys:
            current_preset = GRAIN_DEFAULT_PRESET[time_grain]
            st.session_state.f_time_preset = current_preset

        preset_idx = preset_keys.index(current_preset)

        selected_preset = st.selectbox(
            FILTER_LABELS["time_range"][lang],
            preset_keys,
            format_func=lambda x: time_options[x][lang],
            index=preset_idx,
            key="sb_time_preset"
        )
        st.session_state.f_time_preset = selected_preset

        # Compute date ranges
        if time_grain == "Giờ":
            latest_dt = get_latest_available_datetime()
            latest_label = latest_dt.strftime("%H:%M %d/%m/%Y")
            now = datetime.now().replace(minute=0, second=0, microsecond=0)
            if latest_dt < now:
                st.warning(f"{FILTER_LABELS['latest_data'][lang]} {latest_label}")
            else:
                st.caption(f"{FILTER_LABELS['latest_data'][lang]} {latest_label}")

            if selected_preset != 0:
                end_dt = latest_dt
                start_dt = end_dt - timedelta(hours=max(selected_preset - 1, 0))
                st.session_state.f_date_range = [start_dt, end_dt]
            else:
                custom_dates = st.date_input(
                    FILTER_LABELS["custom_date"][lang],
                    value=[latest_dt.date() - timedelta(days=1), latest_dt.date()],
                    max_value=latest_dt.date(),
                    key="sb_custom_dates_hour"
                )
                if isinstance(custom_dates, list | tuple) and len(custom_dates) == 2:
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
            latest_date = get_latest_available_date()
            today = datetime.now().date()
            latest_label = latest_date.strftime("%d/%m/%Y")
            if latest_date < today:
                st.warning(f"{FILTER_LABELS['latest_data_avail'][lang]} {latest_label}")
            else:
                st.caption(f"{FILTER_LABELS['latest_data'][lang]} {latest_label}")

            if selected_preset != 0:
                end_date = latest_date
                start_date = end_date - timedelta(days=max(selected_preset - 1, 0))
                st.session_state.f_date_range = [start_date, end_date]
            else:
                st.session_state.f_date_range = st.date_input(
                    FILTER_LABELS["custom_date"][lang],
                    value=st.session_state.f_date_range,
                    max_value=latest_date,
                    key="sb_custom_dates_day"
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
