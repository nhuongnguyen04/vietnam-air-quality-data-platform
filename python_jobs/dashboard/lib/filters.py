from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

from .clickhouse_client import query_df
from .data_service import get_hierarchy_metadata
from .i18n import t

# ── Per-grain time preset options ──────────────────────────────────────────────
GRAIN_TIME_OPTIONS = {
    "Giờ": {
        6:   {"vi": "6 giờ", "en": "6 hours"},
        24:  {"vi": "24 giờ", "en": "24 hours"},
        48:  {"vi": "48 giờ", "en": "48 hours"},
        168: {"vi": "7 ngày", "en": "7 days"},
        0:   {"vi": "Tùy chọn", "en": "Custom"},
    },
    "Ngày": {
        7:   {"vi": "7 ngày", "en": "7 days"},
        30:  {"vi": "30 ngày", "en": "30 days"},
        90:  {"vi": "3 tháng", "en": "3 months"},
        365: {"vi": "1 năm", "en": "1 year"},
        0:   {"vi": "Tùy chọn", "en": "Custom"},
    },
    "Tháng": {
        30:  {"vi": "1 tháng", "en": "1 month"},
        90:  {"vi": "3 tháng", "en": "3 months"},
        365: {"vi": "1 năm", "en": "1 year"},
        0:   {"vi": "Tùy chọn", "en": "Custom"},
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

GRAIN_DEFAULT_PRESET = {
    "Giờ":  24,
    "Ngày": 30,
    "Tháng": 90,
}

@st.cache_data(ttl=300)
def get_latest_available_date():
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

def render_top_filters(render_tabs_fn=None, key_suffix="", compact=False):
    suffix = f"_{key_suffix}" if key_suffix else ""
    
    # Synchronize widget keys back to the state keys
    for state_key, widget_key in [
        ("f_spatial_grain", f"sb_spatial_grain{suffix}"),
        ("f_scope_val", f"sb_scope_val{suffix}"),
        ("f_pollutant", f"sb_pollutant{suffix}"),
        ("f_time_grain", f"sb_time_grain{suffix}"),
        ("f_time_preset", f"sb_time_preset{suffix}"),
    ]:
        if widget_key in st.session_state:
            st.session_state[state_key] = st.session_state[widget_key]

    init_filter_state()
    lang = st.session_state.lang
    latest_date = get_latest_available_date()

    # 1. Geography Label
    geo_label = "📍 " + ("Geography" if lang == "en" else "Địa lý")
    if st.session_state.f_spatial_grain != "Toàn quốc":
        if st.session_state.f_scope_val:
            geo_label = f"📍 {st.session_state.f_scope_val}"
        else:
            geo_label = f"📍 {st.session_state.f_spatial_grain}"
    else:
        geo_label = "📍 " + ("National" if lang == "en" else "Toàn quốc")

    # 2. Metrics Label
    pollutant_map = {
        "aqi":  "AQI",
        "pm25": "PM2.5",
        "pm10": "PM10",
        "no2":  "NO2",
        "o3":   "O3",
        "so2":  "SO2",
        "co":   "CO",
    }
    selected_pollutant = st.session_state.f_pollutant
    if selected_pollutant in pollutant_map:
        metrics_label = f"🔬 {pollutant_map[selected_pollutant]}"
    else:
        metrics_label = "🔬 " + ("Metrics" if lang == "en" else "Chỉ số")

    # 3. Timeframe Label
    time_label = "⏱ " + ("Timeframe" if lang == "en" else "Thời gian")
    try:
        tg_val = st.session_state.f_time_grain
        tp_val = st.session_state.f_time_preset
        if tg_val in GRAIN_TIME_OPTIONS and tp_val in GRAIN_TIME_OPTIONS[tg_val]:
            time_label = f"⏱ {GRAIN_TIME_OPTIONS[tg_val][tp_val][lang]}"
        elif tp_val == 0:
            time_label = "⏱ " + ("Custom" if lang == "en" else "Tùy chọn")
    except Exception:
        pass

    st.markdown("<div class='top-filter-bar' style='margin-bottom: 1rem;'>", unsafe_allow_html=True)
    if compact:
        c1, c2, c3, c4 = st.columns([1.5, 1.25, 1.2, 1.0], gap="small")
    else:
        c1, c2, c3, c4, c5 = st.columns([1.5, 1.25, 1.2, 1.0, 4.0], gap="small")

    with c1:
        with st.popover(geo_label, use_container_width=True, key=f"popover_geo{suffix}"):
            spatial_options = ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh", "Phường"]
            spatial_map = {
                "Toàn quốc": FILTER_LABELS["spatial_national"][lang],
                "Vùng": FILTER_LABELS["spatial_region"][lang],
                "Khu vực": FILTER_LABELS["spatial_area"][lang],
                "Tỉnh": FILTER_LABELS["spatial_province"][lang],
                "Phường": FILTER_LABELS["spatial_ward"][lang],
            }
            spatial_idx = spatial_options.index(st.session_state.f_spatial_grain) if st.session_state.f_spatial_grain in spatial_options else 0
            
            st.session_state.f_spatial_grain = st.selectbox(
                t("filter_spatial_grain", lang),
                spatial_options,
                format_func=lambda x: spatial_map[x],
                index=spatial_idx,
                key=f"sb_spatial_grain{suffix}"
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
                st.session_state.f_scope_val = st.selectbox(scope_label, scope_options, index=scope_idx, key=f"sb_scope_val{suffix}")
            else:
                st.session_state.f_scope_val = None

    with c2:
        with st.popover(time_label, use_container_width=True, key=f"popover_time{suffix}"):
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
                key=f"sb_time_grain{suffix}"
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
                key=f"sb_time_preset{suffix}"
            )
            st.session_state.f_time_preset = selected_preset

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
                        key=f"sb_custom_dates_hour{suffix}"
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
                        key=f"sb_custom_dates_day{suffix}"
                    )
                time_unit = "day"

    with c3:
        with st.popover(metrics_label, use_container_width=True, key=f"popover_metrics{suffix}"):
            pollutant_map = {
                "aqi":  "AQI",
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
                key=f"sb_pollutant{suffix}"
            )
            st.session_state.f_standard = st.session_state.get("standard", "VN_AQI")

    with c4:
        if st.button("🔄 " + ("Reset" if lang == "en" else "Đặt lại"), use_container_width=True, key=f"reset_btn{suffix}"):
            st.session_state.f_spatial_grain = "Toàn quốc"
            st.session_state.f_scope_val = None
            st.session_state.f_time_grain = "Ngày"
            st.session_state.f_time_preset = 30
            st.session_state.f_date_range = [latest_date - timedelta(days=30), latest_date]
            st.session_state.f_pollutant = "aqi"
            # Clear widget keys from session state to prevent them from overriding the reset defaults
            for k in [
                f"sb_spatial_grain{suffix}", 
                f"sb_scope_val{suffix}", 
                f"sb_pollutant{suffix}", 
                f"sb_time_grain{suffix}", 
                f"sb_time_preset{suffix}",
                f"sb_custom_dates_hour{suffix}",
                f"sb_custom_dates_day{suffix}"
            ]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

    if not compact:
        with c5:
            # Import dynamically inside to prevent circular dependencies
            from lib.data_service import get_current_aqi_status, localize_source_mix, localize_confidence_level
            from lib.aqi_utils import get_aqi_category

            if render_tabs_fn:
                col_tabs, col_alerts = st.columns([2.2, 1.8], gap="small")
                with col_tabs:
                    render_tabs_fn()
            else:
                col_spacer, col_alerts = st.columns([2.3, 1.7], gap="small")

            with col_alerts:
                try:
                    current_df = get_current_aqi_status()
                    if not current_df.empty:
                        alert_provinces = current_df[current_df["current_aqi"] > 150]
                        has_alerts = not alert_provinces.empty

                        if has_alerts:
                            btn_label = f"⚠️ {len(alert_provinces)} tỉnh vượt ngưỡng" if lang == "vi" else f"⚠️ {len(alert_provinces)} provinces exceeding"
                            st.markdown("""
                            <style>
                            div[data-testid="stPopover"] button {
                                border-color: #ef4444 !important;
                                color: #ef4444 !important;
                                background: rgba(239, 68, 68, 0.05) !important;
                            }
                            </style>
                            """, unsafe_allow_html=True)
                        else:
                            btn_label = "✅ An toàn" if lang == "vi" else "✅ Safe"

                        with st.popover(btn_label, use_container_width=True, key=f"popover_alerts{suffix}"):
                            if has_alerts:
                                st.markdown(f"### ⚠️ {t('alert_title', lang)}")
                                alert_lines = ", ".join(f"**{row.province}** ({int(row.current_aqi)})" for _, row in alert_provinces.iterrows())
                                st.warning(f"Các tỉnh/thành vượt ngưỡng AQI 150: {alert_lines}" if lang == "vi" else f"Provinces exceeding AQI 150: {alert_lines}")
                            else:
                                st.success("Không có tỉnh thành nào vượt ngưỡng AQI 150" if lang == "vi" else "No provinces exceeding AQI 150")

                            st.markdown("#### 📊 Bảng AQI hiện tại theo tỉnh" if lang == "vi" else "#### 📊 Current AQI by Province")
                            display_current = current_df.copy()
                            display_current["AQI hiện tại"] = display_current["current_aqi"].apply(lambda x: int(x))
                            display_current["Mức độ"] = display_current["current_aqi"].apply(lambda x: get_aqi_category(x))
                            display_current["Cập nhật lúc"] = display_current["as_of_hour"].apply(lambda x: x.strftime("%H:%M %d/%m") if hasattr(x, "strftime") else str(x))
                            display_current["Nguồn"] = display_current["source_mix"].apply(lambda x: localize_source_mix(x, lang))
                            display_current["Độ tin cậy"] = display_current["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
                            st.dataframe(
                                display_current[["province", "AQI hiện tại", "Mức độ", "main_pollutant", "Nguồn", "Độ tin cậy", "Cập nhật lúc"]].rename(columns={"province": "Tỉnh/thành", "main_pollutant": "Ô nhiễm chính"}),
                                hide_index=True, use_container_width=True,
                            )
                except Exception as e:
                    pass

    st.markdown("</div>", unsafe_allow_html=True)

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
