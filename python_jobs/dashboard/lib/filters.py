import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from .data_service import get_hierarchy_metadata
from .i18n import t

def init_filter_state():
    """Initialize session state for filters if not already present."""
    defaults = {
        "f_spatial_grain": "Toàn quốc",
        "f_scope_val": None,
        "f_time_preset": 30, # 30 days
        "f_date_range": [datetime.now() - timedelta(days=30), datetime.now()],
        "f_pollutant": "aqi",
        "f_standard": st.session_state.get("standard", "TCVN"),
        "lang": st.session_state.get("lang", "vi")
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
    tg_idx = time_grain_options.index(st.session_state.get("f_time_grain", "Ngày")) if st.session_state.get("f_time_grain") in time_grain_options else 1
    st.session_state.f_time_grain = st.sidebar.selectbox(
        "Độ phân giải thời gian",
        time_grain_options,
        index=tg_idx
    )

    # 1. Spatial Grain
    spatial_options = ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh", "Phường"]
    spatial_idx = spatial_options.index(st.session_state.f_spatial_grain) if st.session_state.f_spatial_grain in spatial_options else 0
    
    st.session_state.f_spatial_grain = st.sidebar.selectbox(
        t("filter_spatial_grain", lang),
        spatial_options,
        index=spatial_idx
    )

    # 2. Scope (Province/Region selection)
    hierarchy_df = get_hierarchy_metadata()
    scope_disabled = st.session_state.f_spatial_grain == "Toàn quốc"
    
    scope_choices = {
        "Vùng":    ("Chọn miền", sorted(hierarchy_df['region_3'].unique())),
        "Khu vực": ("Chọn khu vực", sorted(hierarchy_df['region_8'].unique())),
        "Tỉnh":    ("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique())),
        "Phường":  ("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique())),
    }.get(st.session_state.f_spatial_grain, ("Chọn", []))

    scope_label, scope_options = scope_choices
    
    # Handle current value validity
    current_scope = st.session_state.f_scope_val
    if current_scope not in scope_options:
        scope_idx = 0
    else:
        scope_idx = scope_options.index(current_scope)

    if not scope_disabled:
        st.session_state.f_scope_val = st.sidebar.selectbox(
            scope_label,
            scope_options,
            index=scope_idx
        )
    else:
        st.session_state.f_scope_val = None

    st.sidebar.markdown("---")

    # 3. Pollutant Filter
    pollutant_map = {
        "aqi": "AQI Tổng hợp",
        "pm25": "PM2.5",
        "pm10": "PM10",
        "no2": "NO2",
        "o3": "O3",
        "so2": "SO2",
        "co": "CO"
    }
    poll_keys = list(pollutant_map.keys())
    poll_idx = poll_keys.index(st.session_state.f_pollutant) if st.session_state.f_pollutant in poll_keys else 0
    
    st.session_state.f_pollutant = st.sidebar.selectbox(
        "Chất ô nhiễm quan tâm" if lang == "vi" else "Target Pollutant",
        poll_keys,
        format_func=lambda x: pollutant_map[x],
        index=poll_idx
    )

    # 4. Standard is handled globally in app.py
    # We just read it from session state here
    st.session_state.f_standard = st.session_state.get("standard", "TCVN")

    st.sidebar.markdown("---")

    # 5. Time Filter (Quick Select + Custom Range)
    TIME_OPTIONS = {
        7: "7 ngày gần nhất",
        30: "30 ngày gần nhất",
        90: "3 tháng gần nhất",
        365: "1 năm gần nhất",
        0: "Tùy chọn khoảng ngày"
    }
    
    preset_keys = list(TIME_OPTIONS.keys())
    preset_idx = preset_keys.index(st.session_state.f_time_preset) if st.session_state.f_time_preset in preset_keys else 1
    
    selected_preset = st.sidebar.selectbox(
        "Khoảng thời gian",
        preset_keys,
        format_func=lambda x: TIME_OPTIONS[x],
        index=preset_idx
    )
    
    if selected_preset != 0:
        # Update date range based on preset
        end_date = datetime.now()
        start_date = end_date - timedelta(days=selected_preset)
        st.session_state.f_date_range = [start_date, end_date]
        st.session_state.f_time_preset = selected_preset
    else:
        # Custom Range
        st.session_state.f_time_preset = 0
        st.session_state.f_date_range = st.sidebar.date_input(
            "Chọn khoảng ngày",
            value=st.session_state.f_date_range,
            max_value=datetime.now()
        )

    return {
        "spatial_grain": st.session_state.f_spatial_grain,
        "time_grain": st.session_state.f_time_grain,
        "scope_val": st.session_state.f_scope_val,
        "pollutant": st.session_state.f_pollutant,
        "standard": st.session_state.f_standard,
        "date_range": st.session_state.f_date_range,
        "time_preset": st.session_state.f_time_preset
    }
