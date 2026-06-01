import streamlit as st
from lib.i18n import t
from lib.style import inject_style, render_top_bar

# ── Page Configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="VN Air Quality Analytics",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session State Initialization ──────────────────────────────────────────────
if "theme" not in st.session_state:
    st.session_state.theme = "light"
if "standard" not in st.session_state:
    st.session_state.standard = "VN_AQI"
if "lang" not in st.session_state:
    st.session_state.lang = "vi"

# ── Styling ───────────────────────────────────────────────────────────────────
inject_style()

# ── Sidebar Navigation ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

pages = {
    t("group_main", lang): [
        st.Page("pages/1_Overview.py", title=t("nav_overview", lang), icon=":material/grid_view:"),
        st.Page("pages/2_Pollutants.py", title=t("nav_pollutants", lang), icon=":material/bar_chart:"),
        st.Page("pages/4_Historical_Trend.py", title=t("nav_trends", lang), icon=":material/history:"),
    ],
    t("group_analysis", lang): [
        st.Page("pages/6_Traffic_Impact.py", title=t("nav_traffic", lang), icon=":material/traffic:"),
        st.Page("pages/9_Weather_Impact.py", title=t("nav_weather", lang), icon=":material/cloud:"),
        st.Page("pages/7_Health_Risk.py", title=t("nav_health", lang), icon=":material/monitor_heart:"),
    ],
    t("group_system", lang): [
        st.Page("pages/5_Alerts.py", title=t("nav_alerts", lang), icon=":material/warning:"),
        st.Page("pages/3_Source_Comparison.py", title=t("nav_comparison", lang), icon=":material/database:"),
        st.Page("pages/8_Status.py", title=t("nav_status", lang), icon=":material/show_chart:"),
        st.Page("pages/10_Ask_Data.py", title=t("nav_ask_data", lang), icon=":material/forum:"),
    ]
}

# ── Sidebar Utilities ────────────────────────────────────────────────────────
with st.sidebar:
    pass
    
    # Sidebar footer with version & freshness
    st.markdown(f"""
    <div style='position: fixed; bottom: 15px; left: 15px; width: 230px; font-family:"Inter",sans-serif; font-size:0.75rem; opacity:0.5;'>
        <div style='display:flex; align-items:center; gap:6px;'>
            <span style='height:8px; width:8px; background-color:#10b981; border-radius:50%; display:inline-block;'></span>
            <span>Platform Status: Operational</span>
        </div>
        <div style='margin-top:2px;'>v1.2.0 · Live Data Sync</div>
    </div>
    """, unsafe_allow_html=True)

# ── Run Navigation ────────────────────────────────────────────────────────────
pg = st.navigation(pages)
pg.run()
