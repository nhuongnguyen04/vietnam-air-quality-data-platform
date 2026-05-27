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
    t("nav_overview", lang): [
        st.Page("pages/1_Overview.py", title=t("nav_overview", lang), icon=":material/dashboard:"),
        st.Page("pages/2_Pollutants.py", title=t("nav_pollutants", lang), icon=":material/analytics:"),
        st.Page("pages/4_Historical_Trend.py", title=t("nav_trends", lang), icon=":material/history:"),
    ],
    t("nav_environment", lang): [
        st.Page("pages/6_Traffic_Impact.py", title=t("nav_traffic", lang), icon=":material/traffic:"),
        st.Page("pages/9_Weather_Impact.py", title=t("nav_weather", lang), icon=":material/air:"),
        st.Page("pages/10_Ask_Data.py", title=t("nav_ask_data", lang), icon=":material/psychology:"),
    ],
    t("nav_health", lang): [
        st.Page("pages/7_Health_Risk.py", title=t("nav_health", lang), icon=":material/health_and_safety:"),
        st.Page("pages/5_Alerts.py", title=t("nav_alerts", lang), icon=":material/notification_important:"),
    ],
    t("nav_status", lang): [
        st.Page("pages/3_Source_Comparison.py", title=t("nav_comparison", lang), icon=":material/compare:"),
        st.Page("pages/8_Status.py", title=t("nav_status", lang), icon=":material/settings_suggest:"),
    ]
}

# ── Sidebar Utilities ────────────────────────────────────────────────────────
with st.sidebar:
    # Premium Brand Logo Area
    st.markdown(f"""
    <div class='sidebar-brand-container' style='padding: 0.5rem 0; margin-bottom: 1.25rem;'>
        <h1 style='margin:0; font-family:"Outfit",sans-serif; font-size:1.6rem; font-weight:800;
                   background: linear-gradient(135deg, #0891B2, #06B6D4); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
            🌿 GreenAir VN
        </h1>
        <p style='margin:0.25rem 0 0 0; font-size:0.8rem; opacity:0.6; font-family:"Inter",sans-serif;'>
            Vietnam Air Quality Data Platform
        </p>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    st.markdown("<div class='sidebar-filters-container'>", unsafe_allow_html=True)
    st.subheader(t("standard_guidelines", lang))
    st.select_slider(
        label=t("standard_guidelines", lang),
        options=["VN_AQI", "WHO 2021"],
        key="standard",
        label_visibility="collapsed"
    )
    _std_help = (
        "**VN_AQI**: Chỉ số AQI Việt Nam (QĐ 1459/QĐ-TCMT).<br>"
        "**WHO 2021**: Hướng dẫn chất lượng không khí toàn cầu của WHO."
    ) if lang == "vi" else (
        "**VN_AQI**: Vietnam AQI standard (Decision 1459).<br>"
        "**WHO 2021**: WHO Global Air Quality Guidelines."
    )
    st.markdown(f"<div style='font-size:0.78rem; opacity:0.7; line-height:1.4;'>{_std_help}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.divider()
    
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
