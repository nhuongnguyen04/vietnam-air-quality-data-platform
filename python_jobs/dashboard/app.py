import streamlit as st
from lib.style import inject_style, render_top_bar
from lib.i18n import t

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
    st.session_state.standard = "TCVN"
if "lang" not in st.session_state:
    st.session_state.lang = "vi"

# ── Styling ───────────────────────────────────────────────────────────────────
inject_style()
render_top_bar()

# ── Sidebar Navigation ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

pages = {
    t("nav_overview", lang): [
        st.Page("pages/1_Overview.py", title=t("nav_overview", lang), icon=":material/dashboard:"),
        st.Page("pages/2_Pollutants.py", title=t("nav_pollutants", lang), icon=":material/analytics:"),
    ],
    t("nav_environment", lang): [
        st.Page("pages/6_Traffic_Impact.py", title=t("nav_traffic", lang), icon=":material/traffic:"),
        st.Page("pages/9_Weather_Impact.py", title=t("nav_weather", lang), icon=":material/air:"),
    ],
    t("nav_health", lang): [
        st.Page("pages/7_Health_Risk.py", title=t("nav_health", lang), icon=":material/health_and_safety:"),
    ],
    t("nav_status", lang): [
        st.Page("pages/8_Status.py", title=t("nav_status", lang), icon=":material/settings_suggest:"),
    ]
}

# ── Sidebar Utilities ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"# VN Air Quality")
    st.divider()
    
    st.subheader(t("standard_guidelines", lang))
    st.select_slider(
        label=t("standard_guidelines", lang),
        options=["TCVN", "WHO 2021"],
        key="standard",
        label_visibility="collapsed"
    )
    st.divider()

# ── Run Navigation ────────────────────────────────────────────────────────────
pg = st.navigation(pages)
pg.run()
