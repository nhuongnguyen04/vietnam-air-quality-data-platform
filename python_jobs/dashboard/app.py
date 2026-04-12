"""Vietnam Air Quality Dashboard — entry point with st.navigation()."""
from __future__ import annotations

import streamlit as st

from pages import (
    page_overview,
    page_pollutants,
    page_comparison,
    page_historical,
    page_alerts,
)

st.set_page_config(
    page_title="Vietnam Air Quality Dashboard",
    page_icon="🌿",
    layout="wide",
)

pg = st.navigation(
    {
        "Dashboards": [page_overview, page_pollutants, page_comparison],
        "Analytics": [page_historical],
        "System": [page_alerts],
    }
)
pg.run()
