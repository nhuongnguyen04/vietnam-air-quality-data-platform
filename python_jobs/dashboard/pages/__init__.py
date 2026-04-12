"""Page definitions for st.navigation()."""
from __future__ import annotations

import streamlit as st

page_overview = st.Page(
    "pages/1_Overview.py",
    title="Tổng quan",
    icon="📊",
    default=True,
)
page_pollutants = st.Page(
    "pages/2_Pollutants.py",
    title="Chất ô nhiễm",
    icon="🧪",
)
page_comparison = st.Page(
    "pages/3_Source_Comparison.py",
    title="So sánh nguồn",
    icon="🔗",
)
page_historical = st.Page(
    "pages/4_Historical_Trend.py",
    title="Xu hướng lịch sử",
    icon="📈",
)
page_alerts = st.Page(
    "pages/5_Alerts.py",
    title="Cảnh báo",
    icon="🚨",
)

__all__ = [
    "page_overview",
    "page_pollutants",
    "page_comparison",
    "page_historical",
    "page_alerts",
]
