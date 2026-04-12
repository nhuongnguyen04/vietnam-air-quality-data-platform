"""Page definitions for st.navigation()."""
from __future__ import annotations

import streamlit as st

page_overview = st.Page(
    "pages/1_Overview.py",
    title="Tổng quan",
    icon=":material/dashboard:",
    default=True,
)
page_pollutants = st.Page(
    "pages/2_Pollutants.py",
    title="Chất ô nhiễm",
    icon=":material/science:",
)
page_traffic = st.Page(
    "pages/6_Traffic_Impact.py",
    title="Ảnh hưởng Giao thông",
    icon=":material/traffic:",
)
page_health = st.Page(
    "pages/7_Health_Risk.py",
    title="Rủi ro Sức khỏe",
    icon=":material/health_and_safety:",
)
page_comparison = st.Page(
    "pages/3_Source_Comparison.py",
    title="So sánh nguồn",
    icon=":material/compare_arrows:",
)
page_historical = st.Page(
    "pages/4_Historical_Trend.py",
    title="Xu hướng lịch sử",
    icon=":material/history:",
)
page_status = st.Page(
    "pages/8_Status.py",
    title="Trạng thái Hệ thống",
    icon=":material/monitor_heart:",
)
page_alerts = st.Page(
    "pages/5_Alerts.py",
    title="Cảnh báo",
    icon=":material/notifications_active:",
)

__all__ = [
    "page_overview",
    "page_pollutants",
    "page_traffic",
    "page_health",
    "page_comparison",
    "page_historical",
    "page_status",
    "page_alerts",
]

