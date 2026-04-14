import streamlit as st
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
import plotly.express as px

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("status_title", lang))

@st.cache_data(ttl=300)
def get_platform_health():
    q = """
    SELECT
        source,
        last_seen,
        lag_hours,
        health_status,
        is_reliable
    FROM air_quality.dm_platform_data_health
    """
    return query_df(q)

df = get_platform_health()

if not df.empty:
    avg_lag = df.lag_hours.mean()
    reliable_count = df.is_reliable.sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card(t("nav_status", lang) if lang=="en" else "Độ trễ trung bình", f"{avg_lag:.1f}h", icon="status")
    with col2:
        render_metric_card(t("nav_alerts", lang) if lang=="en" else "Nguồn tin cậy", f"{reliable_count}/{len(df)}", icon="insights")
    with col3:
        render_metric_card(t("nav_overview", lang) if lang=="en" else "Trạng thái Hệ thống", "Healthy" if avg_lag < 5 else "Delayed", icon="error")

    st.markdown("---")
    
    # Source Health Table
    st.subheader(t("status_title", lang) if lang=="en" else "Giám sát độ tin cậy nguồn tin")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Lag Chart
    st.subheader(t("chart_top_polluted", lang) if lang=="en" else "Độ trễ theo nguồn dữ liệu")
    fig = px.bar(df, x="source", y="lag_hours", color="health_status",
                color_discrete_map={"Healthy": "#09ab3b", "Warning": "#ffa500", "Critical": "#ff4b4b"})
    fig.update_layout(get_plotly_layout())
    st.plotly_chart(fig, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu độ trễ hệ thống."), use_container_width=True)
