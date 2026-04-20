import streamlit as st
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
import plotly.express as px

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

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
    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card(t("avg_lag", lang), f"{avg_lag:.1f}h", icon="status")
    with col2:
        render_metric_card(t("reliable_sources", lang), f"{reliable_count}/{len(df)}", icon="insights")
    with col3:
        render_metric_card(t("system_status", lang), "Healthy" if avg_lag < 5 else "Delayed", icon="error")

    st.markdown("---")
    
    # Source Health Table
    st.subheader(t("source_reliability_monitoring", lang))
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Lag Chart
    st.subheader(t("chart_data_freshness_by_source", lang))
    fig = px.bar(df, x="source", y="lag_hours", color="health_status",
                color_discrete_map={"Healthy": "#09ab3b", "Warning": "#ffa500", "Critical": "#ff4b4b"})
    fig.update_layout(get_plotly_layout())
    st.plotly_chart(fig, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu độ trễ hệ thống."), use_container_width=True)
