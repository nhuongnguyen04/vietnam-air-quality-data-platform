import streamlit as st
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t
import plotly.express as px

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("health_title", lang))

@st.cache_data(ttl=300)
def get_health_risks():
    q = """
    SELECT
        province,
        population,
        time_weighted_pm25,
        total_exposure_index_m,
        risk_category,
        national_risk_rank
    FROM air_quality.dm_regional_health_risk_ranking
    ORDER BY national_risk_rank ASC
    """
    return query_df(q)

df = get_health_risks()

if not df.empty:
    top_risk = df.iloc[0]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card(t("nav_health", lang) if lang=="en" else "Tỉnh rủi ro cao nhất", top_risk.province, icon="health")
    with col2:
        render_metric_card(t("metric_active", lang) if lang=="en" else "Chỉ số Phơi nhiễm TB", f"{df.total_exposure_index_m.mean():.1f}M", icon="location_on")
    with col3:
        render_metric_card(t("metric_worst", lang) if lang=="en" else "Khu vực Nguy cơ cao", str(len(df[df.risk_category == 'High'])), icon="error")

    st.markdown("---")
    
    # Risk Distribution Bar Chart
    st.subheader(t("chart_top_polluted", lang) if lang=="en" else "Xếp hạng Rủi ro Quốc gia (theo Chỉ số Phơi nhiễm)")
    fig = px.bar(df, x="total_exposure_index_m", y="province", color="risk_category",
                orientation='h',
                color_discrete_map={"High": "#ff4b4b", "Moderate": "#ffa500", "Low": "#09ab3b"})
    fig.update_layout(get_plotly_layout(height=600))
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No Health Risk data found in ClickHouse.")
