import streamlit as st
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
import plotly.express as px

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

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
        # Use simple mean now that data is fixed, or filter out truly missing sensors (index > 0)
        mean_val = df[df.total_exposure_index_m > 0].total_exposure_index_m.mean()
        render_metric_card(t("metric_active", lang) if lang=="en" else "Chỉ số Phơi nhiễm TB", f"{mean_val:.1f}M", icon="location_on")
    with col3:
        render_metric_card(t("metric_worst", lang) if lang=="en" else "Khu vực Nguy cơ cao", str(len(df[df.risk_category == 'CRITICAL'])), icon="error")

    st.markdown("---")
    
    # Risk Distribution Bar Chart
    st.subheader(t("chart_top_polluted", lang) if lang=="en" else "Xếp hạng Rủi ro Quốc gia (theo Chỉ số Phơi nhiễm)")
    
    # Sort descending for better UX (highest impact at top)
    df_plot = df.sort_values("total_exposure_index_m", ascending=True)
    
    fig = px.bar(df_plot, x="total_exposure_index_m", y="province", color="risk_category",
                orientation='h',
                color_discrete_map={
                    "CRITICAL": "#ff4b4b",  # Red
                    "HIGH RISK": "#ffa500", # Orange
                    "MODERATE": "#09ab3b",  # Green
                    "LOW": "#3b82f6"        # Blue
                })
    
    # Increased height to fit all 63 provinces and fixed labels
    fig.update_layout(get_plotly_layout(height=1200))
    fig.update_layout(
        yaxis={
            'categoryorder':'total ascending',
            'dtick': 1  # Force every tick to show
        },
        margin=dict(l=150) # Extra space for long province names
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu rủi ro sức khỏe."), use_container_width=True)
