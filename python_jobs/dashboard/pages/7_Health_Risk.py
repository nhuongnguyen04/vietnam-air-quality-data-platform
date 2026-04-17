import streamlit as st
from lib.clickhouse_client import query_df
from lib.data_service import get_hierarchy_metadata
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
import plotly.express as px

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("health_title", lang))

# ── Filters (Hierarchical) ────────────────────────────────────────────────────
hierarchy_df = get_hierarchy_metadata()

st.sidebar.markdown(f"### 📍 {t('nav_health', lang)} Filters")

spatial_grain = st.sidebar.selectbox(
    "Cấp độ hiển thị" if lang == "vi" else "Spatial Grain",
    ["Toàn quốc", "Vùng", "Khu vực"],
    index=0
)

scope_val = None
if spatial_grain == "Vùng":
    scope_val = st.sidebar.selectbox("Chọn miền", sorted(hierarchy_df['region_3'].unique()))
elif spatial_grain == "Khu vực":
    scope_val = st.sidebar.selectbox("Chọn khu vực", sorted(hierarchy_df['region_8'].unique()))

@st.cache_data(ttl=300)
def get_health_risks(spatial_grain, scope_val):
    where = "1=1"
    if spatial_grain == "Vùng" and scope_val:
        where = f"region_3 = '{scope_val}'"
    elif spatial_grain == "Khu vực" and scope_val:
        where = f"region_8 = '{scope_val}'"
        
    q = f"""
    SELECT
        province,
        population,
        time_weighted_pm25,
        total_exposure_index_m,
        risk_category,
        national_risk_rank
    FROM air_quality.dm_regional_health_risk_ranking
    WHERE {where}
    ORDER BY total_exposure_index_m DESC
    """
    return query_df(q)

df = get_health_risks(spatial_grain, scope_val)

if not df.empty:
    top_risk = df.iloc[0]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card("Khu vực rủi ro nhất" if lang=="vi" else "Worst Location", top_risk.province, icon="health")
    with col2:
        mean_val = df[df.total_exposure_index_m > 0].total_exposure_index_m.mean()
        render_metric_card("Chỉ số Phơi nhiễm TB" if lang=="vi" else "Avg Exposure Index", f"{mean_val:.1f}M", icon="location_on")
    with col3:
        high_risk_count = len(df[df.risk_category == 'CRITICAL'])
        render_metric_card("Điểm nóng nguy kịch" if lang=="vi" else "Critical Hotspots", str(high_risk_count), icon="error")

    st.markdown("---")
    
    # Risk Distribution Bar Chart
    st.subheader("Xếp hạng Rủi ro (theo Chỉ số Phơi nhiễm)" if lang=="vi" else "Risk Ranking (by Exposure Index)")
    
    # Sort ascending for horizontal bar (longest bar at bottom/end)
    df_plot = df.sort_values("total_exposure_index_m", ascending=True)
    
    fig = px.bar(df_plot, x="total_exposure_index_m", y="province", color="risk_category",
                orientation='h',
                color_discrete_map={
                    "CRITICAL": "#ff4b4b",
                    "HIGH RISK": "#ffa500",
                    "MODERATE": "#09ab3b",
                    "LOW": "#3b82f6"
                })
    
    chart_height = max(400, len(df_plot) * 25)
    fig.update_layout(get_plotly_layout(height=chart_height))
    fig.update_layout(
        yaxis={'categoryorder':'total ascending', 'dtick': 1},
        margin=dict(l=150)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu rủi ro cho khu vực này."), use_container_width=True)
