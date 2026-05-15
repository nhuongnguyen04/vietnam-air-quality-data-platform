"""
Trang Rủi ro Sức khỏe (Health Risk) đánh giá tác động của ô nhiễm không khí đối với
sức khỏe con người. Cung cấp các khuyến nghị hành động dựa trên chỉ số AQI để bảo vệ
nhân dân, đặc biệt là các nhóm nhạy cảm.
"""
import plotly.express as px
import streamlit as st
from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import get_hierarchy_metadata
from lib.i18n import t
from lib.style import get_plotly_layout, render_metric_card

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("health_title", lang))

# ── Filters (Hierarchical) ────────────────────────────────────────────────────
hierarchy_df = get_hierarchy_metadata()

st.sidebar.markdown(f"### 📍 {t('nav_health', lang)} Filters")

spatial_grain = st.sidebar.selectbox(
    t("chart_label_type", lang) if lang=="en" else "Cấp độ hiển thị",
    [t("national", lang) if lang=="en" else "Toàn quốc", t("region", lang) if lang=="en" else "Vùng", t("area", lang) if lang=="en" else "Khu vực"],
    index=0
)

scope_val = None
if spatial_grain in ["Vùng", "Region"]:
    scope_val = st.sidebar.selectbox(t("filter_province_select", lang) if lang=="en" else "Chọn miền", sorted(hierarchy_df['region_3'].unique()))
elif spatial_grain in ["Khu vực", "Area"]:
    scope_val = st.sidebar.selectbox(t("filter_province_select", lang) if lang=="en" else "Chọn khu vực", sorted(hierarchy_df['region_8'].unique()))

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
        render_metric_card(t("worst_location", lang), top_risk.province, icon="health")
    with col2:
        mean_val = df[df.total_exposure_index_m > 0].total_exposure_index_m.mean()
        render_metric_card(t("avg_exposure_index", lang), f"{mean_val:.1f}M", icon="location_on")
    with col3:
        high_risk_count = len(df[df.risk_category == 'CRITICAL'])
        render_metric_card(t("critical_hotspots", lang), str(high_risk_count), icon="error")

    st.markdown("---")

    # Risk Distribution Bar Chart
    st.subheader(t("risk_ranking_title", lang))

    # Sort ascending for horizontal bar (longest bar at bottom/end)
    risk_category_labels = {
        "CRITICAL": t("risk_critical", lang),
        "HIGH RISK": t("risk_high", lang),
        "MODERATE": t("risk_moderate", lang),
        "LOW": t("risk_low", lang),
    }
    df_plot = df.sort_values("total_exposure_index_m", ascending=True).copy()
    df_plot["risk_category_label"] = (
        df_plot["risk_category"].map(risk_category_labels).fillna(df_plot["risk_category"])
    )

    fig = px.bar(df_plot, x="total_exposure_index_m", y="province", color="risk_category_label",
                orientation='h',
                labels={
                    "total_exposure_index_m": t("exposure_index", lang),
                    "province": t("province", lang),
                    "risk_category_label": t("risk_level", lang),
                },
                color_discrete_map={
                    t("risk_critical", lang): "#ff4b4b",
                    t("risk_high", lang): "#ffa500",
                    t("risk_moderate", lang): "#09ab3b",
                    t("risk_low", lang): "#3b82f6"
                })

    chart_height = max(400, len(df_plot) * 25)
    fig.update_layout(get_plotly_layout(height=chart_height))
    fig.update_layout(
        yaxis={'categoryorder':'total ascending', 'dtick': 1},
        margin={"l": 150}
    )
    st.plotly_chart(fig, width='stretch')
else:
    st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu rủi ro cho khu vực này."), width='stretch')
