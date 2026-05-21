"""
Trang Rủi ro Sức khỏe (Health Risk) đánh giá tác động của ô nhiễm không khí đối với
sức khỏe con người. Hiển thị xếp hạng kép: theo mức ô nhiễm PM2.5 thuần túy và theo
quy mô phơi nhiễm dân số. Sử dụng ngưỡng WHO 2021 và QCVN 05:2023.
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
        exposure_risk_category,
        pollution_rank,
        exposure_rank,
        national_risk_rank
    FROM air_quality.dm_regional_health_risk_ranking
    WHERE {where}
    ORDER BY national_risk_rank ASC
    """
    return query_df(q)

df = get_health_risks(spatial_grain, scope_val)

if not df.empty:
    top_polluted = df.sort_values("time_weighted_pm25", ascending=False).iloc[0]
    top_exposed = df.sort_values("total_exposure_index_m", ascending=False).iloc[0]

    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card(
            t("worst_location", lang),
            top_polluted.province,
            icon="health"
        )
    with col2:
        mean_pm25 = df.time_weighted_pm25.mean()
        render_metric_card(
            t("health_avg_pm25", lang) if lang == "vi" else "Avg PM2.5 (30d)",
            f"{mean_pm25:.1f} µg/m³",
            icon="device_thermostat"
        )
    with col3:
        high_risk_count = len(df[df.risk_category.isin(['CRITICAL', 'HIGH RISK'])])
        render_metric_card(t("critical_hotspots", lang), str(high_risk_count), icon="error")

    st.markdown("---")

    # ── Chart 1: Pollution Ranking (PM2.5) ────────────────────────────────
    st.subheader(t("health_pollution_ranking", lang) if lang == "vi" else "🏭 Air Pollution Ranking (PM2.5)")

    risk_category_labels = {
        "CRITICAL": t("risk_critical", lang),
        "HIGH RISK": t("risk_high", lang),
        "MODERATE": t("risk_moderate", lang),
        "LOW": t("risk_low", lang),
    }
    df_pm25 = df.sort_values("time_weighted_pm25", ascending=True).copy()
    df_pm25["risk_label"] = (
        df_pm25["risk_category"].map(risk_category_labels).fillna(df_pm25["risk_category"])
    )

    fig1 = px.bar(df_pm25, x="time_weighted_pm25", y="province", color="risk_label",
                orientation='h',
                labels={
                    "time_weighted_pm25": "PM2.5 (µg/m³, 30d avg)",
                    "province": t("province", lang),
                    "risk_label": t("risk_level", lang),
                },
                color_discrete_map={
                    t("risk_critical", lang): "#ff4b4b",
                    t("risk_high", lang): "#ffa500",
                    t("risk_moderate", lang): "#09ab3b",
                    t("risk_low", lang): "#3b82f6"
                })

    # Add WHO and QCVN reference lines
    fig1.add_vline(x=15, line_dash="dash", line_color="#f59e0b",
                   annotation_text="WHO 2021 (15µg/m³)", annotation_position="top right")
    fig1.add_vline(x=25, line_dash="dash", line_color="#ef4444",
                   annotation_text="QCVN (25µg/m³ annual)", annotation_position="top right")

    chart_height = max(400, len(df_pm25) * 25)
    fig1.update_layout(get_plotly_layout(height=chart_height))
    fig1.update_layout(
        yaxis={'categoryorder':'total ascending', 'dtick': 1},
        margin={"l": 150}
    )
    st.plotly_chart(fig1, width='stretch')

    _caption_pm25 = (
        "Xếp hạng theo **nồng độ PM2.5 trung bình 30 ngày**. "
        "Ngưỡng: WHO 2021 (15 µg/m³), QCVN 05:2023 (25 µg/m³/năm)."
    ) if lang == "vi" else (
        "Ranked by **30-day average PM2.5 concentration**. "
        "Thresholds: WHO 2021 (15 µg/m³), QCVN 05:2023 (25 µg/m³ annual)."
    )
    st.caption(_caption_pm25)

    st.markdown("---")

    # ── Chart 2: Population Exposure Ranking ──────────────────────────────
    st.subheader(t("health_exposure_ranking", lang) if lang == "vi" else "👥 Population Exposure Ranking")

    df_exp = df.sort_values("total_exposure_index_m", ascending=True).copy()
    df_exp["exp_risk_label"] = (
        df_exp["exposure_risk_category"].map(risk_category_labels).fillna(df_exp["exposure_risk_category"])
    )

    fig2 = px.bar(df_exp, x="total_exposure_index_m", y="province", color="exp_risk_label",
                orientation='h',
                labels={
                    "total_exposure_index_m": t("exposure_index", lang),
                    "province": t("province", lang),
                    "exp_risk_label": t("risk_level", lang),
                },
                color_discrete_map={
                    t("risk_critical", lang): "#ff4b4b",
                    t("risk_high", lang): "#ffa500",
                    t("risk_moderate", lang): "#09ab3b",
                    t("risk_low", lang): "#3b82f6"
                })

    fig2.update_layout(get_plotly_layout(height=chart_height))
    fig2.update_layout(
        yaxis={'categoryorder':'total ascending', 'dtick': 1},
        margin={"l": 150}
    )
    st.plotly_chart(fig2, width='stretch')

    _caption_exp = (
        "Xếp hạng theo **PM2.5 × Dân số** (chỉ số phơi nhiễm). "
        "Tỉnh đông dân có chỉ số cao hơn ngay cả khi PM2.5 thấp. "
        "Dân số: NQ 202/2025 (34 tỉnh)."
    ) if lang == "vi" else (
        "Ranked by **PM2.5 × Population** (exposure index). "
        "Populous provinces rank higher even with lower PM2.5. "
        "Population: post-NQ 202/2025 (34 provinces)."
    )
    st.caption(_caption_exp)
else:
    st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu rủi ro cho khu vực này."), width='stretch')

