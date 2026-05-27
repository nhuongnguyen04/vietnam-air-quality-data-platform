"""
Health Risk Assessment page.
Analyzes regional fine particle PM2.5 risk index and population exposure.
"""
import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import escape_value, localize_confidence_level, localize_source_mix
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state, RISK_PALETTE

@st.cache_data(ttl=300)
def get_health_risks(spatial_grain, scope_val):
    where = "1=1"
    if spatial_grain in ["Vùng", "Region"] and scope_val:
        where = f"region_3 = '{escape_value(scope_val)}'"
    elif spatial_grain in ["Khu vực", "Area"] and scope_val:
        where = f"region_8 = '{escape_value(scope_val)}'"
    elif spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"] and scope_val:
        where = f"province = '{escape_value(scope_val)}'"

    q = f"""
    SELECT
        province,
        population,
        time_weighted_pm25,
        confidence_score,
        confidence_level,
        source_mix,
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

@page_wrapper("health", "🏥 Health Risk Assessment", icon="🏥")
def main(lang: str):
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_sidebar_filters()
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]

    with st.spinner(t("loading", lang) if lang == "en" else "Đang phân tích rủi ro sức khỏe..."):
        df = get_health_risks(spatial_grain, scope_val)

    if not df.empty:
        top_polluted = df.sort_values("time_weighted_pm25", ascending=False).iloc[0]
        mean_pm25 = df.time_weighted_pm25.mean()
        high_risk_count = len(df[df.risk_category.isin(['CRITICAL', 'HIGH RISK'])])

        # KPI Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            render_metric_card(t("worst_location", lang), top_polluted.province, icon="health")
        with col2:
            render_metric_card(t("health_avg_pm25", lang), f"{mean_pm25:.1f} µg/m³", icon="device_thermostat")
        with col3:
            render_metric_card(t("critical_hotspots", lang), str(high_risk_count), icon="error")

        render_section_divider()

        # Premium addition: Risk Summary Donut Chart alongside ranking
        c_left, c_right = st.columns([1.2, 1.8])
        
        with c_left:
            title_text = "Phân bổ Cấp độ Rủi ro" if lang == "vi" else "Risk Category Distribution"
            st.markdown(f"#### 📊 {title_text}")
            risk_summary = df["risk_category"].value_counts().reset_index()
            risk_summary.columns = ["risk_category", "count"]
            risk_category_labels = {
                "CRITICAL": t("risk_critical", lang),
                "HIGH RISK": t("risk_high", lang),
                "MODERATE": t("risk_moderate", lang),
                "LOW": t("risk_low", lang),
            }
            risk_summary["risk_label"] = risk_summary["risk_category"].map(risk_category_labels)
            
            fig_donut = px.pie(
                risk_summary,
                values="count",
                names="risk_label",
                hole=0.45,
                color="risk_label",
                color_discrete_map={
                    t("risk_critical", lang): RISK_PALETTE["risk_critical"],
                    t("risk_high", lang): RISK_PALETTE["risk_high"],
                    t("risk_moderate", lang): RISK_PALETTE["risk_moderate"],
                    t("risk_low", lang): RISK_PALETTE["risk_low"],
                }
            )
            fig_donut.update_layout(get_plotly_layout(height=350, compact=True))
            st.plotly_chart(fig_donut, use_container_width=True)

        with c_right:
            st.markdown(f"#### 🏭 {t('health_pollution_ranking', lang)}")
            df_pm25 = df.sort_values("time_weighted_pm25", ascending=True).copy()
            df_pm25["risk_label"] = df_pm25["risk_category"].map(risk_category_labels).fillna(df_pm25["risk_category"])
            df_pm25["confidence_label"] = df_pm25["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
            df_pm25["source_label"] = df_pm25["source_mix"].apply(lambda x: localize_source_mix(x, lang))

            fig1 = px.bar(
                df_pm25, x="time_weighted_pm25", y="province", color="risk_label",
                orientation='h',
                labels={
                    "time_weighted_pm25": "PM2.5 (µg/m³, 30d avg)",
                    "province": t("province", lang),
                    "risk_label": t("risk_level", lang),
                },
                hover_data={"confidence_label": True, "source_label": True, "confidence_score": ":.2f"},
                color_discrete_map={
                    t("risk_critical", lang): RISK_PALETTE["risk_critical"],
                    t("risk_high", lang): RISK_PALETTE["risk_high"],
                    t("risk_moderate", lang): RISK_PALETTE["risk_moderate"],
                    t("risk_low", lang): RISK_PALETTE["risk_low"],
                }
            )
            fig1.add_vline(x=15, line_dash="dash", line_color="#F59E0B", annotation_text="WHO (15 µg)", annotation_position="top right")
            fig1.add_vline(x=25, line_dash="dash", line_color="#EF4444", annotation_text="TCVN (25 µg)", annotation_position="top right")

            chart_height = max(350, len(df_pm25) * 22)
            fig1.update_layout(get_plotly_layout(height=chart_height, compact=True))
            st.plotly_chart(fig1, use_container_width=True)

        st.caption(
            "💡 Xếp hạng dựa trên nồng độ PM2.5 bình quân trọng số thời gian 30 ngày. "
            "Hướng dẫn WHO 2021 (15 µg/m³), Quy chuẩn Việt Nam QCVN 05:2023 (25 µg/m³ hàng năm)."
            if lang == "vi" else
            "💡 Ranked by 30-day time-weighted PM2.5 concentrations. "
            "WHO 2021 guidelines (15 µg/m³) vs Vietnam QCVN 05:2023 guidelines (25 µg/m³ annual)."
        )

        render_section_divider()

        # ── Population Exposure Ranking ──────────────────────────────
        st.markdown(f"#### 👥 {t('health_exposure_ranking', lang)}")
        df_exp = df.sort_values("total_exposure_index_m", ascending=True).copy()
        df_exp["exp_risk_label"] = df_exp["exposure_risk_category"].map(risk_category_labels).fillna(df_exp["exposure_risk_category"])
        df_exp["confidence_label"] = df_exp["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
        df_exp["source_label"] = df_exp["source_mix"].apply(lambda x: localize_source_mix(x, lang))

        fig2 = px.bar(
            df_exp, x="total_exposure_index_m", y="province", color="exp_risk_label",
            orientation='h',
            labels={
                "total_exposure_index_m": t("exposure_index", lang),
                "province": t("province", lang),
                "exp_risk_label": t("risk_level", lang),
            },
            hover_data={"confidence_label": True, "source_label": True, "confidence_score": ":.2f"},
            color_discrete_map={
                t("risk_critical", lang): RISK_PALETTE["risk_critical"],
                t("risk_high", lang): RISK_PALETTE["risk_high"],
                t("risk_moderate", lang): RISK_PALETTE["risk_moderate"],
                t("risk_low", lang): RISK_PALETTE["risk_low"],
            }
        )
        fig2.update_layout(get_plotly_layout(height=chart_height, compact=True))
        st.plotly_chart(fig2, use_container_width=True)

        st.caption(
            "💡 Chỉ số phơi nhiễm dân số tính bằng PM2.5 × Dân số (triệu người). "
            "Giúp ưu tiên xử lý các vùng có mật độ dân số tập trung đông đảo."
            if lang == "vi" else
            "💡 Population exposure index calculated as PM2.5 × Population (Millions). "
            "Enables strategic mitigation prioritization based on total exposed population size."
        )

    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu rủi ro sức khỏe cho vùng này."), use_container_width=True)

if __name__ == "__main__":
    main()
