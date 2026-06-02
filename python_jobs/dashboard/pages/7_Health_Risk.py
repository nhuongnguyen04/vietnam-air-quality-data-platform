"""
Health Risk Assessment page.
Analyzes regional fine particle PM2.5 risk index and population exposure.
"""
import pandas as pd
import plotly.express as px
import streamlit as st

from lib.clickhouse_client import query_df
from lib.data_service import escape_value, localize_confidence_level, localize_source_mix
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import render_section_divider, page_wrapper
from lib.chart_config import get_plotly_layout, create_empty_state, RISK_PALETTE
from lib.ui_components import render_kpi_card

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

@page_wrapper("health_risk", "Rủi ro sức khỏe & phơi nhiễm dân số", icon="🏥", skip_hero=True)
def main(lang):
    # ── Initialize Layout & Styling ────────────────────────────────────────────────

    # ── Top Filters ────────────────────────────────────────────────────────────
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]

    # ── Get active state configurations ────────────────────────────────────────────
    lang = st.session_state.get("lang", "vi")
    theme = st.session_state.get("theme", "light")

    # ── Title and Subtitle ─────────────────────────────────────────────────────────
    title_text = "Rủi ro sức khỏe & phơi nhiễm dân số" if lang == "vi" else "Health Risk & Population Exposure"
    subtitle_text = "PM2.5 trọng số thời gian 30 ngày · QCVN 05:2023 · WHO 2021" if lang == "vi" else "30-day time-weighted PM2.5 · QCVN 05:2023 · WHO 2021"
    
    st.markdown("<div style='margin-top: 0.75rem;'></div>", unsafe_allow_html=True)
    st.markdown(f"<h2 style='margin:0; padding:0; font-family:\"Outfit\",sans-serif; font-size:1.65rem; font-weight:700; opacity: 0.95;'>{title_text}</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='margin:0.25rem 0 0 0; font-size:0.8rem; opacity:0.6; font-weight: 500;'>{subtitle_text}</p>", unsafe_allow_html=True)

    with st.spinner(t("loading", lang) if lang == "en" else "Đang phân tích rủi ro sức khỏe..."):
        df = get_health_risks(spatial_grain, scope_val)

    if not df.empty:
        top_polluted = df.sort_values("time_weighted_pm25", ascending=False).iloc[0]
        mean_pm25 = df.time_weighted_pm25.mean()
        high_risk_count = len(df[df.risk_category.isin(['CRITICAL', 'HIGH RISK'])])
        total_pop = df.population.sum() if "population" in df.columns else 0

        # Col 1: Worst location
        worst_loc_label = "Ô nhiễm nhất" if lang == "vi" else "Most Polluted"
        worst_loc_val = top_polluted.province
        worst_loc_sub = f"{top_polluted.time_weighted_pm25:.1f} µg/m³ PM2.5"
        
        # Col 2: Avg PM2.5
        if lang == "vi":
            if spatial_grain in ["Vùng", "Region"]:
                avg_label = "PM2.5 TB vùng"
            elif spatial_grain in ["Khu vực", "Area"]:
                avg_label = "PM2.5 TB khu vực"
            elif spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"]:
                avg_label = "PM2.5 TB tỉnh/thành"
            else:
                avg_label = "PM2.5 TB quốc gia"
        else:
            if spatial_grain in ["Vùng", "Region"]:
                avg_label = "Region Avg PM2.5"
            elif spatial_grain in ["Khu vực", "Area"]:
                avg_label = "Area Avg PM2.5"
            elif spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"]:
                avg_label = "Province Avg PM2.5"
            else:
                avg_label = "National Avg PM2.5"
                
        avg_val = f"{mean_pm25:.1f} <span style='font-size: 1.1rem; font-weight: 500;'>µg/m³</span>"
        if mean_pm25 > 15:
            avg_sub = f"vượt WHO {mean_pm25 / 15:.1f}×" if lang == "vi" else f"exceeds WHO {mean_pm25 / 15:.1f}×"
            avg_color = "#B45309" if theme == "light" else "#f59e0b" # Deep amber in light mode
        else:
            avg_sub = "đạt chuẩn WHO" if lang == "vi" else "meets WHO standard"
            avg_color = "#065F46" if theme == "light" else "#10b981" # Forest green in light mode
            
        # Col 3: Critical Hotspots
        if lang == "vi":
            crit_label = "Điểm nguy kịch" if spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"] else "Tỉnh nguy kịch"
            crit_sub = "rủi ro Critical+High"
        else:
            crit_label = "Critical Hotspots" if spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"] else "Critical Provinces"
            crit_sub = "Critical+High risk"
        crit_val = str(high_risk_count)
        crit_color = "#dc2626" if theme == "light" else "#ef4444" # Deep red in light mode
        if high_risk_count == 0:
            crit_color = "#065F46" if theme == "light" else "#10b981"
        
        # Col 4: Exposed Population
        pop_label = "Dân số phơi nhiễm" if lang == "vi" else "Exposed Population"
        pop_val = f"{total_pop / 1_000_000:.1f}M" if total_pop > 1_000_000 else f"{total_pop:,.0f}"
        pop_sub = "môi trường không đạt WHO" if lang == "vi" else "non-compliant WHO env"
        if mean_pm25 <= 15:
            pop_sub = "môi trường đạt WHO" if lang == "vi" else "compliant WHO env"

        # Render the KPI metrics row using columns and the custom cards
        kpi_cols = st.columns(4)
        
        with kpi_cols[0]:
            render_kpi_card(
                title=worst_loc_label,
                value=worst_loc_val,
                subtext=worst_loc_sub,
                val_color="#dc2626" if theme == "light" else "#ef4444"
            )
            
        with kpi_cols[1]:
            render_kpi_card(
                title=avg_label,
                value=avg_val,
                subtext=avg_sub,
                val_color=avg_color
            )
            
        with kpi_cols[2]:
            render_kpi_card(
                title=crit_label,
                value=crit_val,
                subtext=crit_sub,
                val_color=crit_color
            )
            
        with kpi_cols[3]:
            # Highlight population subtext in red if non-compliant with WHO
            pop_sub_color = ("#dc2626" if theme == "light" else "#f87171") if mean_pm25 > 15 else ("#065F46" if theme == "light" else "#34d399")
            render_kpi_card(
                title=pop_label,
                value=pop_val,
                subtext=pop_sub,
                val_color="#0f172a" if theme == "light" else "#f8fafc",
                sub_color=pop_sub_color
            )

        # Premium addition: Risk Summary Donut Chart alongside ranking
        risk_category_labels = {
            "CRITICAL": t("risk_critical", lang),
            "HIGH RISK": t("risk_high", lang),
            "MODERATE": t("risk_moderate", lang),
            "LOW": t("risk_low", lang),
        }
        
        df_pm25 = df.sort_values("time_weighted_pm25", ascending=False).copy()
        df_pm25["risk_label"] = df_pm25["risk_category"].map(risk_category_labels).fillna(df_pm25["risk_category"])
        df_pm25["confidence_label"] = df_pm25["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
        df_pm25["source_label"] = df_pm25["source_mix"].apply(lambda x: localize_source_mix(x, lang))
        
        chart_height = 450

        c_left, c_right = st.columns([1.2, 1.8])
        
        with c_left:
            title_text = "Phân bổ Cấp độ Rủi ro" if lang == "vi" else "Risk Category Distribution"
            st.markdown(f"#### 📊 {title_text}")
            risk_summary = df["risk_category"].value_counts().reset_index()
            risk_summary.columns = ["risk_category", "count"]
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
            fig_donut.update_layout(get_plotly_layout(height=chart_height, compact=True))
            st.plotly_chart(fig_donut, use_container_width=True)

        with c_right:
            st.markdown(f"#### 🏭 {t('health_pollution_ranking', lang)}")
            fig1 = px.bar(
                df_pm25, x="province", y="time_weighted_pm25", color="risk_label",
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
            fig1.add_hline(y=15, line_dash="dash", line_color="#F59E0B", annotation_text="WHO (15 µg)", annotation_position="top right")
            fig1.add_hline(y=25, line_dash="dash", line_color="#EF4444", annotation_text="TCVN (25 µg)", annotation_position="top right")

            layout1 = get_plotly_layout(height=chart_height, compact=True)
            layout1["margin"]["r"] = 80  # Expand right margin to prevent TCVN / WHO labels clipping
            fig1.update_layout(layout1)
            fig1.update_xaxes(type='category', dtick=1, tickangle=-45)
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
        df_exp = df.sort_values("total_exposure_index_m", ascending=False).copy()
        df_exp["exp_risk_label"] = df_exp["exposure_risk_category"].map(risk_category_labels).fillna(df_exp["exposure_risk_category"])
        df_exp["confidence_label"] = df_exp["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
        df_exp["source_label"] = df_exp["source_mix"].apply(lambda x: localize_source_mix(x, lang))

        fig2 = px.bar(
            df_exp, x="province", y="total_exposure_index_m", color="exp_risk_label",
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
        fig2.update_xaxes(type='category', dtick=1, tickangle=-45)
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
