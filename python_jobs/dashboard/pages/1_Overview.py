"""
Overview Dashboard page.
Provides a high-level visual summary of Vietnam air quality.
"""
import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import get_aqi_category
from lib.data_service import (
    get_aqi_distribution,
    get_chart_data,
    get_current_aqi_status,
    get_national_summary,
    get_pollutant_cols,
    get_source_correlation,
    get_source_table,
    localize_confidence_level,
    localize_source_mix,
)
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.tab_renderer import render_coverage_banner, render_3_tabs
from lib.ui_components import render_map_component, render_distribution_chart, render_ranking_chart

def render_source_dashboard(source_name: str, filters: dict, lang: str, theme: str):
    """Render the dashboard for a specific data source."""
    spatial_grain = filters["spatial_grain"]
    time_grain    = filters["time_grain"]
    time_unit     = filters["time_unit"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]
    pollutant     = filters["pollutant"]
    standard      = filters["standard"]

    table_name = get_source_table(spatial_grain, time_grain, source_name)
    display_col, max_col = get_pollutant_cols(pollutant, standard)
    val_label = "AQI" if pollutant == "aqi" else ("PM2.5" if pollutant == "pm25" else ("PM10" if pollutant == "pm10" else pollutant.upper()))

    # 1. Spatial Coverage Banners
    render_coverage_banner(source_name, spatial_grain, scope_val, lang)

    # 2. KPI row
    kpi_cols = st.columns(4)
    m_col = display_col if table_name.endswith("_hourly") else max_col
    summary = get_national_summary(table_name, display_col, m_col, spatial_grain, scope_val, date_range, time_unit, source_name)
    
    if not summary.empty:
        row = summary.iloc[0]
        
        # Format labels and values dynamically depending on pollutant type
        if pollutant != "aqi":
            avg_title = ("Nồng độ Trung bình Quốc gia" if lang == "vi" else "National Avg Concentration") + f" ({val_label})"
            worst_title = ("Nồng độ cao nhất ghi nhận" if lang == "vi" else "Worst Concentration Recorded") + f" ({val_label})"
            unit_suffix = " µg/m³" if pollutant in ["pm25", "pm10", "no2", "so2", "o3"] else " ppb" if pollutant == "co" else ""
            avg_display = f"{int(row.avg_val or 0)}{unit_suffix}"
            worst_display = f"{int(row.max_val or 0)}{unit_suffix}"
        else:
            avg_title = f"{t('metric_national_avg', lang)} ({val_label})"
            worst_title = f"{t('metric_worst', lang)} ({val_label})"
            avg_display = f"{int(row.avg_val or 0)}"
            worst_display = f"{int(row.max_val or 0)}"

        with kpi_cols[0]:
            render_metric_card(avg_title, avg_display, icon="insights")
        with kpi_cols[1]:
            if pollutant != "aqi":
                render_metric_card(
                    "Chất ô nhiễm đang lọc" if lang == "vi" else "Filtered Pollutant",
                    val_label,
                    icon="biotech"
                )
            else:
                render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
        with kpi_cols[2]:
            render_metric_card(worst_title, worst_display, icon="error")
        with kpi_cols[3]:
            render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")
    else:
        for idx in range(4):
            with kpi_cols[idx]:
                render_metric_card(t("metric_national_avg", lang) if idx == 0 else "...", "N/A", icon="insights")

    # Contextual seasonal captions
    if date_range and len(date_range) == 2:
        _months = set()
        if hasattr(date_range[0], 'month'): _months.add(date_range[0].month)
        if hasattr(date_range[1], 'month'): _months.add(date_range[1].month)
        if _months & {5, 6, 7, 8, 9}:
            _seasonal = (
                "🌧 **Mùa mưa (Tháng 5-9):** PM2.5 giảm đáng kể nhờ hiệu ứng rửa trôi khí quyển và luồng gió."
                if lang == "vi" else
                "🌧 **Monsoon Context (May-Sep):** PM2.5 drops significantly due to rain wash-out."
            )
            st.caption(f"<div style='font-style:italic; opacity:0.8; font-size:0.85rem;'>{_seasonal}</div>", unsafe_allow_html=True)

    # 3. Dynamic map rendering & Charts (2-column layout: Map on the left, stacked Charts on the right)
    c_map, c_charts = st.columns([1.1, 0.9], gap="large")

    map_df = get_chart_data(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name)

    with c_map:
        st.markdown(f"#### 🗺️ {t('map_title', lang)} ({val_label})")
        # Scatter is default and only view mode now
        render_map_component(map_df, "Scatter", spatial_grain, source_name, scope_val, pollutant, standard, theme, val_label, lang, height=500)

    with c_charts:
        # 1st chart: Top 10 ranking
        top_title = "Top 10 Ô Nhiễm" if lang == "vi" else "Top 10 Polluted"
        st.markdown(f"#### 🏆 {top_title}")
        if not map_df.empty:
            color_scale = "Viridis" if theme == "light" else "Plasma"
            range_val = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]
            label_col = "ward_name" if (spatial_grain in ["Tỉnh", "Phường"] or source_name == "aqiin") else "province"

            if source_name == "aqiin":
                agg_cols = ["display_val"]
                if "confidence_score" in map_df.columns: agg_cols.append("confidence_score")
                
                if spatial_grain not in ["Tỉnh", "Phường"]:
                    rank_df = map_df.groupby("province")[agg_cols].mean().reset_index()
                    bar_y_col = "province"
                else:
                    group_col = "actual_ward_name" if "actual_ward_name" in map_df.columns else "ward_name"
                    rank_df = map_df.groupby(["province", group_col])[agg_cols].mean().reset_index()
                    bar_y_col = group_col
            else:
                rank_df = map_df.copy()
                bar_y_col = label_col

            render_ranking_chart(rank_df, bar_y_col, color_scale, range_val, val_label, pollutant, standard, lang, height=230)
        else:
            st.plotly_chart(create_empty_state("No data", height=230), use_container_width=True)

        # 2nd chart: Distribution
        dist_title = t('chart_aqi_dist', lang) if pollutant == 'aqi' else 'Phân bố ' + val_label
        st.markdown(f"#### 📊 {dist_title}")
        df_dist = get_aqi_distribution(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name, lang=lang) if pollutant == "aqi" else map_df
        render_distribution_chart(df_dist, pollutant, standard, val_label, lang, height=230)


def render_comparison_tab(filters: dict, lang: str):
    """Render comparison between ground and satellite data."""
    spatial_grain = filters["spatial_grain"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]

    st.markdown("#### ⚡ Tương quan & Độ tin cậy giữa các nguồn")
    st.markdown("So sánh chênh lệch giữa trạm quan trắc mặt đất (AQI.in) và mô hình lưới SILAM (OpenWeather)." if lang == "vi" else "Compare observations from ground monitors (AQI.in) vs SILAM satellite grid model.")

    corr_df = get_source_correlation(
        province=scope_val if spatial_grain in ["Tỉnh", "Phường"] else None,
        start_date=date_range[0].strftime("%Y-%m-%d") if date_range and len(date_range) >= 1 else None,
        end_date=date_range[1].strftime("%Y-%m-%d") if date_range and len(date_range) >= 2 else None,
    )

    if corr_df.empty:
        st.plotly_chart(create_empty_state("No comparison data available for current selection."), use_container_width=True)
        return

    both_sources_df = corr_df[corr_df["aqiin_aqi"].notnull() & corr_df["ow_aqi"].notnull()]
    
    avg_bias = both_sources_df["aqi_bias"].mean()
    avg_mae = both_sources_df["aqi_mae"].mean()
    agree_rows = both_sources_df[both_sources_df["category_agreement"].isin(["both_good", "both_unhealthy"])]
    agree_pct = (len(agree_rows) * 100.0 / len(both_sources_df)) if len(both_sources_df) > 0 else 0

    c_corr = st.columns(4)
    with c_corr[0]:
        val = "Có trạm đo" if not both_sources_df.empty else "Không có trạm"
        render_metric_card("Trạm mặt đất" if lang == "vi" else "Ground Status", val, icon="location")
    with c_corr[1]:
        bias_text = f"{avg_bias:+.1f} AQI" if not pd.isna(avg_bias) else "N/A"
        render_metric_card("Độ lệch TB (Bias)", bias_text, icon="insights")
    with c_corr[2]:
        mae_text = f"{avg_mae:.1f} AQI" if not pd.isna(avg_mae) else "N/A"
        render_metric_card("Sai số MAE", mae_text, icon="error")
    with c_corr[3]:
        agree_text = f"{agree_pct:.0f}%" if len(both_sources_df) > 0 else "N/A"
        render_metric_card("Đồng thuận phân loại", agree_text, icon="star")

    render_section_divider()

    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.markdown("##### 📈 Xu hướng thời gian theo Nguồn")
        timeline_df = corr_df.groupby("date")[["aqiin_aqi", "ow_aqi"]].mean().reset_index()
        fig_timeline = px.line(
            timeline_df, x="date", y=["aqiin_aqi", "ow_aqi"],
            labels={"value": "AQI VN", "variable": "Nguồn"},
            color_discrete_map={"aqiin_aqi": SOURCE_PALETTE["📡 Mặt đất"], "ow_aqi": SOURCE_PALETTE["🛰️ Vệ tinh"]}
        )
        fig_timeline.data[0].name = "📡 Mặt đất"
        fig_timeline.data[1].name = "🛰️ Vệ tinh"
        fig_timeline.update_layout(get_plotly_layout(height=280, compact=True), hovermode="x unified")
        st.plotly_chart(fig_timeline, use_container_width=True)

    with col_chart2:
        st.markdown("##### 🎯 Tương quan PM2.5 (Mặt đất vs Vệ tinh)")
        if both_sources_df.empty:
            st.plotly_chart(create_empty_state("No pairwise data for scatter plot", height=280), use_container_width=True)
        else:
            r_val = both_sources_df["aqiin_pm25"].corr(both_sources_df["ow_pm25"])
            fig_scatter = px.scatter(
                both_sources_df, x="aqiin_pm25", y="ow_pm25",
                hover_name="province", hover_data=["date", "aqiin_aqi", "ow_aqi"],
                labels={"aqiin_pm25": "Mặt đất (µg/m³)", "ow_pm25": "Vệ tinh (µg/m³)"},
                color="aqi_bias",
                color_continuous_scale="RdBu_r"
            )
            fig_scatter.update_layout(get_plotly_layout(height=280, compact=True))
            st.plotly_chart(fig_scatter, use_container_width=True)
            if not pd.isna(r_val):
                st.caption(f"<div style='font-size:0.8rem; margin-top:-10px;'>Hệ số tương quan tuyến tính Pearson (r): **{r_val:.2f}**</div>", unsafe_allow_html=True)


@page_wrapper("overview", "📊 Vietnam Air Quality Overview", icon="📊")
def main(lang: str):
    theme = st.session_state.get("theme", "light")
    
    # Call filter bar once at the top so it is shared across all tabs
    filters = render_top_filters()

    def render_ground():
        render_source_dashboard("aqiin", filters, lang, theme)

    def render_sat():
        render_source_dashboard("openweather", filters, lang, theme)

    def render_comp():
        render_comparison_tab(filters, lang)

    render_3_tabs(
        lang=lang,
        ground_label="📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
        sat_label="🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
        comp_label="📊 So sánh chỉ số" if lang == "vi" else "📊 Index Comparison",
        render_ground_fn=render_ground,
        render_sat_fn=render_sat,
        render_comp_fn=render_comp,
        sat_info_text_vi="🛰️ Mô hình SILAM: Phủ sóng 100% nhưng có xu hướng đánh giá thấp (underestimate) AQI thực tế từ 1.5 - 2.5 lần.",
        sat_info_text_en="🛰️ SILAM Model: 100% coverage, but typically underestimates AQI by 1.5x - 2.5x vs ground monitors."
    )

if __name__ == "__main__":
    main()
