"""
Overview Dashboard page.
Provides a high-level visual summary of Vietnam air quality.
"""
import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import (
    get_aqi_category,
    get_aqi_color_range,
    get_aqi_color_scale,
    get_aqi_colorbar_config,
    get_aqi_discrete_colors,
    render_empty_chart,
)
from lib.data_service import (
    get_aqi_distribution,
    get_chart_data,
    get_current_aqi_status,
    get_national_summary,
    get_pollutant_cols,
    get_source_correlation,
    get_source_coverage,
    get_source_table,
    localize_confidence_level,
    localize_source_mix,
)
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.tab_renderer import render_coverage_banner, render_3_tabs

@page_wrapper("overview", "📊 Vietnam Air Quality Overview", icon="📊")
def main(lang: str):
    theme = st.session_state.get("theme", "light")
    
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_sidebar_filters()
    spatial_grain = filters["spatial_grain"]
    time_grain    = filters["time_grain"]
    time_unit     = filters["time_unit"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]
    pollutant     = filters["pollutant"]
    standard      = filters["standard"]

    # ── Real-Time Alert Banner ──────────────────────────────────────────────────
    current_df = get_current_aqi_status()
    if not current_df.empty:
        alert_provinces = current_df[current_df["current_aqi"] > 150]
        
        # Premium Alert Card
        if not alert_provinces.empty:
            alert_lines = " · ".join(
                f"🔴 **{row.province}**: {int(row.current_aqi)}"
                for _, row in alert_provinces.head(6).iterrows()
            )
            st.markdown(f"""
            <div class="glass-card" style="border-left: 5px solid #ef4444; background: rgba(239, 68, 68, 0.05); padding: 1rem 1.25rem;">
                <h4 style="margin:0 0 0.5rem 0; color:#ef4444;">⚠️ Cảnh báo chất lượng không khí nguy hại (AQI &gt; 150)</h4>
                <p style="margin:0; font-size:0.9rem; line-height:1.4;">{alert_lines}</p>
                <p style="margin:0.5rem 0 0 0; font-size:0.75rem; opacity:0.6; font-style:italic;">
                    Dữ liệu giờ gần nhất đồng bộ với hệ thống cảnh báo Telegram. Các biểu đồ bên dưới hiển thị theo lọc lịch sử.
                </p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="glass-card" style="border-left: 5px solid #10b981; background: rgba(16, 185, 129, 0.05); padding: 0.85rem 1.25rem; display:flex; align-items:center; gap:8px;">
                <span style="color:#10b981; font-size:1.1rem;">✅</span>
                <span style="font-size:0.9rem; font-weight:500;">
                    Không có tỉnh thành nào vượt ngưỡng AQI 150 (Dữ liệu quan trắc giờ gần nhất)
                </span>
            </div>
            """, unsafe_allow_html=True)

        with st.expander("📊 Bảng AQI hiện tại theo tỉnh (Cập nhật liên tục)", expanded=False):
            display_current = current_df.copy()
            display_current["AQI hiện tại"] = display_current["current_aqi"].apply(lambda x: int(x))
            display_current["Mức độ"] = display_current["current_aqi"].apply(lambda x: {
                "Good": "🟢 Tốt",
                "Moderate": "🟡 Trung bình",
                "Unhealthy for Sensitive Groups": "🟠 Kém (nhạy cảm)",
                "Unhealthy": "🔴 Xấu",
                "Very Unhealthy": "🟣 Rất xấu",
                "Hazardous": "⚫ Nguy hại",
            }.get(get_aqi_category(x), "❓"))
            display_current["Cập nhật lúc"] = display_current["as_of_hour"].apply(
                lambda x: x.strftime("%H:%M %d/%m") if hasattr(x, "strftime") else str(x)
            )
            display_current["Nguồn"] = display_current["source_mix"].apply(lambda x: localize_source_mix(x, lang))
            display_current["Độ tin cậy"] = display_current["confidence_level"].apply(
                lambda x: localize_confidence_level(x, lang)
            )
            st.dataframe(
                display_current[[
                    "province", "AQI hiện tại", "Mức độ", "main_pollutant",
                    "Nguồn", "Độ tin cậy", "Cập nhật lúc"
                ]]
                .rename(columns={"province": "Tỉnh/thành", "main_pollutant": "Ô nhiễm chính"}),
                hide_index=True,
                use_container_width=True,
            )

    render_section_divider()

    # Column mapping & labels
    display_col, max_col = get_pollutant_cols(pollutant, standard)
    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()

    # Filter Label formatting
    if time_unit == "hour" and date_range and len(date_range) == 2:
        dr_start, dr_end = date_range
        filter_label = f"{dr_start.strftime('%H:%M %d/%m/%Y')} → {dr_end.strftime('%H:%M %d/%m/%Y')}"
    else:
        filter_label = f"{date_range[0]} → {date_range[1]}" if date_range and len(date_range) == 2 else ""

    # ── Source dashboard renderer ──────────────────────────────────────────────
    def render_source_dashboard(source_name: str):
        table_name = get_source_table(spatial_grain, time_grain, source_name)
        
        # 1. Spatial Coverage Banners
        render_coverage_banner(source_name, spatial_grain, scope_val, lang)

        # 2. KPI row
        kpi_cols = st.columns(4)
        m_col = display_col if table_name.endswith("_hourly") else max_col
        summary = get_national_summary(table_name, display_col, m_col, spatial_grain, scope_val, date_range, time_unit, source_name)
        
        if not summary.empty:
            row = summary.iloc[0]
            with kpi_cols[0]:
                render_metric_card(f"{t('metric_national_avg', lang)} ({val_label})", f"{int(row.avg_val or 0)}", icon="insights")
            with kpi_cols[1]:
                render_metric_card(t("metric_dominant", lang), (row.dominant_pollutant or "N/A").upper(), icon="biotech")
            with kpi_cols[2]:
                render_metric_card(f"{t('metric_worst', lang)} ({val_label})", f"{int(row.max_val or 0)}", icon="error")
            with kpi_cols[3]:
                render_metric_card(t("metric_active", lang), f"{int(row.province_count or 0)}", icon="location")
        else:
            for idx in range(4):
                with kpi_cols[idx]:
                    render_metric_card(t("metric_national_avg", lang) if idx == 0 else "...", "N/A", icon="insights")

        # Contextual seasonal captions
        if date_range and len(date_range) == 2:
            _months = set()
            if hasattr(date_range[0], 'month'):
                _months.add(date_range[0].month)
            if hasattr(date_range[1], 'month'):
                _months.add(date_range[1].month)
            if _months & {5, 6, 7, 8, 9}:
                _seasonal = (
                    "🌧 **Mùa mưa (Tháng 5-9):** PM2.5 giảm đáng kể nhờ hiệu ứng rửa trôi khí quyển "
                    "và luồng gió mùa Tây Nam phát tán ô nhiễm."
                    if lang == "vi" else
                    "🌧 **Monsoon Context (May-Sep):** PM2.5 drops significantly due to rain wash-out "
                    "and southwest monsoon wind dispersion."
                )
                st.caption(f"<div style='font-style:italic; opacity:0.8; font-size:0.85rem;'>{_seasonal}</div>", unsafe_allow_html=True)

        # 3. Dynamic map rendering
        c_map1, c_map2 = st.columns([0.75, 0.25], vertical_alignment="center")
        with c_map1:
            st.markdown(f"#### 🗺️ {t('map_title', lang)} ({val_label})")
        with c_map2:
            map_view = st.segmented_control(
                "Map Style",
                options=["Scatter", "Heatmap"],
                default="Scatter",
                key=f"map_view_{source_name}",
                label_visibility="collapsed"
            )

        map_df = get_chart_data(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name)
        label_col = "ward_name" if (spatial_grain in ["Tỉnh", "Phường"] or source_name == "aqiin") else "province"

        if pollutant == "aqi":
            color_scale = get_aqi_color_scale(standard)
            range_val   = get_aqi_color_range(standard)
        else:
            color_scale = "Viridis" if theme == "light" else "Plasma"
            range_val   = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]

        if not map_df.empty:
            map_lat   = map_df.latitude.mean()
            map_lon   = map_df.longitude.mean()
            zoom_level = 8 if (spatial_grain in ["Tỉnh", "Phường"] or (source_name == "aqiin" and scope_val)) else 5

            tooltip_data = {
                "province": True,
                "display_val": ":.1f",
                "latitude": False,
                "longitude": False,
            }
            if "confidence_score" in map_df.columns:
                tooltip_data["confidence_score"] = ":.2f"
            if "source_mix" in map_df.columns:
                tooltip_data["source_mix"] = True

            if map_view == "Heatmap":
                fig_map = px.density_map(
                    map_df,
                    lat="latitude", lon="longitude",
                    z="display_val",
                    radius=18,
                    hover_name=label_col,
                    hover_data=tooltip_data,
                    color_continuous_scale=color_scale,
                    range_color=range_val,
                    zoom=zoom_level,
                    center={"lat": map_lat, "lon": map_lon},
                )
            else:
                fig_map = px.scatter_map(
                    map_df,
                    lat="latitude", lon="longitude",
                    color="display_val",
                    hover_name=label_col,
                    hover_data=tooltip_data,
                    color_continuous_scale=color_scale,
                    range_color=range_val,
                    zoom=zoom_level,
                    center={"lat": map_lat, "lon": map_lon},
                    size="display_val",
                    size_max=24,
                    labels={
                        "display_val": val_label,
                        "province":    t("province", lang),
                        "ward_name":   t("location", lang),
                    },
                )
            map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
            fig_map.update_layout(
                mapbox_style=map_style,
                height=520,
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
            )
            if pollutant == "aqi":
                fig_map.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.plotly_chart(
                create_empty_state(t("no_data", lang) if lang == "en" else "Không có dữ liệu cho vùng này."),
                use_container_width=True,
            )

        # 4. Distribution & Rank columns
        c1, c2 = st.columns(2)
        with c1:
            if pollutant == "aqi":
                st.markdown(f"#### 📊 {t('chart_aqi_dist', lang)}")
                df_dist = get_aqi_distribution(table_name, display_col, spatial_grain, scope_val, date_range, time_unit, source_name, lang=lang)
                if not df_dist.empty:
                    aqi_colors = get_aqi_discrete_colors(standard)
                    color_map = {
                        t("aqi_good", lang):           aqi_colors["Good"],
                        t("aqi_moderate", lang):        aqi_colors["Moderate"],
                        t("aqi_unhealthy_sg", lang):    aqi_colors["Unhealthy for Sensitive Groups"],
                        t("aqi_unhealthy", lang):       aqi_colors["Unhealthy"],
                        t("aqi_very_unhealthy", lang):  aqi_colors["Very Unhealthy"],
                        t("aqi_hazardous", lang):       aqi_colors["Hazardous"],
                    }
                    fig_pie = px.pie(
                        df_dist, values="count", names="aqi_category",
                        color="aqi_category", color_discrete_map=color_map,
                    )
                    fig_pie.update_layout(get_plotly_layout(height=350, compact=True))
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.plotly_chart(create_empty_state("No data"), use_container_width=True)
            else:
                dist_title = f"Phân bố {val_label}" if lang == "vi" else f"{val_label} Distribution"
                st.markdown(f"#### 📊 {dist_title}")
                if not map_df.empty:
                    fig_hist = px.histogram(
                        map_df, x="display_val", marginal="box",
                        labels={"display_val": val_label, "count": t("chart_label_count", lang)},
                    )
                    fig_hist.update_layout(get_plotly_layout(height=350, compact=True))
                    st.plotly_chart(fig_hist, use_container_width=True)

        with c2:
            st.markdown(f"#### 🏆 {t('chart_top_polluted', lang)}")
            if not map_df.empty:
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

                    # Filter medium-or-better confidence
                    if "confidence_score" in rank_df.columns:
                        high_conf = rank_df[rank_df["confidence_score"] >= 0.38]
                        if len(high_conf) >= 5:
                            rank_df = high_conf

                df_top = rank_df.sort_values("display_val", ascending=False).head(8)
                df_top = df_top.sort_values("display_val", ascending=True)

                fig_bar = px.bar(
                    df_top,
                    y=bar_y_col,
                    x="display_val",
                    orientation="h",
                    color="display_val",
                    color_continuous_scale=color_scale,
                    range_color=range_val,
                    labels={"display_val": val_label, "province": t("province", lang)},
                )
                fig_bar.update_layout(get_plotly_layout(height=350, compact=True))
                if pollutant == "aqi":
                    fig_bar.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("No data"), use_container_width=True)

    # ── Comparison tab renderer ──────────────────────────────────────────────────
    def render_comparison_tab():
        st.markdown("#### ⚡ Tương quan & Độ tin cậy giữa các nguồn")
        st.markdown(
            "So sánh chênh lệch giữa trạm quan trắc mặt đất (AQI.in) và mô hình lưới SILAM (OpenWeather)."
            if lang == "vi" else
            "Compare observations from ground monitors (AQI.in) vs SILAM satellite grid model."
        )

        corr_df = get_source_correlation(
            province=scope_val if spatial_grain in ["Tỉnh", "Phường"] else None,
            start_date=date_range[0].strftime("%Y-%m-%d") if date_range and len(date_range) >= 1 else None,
            end_date=date_range[1].strftime("%Y-%m-%d") if date_range and len(date_range) >= 2 else None,
        )

        if corr_df.empty:
            st.plotly_chart(create_empty_state("No comparison data available for current selection."), use_container_width=True)
        else:
            both_sources_df = corr_df[corr_df["aqiin_aqi"].notnull() & corr_df["ow_aqi"].notnull()]
            
            # Metrics
            both_sources_count = both_sources_df["province"].nunique()
            total_provinces = corr_df["province"].nunique()
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
                fig_timeline.update_layout(get_plotly_layout(height=350, compact=True), hovermode="x unified")
                st.plotly_chart(fig_timeline, use_container_width=True)

            with col_chart2:
                st.markdown("##### 🎯 Tương quan PM2.5 (Mặt đất vs Vệ tinh)")
                if both_sources_df.empty:
                    st.plotly_chart(create_empty_state("No pairwise data for scatter plot"), use_container_width=True)
                else:
                    r_val = both_sources_df["aqiin_pm25"].corr(both_sources_df["ow_pm25"])
                    fig_scatter = px.scatter(
                        both_sources_df, x="aqiin_pm25", y="ow_pm25",
                        hover_name="province", hover_data=["date", "aqiin_aqi", "ow_aqi"],
                        labels={"aqiin_pm25": "Mặt đất (µg/m³)", "ow_pm25": "Vệ tinh (µg/m³)"},
                        color="aqi_bias",
                        color_continuous_scale="RdBu_r"
                    )
                    fig_scatter.update_layout(get_plotly_layout(height=350, compact=True))
                    st.plotly_chart(fig_scatter, use_container_width=True)
                    if not pd.isna(r_val):
                        st.caption(f"Hệ số tương quan tuyến tính Pearson (r): **{r_val:.2f}**")

    # ── Execute 3-Tab Renderer ──────────────────────────────────────────────
    render_3_tabs(
        lang=lang,
        ground_label="📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
        sat_label="🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
        comp_label="📊 Tương quan & Độ tin cậy" if lang == "vi" else "📊 Correlation",
        render_ground_fn=lambda: render_source_dashboard("aqiin"),
        render_sat_fn=lambda: render_source_dashboard("openweather"),
        render_comp_fn=render_comparison_tab,
        sat_info_text_vi="🛰️ Mô hình SILAM: Phủ sóng 100% nhưng có xu hướng đánh giá thấp (underestimate) AQI thực tế từ 1.5 - 2.5 lần.",
        sat_info_text_en="🛰️ SILAM Model: 100% coverage, but typically underestimates AQI by 1.5x - 2.5x vs ground monitors."
    )

if __name__ == "__main__":
    main()
