"""
Traffic Impact page.
Analyzes vehicle congestion correlations with PM2.5 levels and ranks hotspots.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from lib.clickhouse_client import query_df
from lib.data_service import (
    build_where_clause,
    get_traffic_correlation_hourly,
    get_traffic_summary_stats,
    get_traffic_ranking_data,
)
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import render_page_hero, render_section_divider, render_info_banner, clean_html, page_wrapper
from lib.chart_config import get_plotly_layout, create_empty_state
from lib.ui_components import render_kpi_card


def clean_label(label):
    if not label:
        return ""
    for prefix in ["phường ", "Phường ", "xã ", "Xã ", "thị trấn ", "Thị trấn "]:
        if label.startswith(prefix):
            return label[len(prefix):]
    return label

def get_hotspot_color(rank_idx):
    if rank_idx < 2:
        return "#EF4444" # Red
    elif rank_idx < 5:
        return "#D97706" # Orange/Gold
    else:
        return "#10B981" # Green



@page_wrapper("traffic", "Ảnh ảnh hưởng giao thông", icon="🚦", skip_hero=True)
def main(lang):
    # ── Page Initialization ────────────────────────────────────────────────────────

    # ── Filters ABOVE Title ────────────────────────────────────────────────────────
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]
    pollutant = filters.get("pollutant", "pm25")

    # ── Page Title & Caption BELOW Filters ─────────────────────────────────────────
    title_text = t("traffic_title", lang)
    if not title_text or title_text == "traffic_title":
        title_text = t("traffic", lang)
    if not title_text or title_text == "traffic":
        title_text = "Ảnh ảnh hưởng giao thông" if lang == "vi" else "Traffic Impact Analysis"

    caption_text = t("traffic_caption", lang)
    if not caption_text or caption_text == "traffic_caption":
        caption_text = "TomTom Flow × AQI.in · tương quan quan sát" if lang == "vi" else "TomTom Flow × AQI.in · observed correlation"

    st.markdown("<div style='margin-top: 0.75rem;'></div>", unsafe_allow_html=True)
    render_page_hero(title_text, caption_text, icon="🚦")

    target_poll = "pm25" if pollutant not in ["pm10"] else pollutant

    if pollutant != "pm25":
        st.warning(
            f"⚠️ Traffic analyses primarily target **PM2.5** and traffic overlap. "
            f"Metrics for **{pollutant.upper()}** are approximations."
            if lang == "en" else
            f"⚠️ Phân tích giao thông chủ yếu tập trung vào **PM2.5** và mật độ tắc nghẽn. "
            f"Các chỉ số cho **{pollutant.upper()}** là ước tính tương đối."
        )

    # ── Data Fetching ─────────────────────────────────────────────────────────────
    with st.spinner(t("loading", lang) if lang == "en" else "Đang phân tích dữ liệu giao thông..."):
        df_hourly = get_traffic_correlation_hourly(date_range, spatial_grain, scope_val, col=target_poll)
        df_summary = get_traffic_summary_stats(spatial_grain, scope_val, date_range)
        df_rank = get_traffic_ranking_data(spatial_grain, scope_val, date_range, col=target_poll)

    if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_pm25):
        stats = df_summary.iloc[0]
        avg_traffic = stats.avg_congestion
        pm25_uplift = stats.avg_pm25_uplift
        comovement_score = stats.avg_comovement_score
        avg_coverage = stats["avg_traffic_coverage_ratio"] if "avg_traffic_coverage_ratio" in stats.index else None
        observed_hours = stats["observed_hours"] if "observed_hours" in stats.index else 0
        low_congestion_hours = stats["low_congestion_hours"] if "low_congestion_hours" in stats.index else 0
        high_congestion_hours = stats["high_congestion_hours"] if "high_congestion_hours" in stats.index else 0
        uplift_sample_days = stats["uplift_sample_days"] if "uplift_sample_days" in stats.index else 0
        
        has_uplift = (
            not pd.isna(pm25_uplift)
            and observed_hours >= 24
            and low_congestion_hours >= 3
            and high_congestion_hours >= 3
            and uplift_sample_days > 0
        )

        # Context Alerts
        render_info_banner(
            t("traffic_caption", lang),
            type="info"
        )
        
        if pd.isna(avg_coverage) or avg_coverage < 0.1 or not has_uplift:
            render_info_banner(
                "Mật độ dữ liệu TomTom tại vùng này chưa đủ tiêu chuẩn để tính chênh lệch phát thải (PM2.5 Uplift). Thống kê Uplift hiển thị N/A."
                if lang == "vi" else
                "TomTom congestion data density is too sparse to evaluate PM2.5 Uplift safely. Uplift card is shown as N/A.",
                type="warning"
            )

        # ── KPI Cards ──────────────────────────────────────────────────────
        # Determine dynamic colors based on values
        # 1. Congestion color
        if avg_traffic >= 0.40:
            congestion_color = "#EF4444" # Red
        elif avg_traffic >= 0.25:
            congestion_color = "#F59E0B" # Amber/Gold
        else:
            congestion_color = "#10B981" # Green

        # 2. PM2.5 Uplift color
        if has_uplift:
            if pm25_uplift >= 10.0:
                uplift_color = "#EF4444" # Red
            elif pm25_uplift >= 5.0:
                uplift_color = "#F59E0B" # Amber/Gold
            else:
                uplift_color = "#10B981" # Green
        else:
            uplift_color = None

        # 3. Co-movement score color
        if comovement_score >= 0.80:
            comovement_color = "#EF4444" # Red
        elif comovement_score >= 0.50:
            comovement_color = "#F59E0B" # Amber/Gold
        else:
            comovement_color = "#CBD5E1" if st.session_state.get("theme", "light") == "dark" else "#0F172A"

        # 4. Coverage ratio color
        if not pd.isna(avg_coverage):
            if avg_coverage >= 0.80:
                coverage_color = "#10B981" # Green
            elif avg_coverage >= 0.50:
                coverage_color = "#F59E0B" # Amber/Gold
            else:
                coverage_color = "#EF4444" # Red
        else:
            coverage_color = None

        c1, c2, c3, c4 = st.columns(4)
        traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"
        uplift_display = f"+{pm25_uplift:.1f} µg/m³" if has_uplift else "N/A"
        coverage_display = f"{avg_coverage:.1%}" if not pd.isna(avg_coverage) else "N/A"

        with c1:
            render_kpi_card(
                t("traffic_congestion_lbl", lang), 
                traffic_display, 
                t("traffic_congestion_sub", lang),
                val_color=congestion_color
            )
        with c2:
            render_kpi_card(
                t("traffic_contribution_lbl", lang), 
                uplift_display, 
                t("traffic_contribution_sub", lang),
                val_color=uplift_color
            )
        with c3:
            render_kpi_card(
                t("traffic_impact_lbl", lang), 
                f"{comovement_score:.2f}", 
                t("traffic_impact_sub", lang),
                val_color=comovement_color
            )
        with c4:
            render_kpi_card(
                t("traffic_coverage_lbl", lang), 
                coverage_display, 
                t("traffic_coverage_sub", lang),
                val_color=coverage_color
            )

        render_section_divider()

        # ── Hourly Correlation & Hotspot Ranking (2-column layout) ─────────
        c_title_left, c_title_right = st.columns([1.0, 1.0], gap="large")
        
        with c_title_left:
            st.markdown(f"##### 📊 {t('traffic_hourly_correlation', lang)} (trục kép)" if lang == "vi" else f"##### 📊 {t('traffic_hourly_correlation', lang)} (Dual Axis)")
            
        with c_title_right:
            right_title = "Điểm nóng giao thông — PM2.5 × congestion" if lang == "vi" else "Traffic Hotspots — PM2.5 × congestion"
            st.markdown(f"##### 🏆 {right_title}")
            
        c_left, c_right = st.columns([1.0, 1.0], gap="large")
        
        with c_left:
            if not df_hourly.empty:
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # PM2.5 Area trace (left Y axis)
                fig.add_trace(
                    go.Scatter(
                        x=df_hourly.hour_val, 
                        y=df_hourly.avg_p, 
                        name=f"{target_poll.upper()} (µg/m³)",
                        fill='tozeroy', 
                        fillcolor='rgba(59, 130, 246, 0.12)',
                        line=dict(shape='spline', smoothing=1.3, color='#3B82F6', width=2.5)
                    ),
                    secondary_y=False,
                )
                
                # Congestion Area trace (right Y axis)
                fig.add_trace(
                    go.Scatter(
                        x=df_hourly.hour_val, 
                        y=df_hourly.avg_congestion * 100, # Display as percentage
                        name="Tắc nghẽn (%)" if lang == "vi" else "Congestion (%)",
                        fill='tozeroy',
                        fillcolor='rgba(217, 119, 6, 0.08)',
                        line=dict(shape='spline', smoothing=1.3, color='#D97706', width=2.5)
                    ),
                    secondary_y=True,
                )
                
                theme = st.session_state.get("theme", "light")
                layout = get_plotly_layout(height=345, compact=True)
                
                fig.update_layout(
                    layout,
                    margin={"l": 40, "r": 40, "t": 20, "b": 20},
                    hovermode="x unified",
                    legend=dict(
                        orientation="h",
                        y=-0.25,
                        x=0.0,
                        xanchor="left",
                        yanchor="top",
                        bgcolor="rgba(0,0,0,0)"
                    )
                )
                
                fig.update_xaxes(
                    title_text="Giờ" if lang == "vi" else "Hour",
                    tickmode='array',
                    tickvals=list(range(24)),
                    ticktext=[str(i) for i in range(24)],
                    gridcolor="rgba(255,255,255,0.05)" if theme == "dark" else "rgba(0,0,0,0.04)"
                )
                
                fig.update_yaxes(
                    title_text=f"{target_poll.upper()} (µg/m³)", 
                    secondary_y=False,
                    gridcolor="rgba(255,255,255,0.05)" if theme == "dark" else "rgba(0,0,0,0.04)"
                )
                fig.update_yaxes(
                    title_text="Tắc nghẽn (%)" if lang == "vi" else "Congestion (%)", 
                    secondary_y=True,
                    showgrid=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu xu hướng giao thông cho vùng này.", height=345), use_container_width=True)
  
        with c_right:
            if not df_rank.empty:
                theme = st.session_state.get("theme", "light")
                text_color = "#cbd5e1" if theme == "dark" else "#0f172a"
                sub_color = "#94a3b8" if theme == "dark" else "#64748b"
                card_bg = "rgba(15, 23, 42, 0.65)" if theme == "dark" else "rgba(255, 255, 255, 0.85)"
                border_color = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(226, 232, 240, 0.8)"
                shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.2)" if theme == "dark" else "0 4px 6px -1px rgba(0, 0, 0, 0.05)"
                glass_blur = "blur(12px)" if theme == "dark" else "blur(8px)"
                
                note = "Chỉ số = PM2.5 avg × congestion avg" if lang == "vi" else "Score = PM2.5 avg × congestion avg"
                
                # Sort and clean data
                df_sorted = df_rank.sort_values(by="impact_score", ascending=False).head(6)
                
                rows_html = ""
                max_score = df_sorted["impact_score"].max() if not df_sorted.empty else 1.0
                bar_bg = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(0, 0, 0, 0.05)"
                
                for idx, (_, row) in enumerate(df_sorted.iterrows()):
                    raw_label = row["label_col"]
                    cleaned_label = clean_label(raw_label)
                    score = row["impact_score"]
                    percentage = (score / max_score) * 100 if max_score > 0 else 0
                    bar_color = get_hotspot_color(idx)
                    
                    rows_html += f"""
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
                        <div style="width: 100px; font-size: 0.85rem; font-weight: 600; color: {text_color}; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="{raw_label}">
                            {cleaned_label}
                        </div>
                        <div style="flex-grow: 1; margin: 0 12px; height: 8px; background: {bar_bg}; border-radius: 4px; overflow: hidden; position: relative;">
                            <div style="width: {percentage}%; height: 100%; background: {bar_color}; border-radius: 4px;"></div>
                        </div>
                        <div style="width: 32px; text-align: right; font-size: 0.85rem; font-weight: 700; color: {text_color};">
                            {score:.1f}
                        </div>
                    </div>
                    """
                    
                html_content = f"""
                <div class="glass-card" style="padding: 1.2rem; min-height: 345px; display: flex; flex-direction: column; justify-content: space-between; background: {card_bg}; backdrop-filter: {glass_blur}; -webkit-backdrop-filter: {glass_blur}; border: 1px solid {border_color}; border-radius: 12px; box-shadow: {shadow}; margin-bottom: 0.65rem;">
                    <div>
                        <div style="display: flex; flex-direction: column; margin-top: 4px;">
                            {rows_html}
                        </div>
                    </div>
                    <div style="font-size: 0.75rem; color: {sub_color}; margin-top: 12px; opacity: 0.8; font-weight: 500;">
                        {note}
                    </div>
                </div>
                """
                st.markdown(clean_html(html_content), unsafe_allow_html=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu xếp hạng giao thông cho vùng này.", height=345), use_container_width=True)
                
        # Center circular down arrow icon
        theme = st.session_state.get("theme", "light")
        btn_bg = "rgba(255, 255, 255, 0.06)" if theme == "dark" else "rgba(0, 0, 0, 0.04)"
        btn_border = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(0, 0, 0, 0.06)"
        arrow_color = "#94a3b8" if theme == "dark" else "#64748b"
        
        st.markdown(f"""
        <div style="display: flex; justify-content: center; margin-top: 1.2rem; margin-bottom: 0.5rem;">
            <div style="width: 40px; height: 40px; border-radius: 50%; background: {btn_bg}; border: 1px solid {btn_border}; display: flex; align-items: center; justify-content: center; cursor: pointer; box-shadow: 0 4px 12px rgba(0,0,0,0.03); transition: all 0.2s;">
                <span style="font-size: 1.2rem; color: {arrow_color}; font-weight: 700;">↓</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu KPI/xu hướng giao thông cho vùng này."), use_container_width=True)




if __name__ == "__main__":
    main()
