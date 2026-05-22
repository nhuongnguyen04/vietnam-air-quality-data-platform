"""Source Comparison page — AQI.in vs OpenWeather comparison."""
from __future__ import annotations

# ruff: noqa: E402
import sys

sys.path.insert(0, "..")

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import (
    build_where_clause,
    get_source_correlation,
    get_source_coverage,
)
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.style import get_plotly_layout, render_metric_card

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("source_comparison_title", lang) if lang == "vi" else "📡 Source Comparison & Correlation")
st.caption(t("source_comparison_caption", lang) if lang == "vi" else "Deep-dive analysis comparing physical ground monitors vs. satellite grid model.")

# ── Methodology explanation (measured vs modeled) ─────────────────────────────
_explanation = (
    "**Tại sao AQI khác nhau giữa 2 nguồn?**\n"
    "- **AQI.in**: Dữ liệu từ **trạm quan trắc thực tế** (ground monitors) — phản ánh ô nhiễm tại điểm đo cụ thể, thường nhạy bén với nguồn thải giao thông/mặt đất.\n"
    "- **OpenWeather (SILAM)**: Ước tính từ **mô hình mạng lưới khí quyển vệ tinh** (~25km grid) — phản ánh trung bình vùng và có độ bao phủ 100% nhưng có xu hướng thấp hơn đo thực tế.\n"
    "- So sánh tương quan Pearson, MAE và Bias giúp đánh giá độ tin cậy và tìm ra hệ số hiệu chỉnh không gian lý tưởng."
) if lang == "vi" else (
    "**Why do AQI values differ between sources?**\n"
    "- **AQI.in**: Physical **ground monitors** — represents local air quality, highly sensitive to immediate ground-level emissions.\n"
    "- **OpenWeather (SILAM)**: Atmospheric **satellite grid model** (~25km) — represents spatial average with 100% coverage, but tends to underestimate compared to physical monitors.\n"
    "- Analyzing Pearson correlation, MAE, and Bias enables data verification and calibration strategies."
)
st.info(_explanation)

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters["pollutant"]
standard = filters["standard"]

# Helper to format metric labels
val_label = "AQI" if pollutant == "aqi" else pollutant.upper()

# ── Helpers ─────────────────────────────────────────────────────────────────────

SOURCE_LABELS = {"aqiin": "📡 Quan trắc mặt đất", "openweather": "🛰️ Mô hình vệ tinh"}
SOURCE_COLORS = {"aqiin": "#2563eb", "openweather": "#f97316"}


@st.cache_data(ttl=300)
def get_source_trend(dates, province: str | None):
    where_clause = build_where_clause("Tỉnh", province, dates)
    q = f"""
    SELECT
        date,
        source,
        round(avg(daily_avg_aqi_us), 1) AS avg_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE {where_clause}
      AND source IN ('aqiin', 'openweather')
    GROUP BY date, source
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_distribution(dates, province: str | None):
    where_clause = build_where_clause("Tỉnh", province, dates)
    q = f"""
    SELECT
        source,
        daily_avg_aqi_us AS avg_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE {where_clause}
      AND source IN ('aqiin', 'openweather')
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_stats(dates):
    where_clause = build_where_clause(None, None, dates)
    q = f"""
    SELECT
        source,
        round(avg(daily_avg_aqi_us), 1)            AS avg_aqi,
        round(max(daily_max_aqi_us), 0)             AS max_aqi,
        round(min(daily_avg_aqi_us), 1)             AS min_aqi,
        count(*)                              AS day_count,
        sum(if(daily_avg_aqi_us <= 50, 1, 0))       AS good_days
    FROM air_quality.fct_air_quality_summary_daily
    WHERE {where_clause}
      AND source IN ('aqiin', 'openweather')
    GROUP BY source
    """
    return query_df(q)


@st.cache_data(ttl=60)
def get_data_freshness():
    q = """
    SELECT
        source,
        reliable_pct,
        latest_lag_hours,
        latest_ingest_lag_hours,
        stale_count,
        offline_count
    FROM air_quality.dm_platform_source_health
    WHERE source IN ('aqiin', 'openweather')
    ORDER BY source
    """
    return query_df(q)

# ── page body ─────────────────────────────────────────────────────────────────────

try:
    # Set up tabbed interface for deep comparison
    tab_corr, tab_stats, tab_freshness = st.tabs([
        "🎯 Tương quan & Chênh lệch" if lang == "vi" else "🎯 Correlation & Bias",
        "📊 Phân bố & Thống kê" if lang == "vi" else "📊 Distribution & Stats",
        "📶 Độ tươi & Sức khỏe Dữ liệu" if lang == "vi" else "📶 Freshness & Platform Health",
    ])
    
    with tab_corr:
        corr_df = get_source_correlation(
            province=scope_val if spatial_grain in ["Tỉnh", "Phường"] else None,
            start_date=date_range[0].strftime("%Y-%m-%d") if date_range and len(date_range) >= 1 else None,
            end_date=date_range[1].strftime("%Y-%m-%d") if date_range and len(date_range) >= 2 else None,
        )
        
        if corr_df.empty:
            st.plotly_chart(render_empty_chart("Không có dữ liệu tương quan cho phạm vi này." if lang == "vi" else "No correlation data available."), width='stretch')
        else:
            both_sources_df = corr_df[corr_df["aqiin_aqi"].notnull() & corr_df["ow_aqi"].notnull()]
            
            # KPI stats row
            c_corr1, c_corr2, c_corr3, c_corr4 = st.columns(4)
            
            avg_bias = both_sources_df["aqi_bias"].mean() if not both_sources_df.empty else float("nan")
            avg_mae = both_sources_df["aqi_mae"].mean() if not both_sources_df.empty else float("nan")
            r_val = both_sources_df["aqiin_pm25"].corr(both_sources_df["ow_pm25"]) if not both_sources_df.empty else float("nan")
            agree_rows = both_sources_df[both_sources_df["category_agreement"].isin(["both_good", "both_unhealthy"])]
            agree_pct = (len(agree_rows) * 100.0 / len(both_sources_df)) if not both_sources_df.empty else 0
            
            with c_corr1:
                render_metric_card(
                    "Độ tương quan Pearson (r)" if lang == "vi" else "Pearson Correlation (r)",
                    f"{r_val:.2f}" if not pd.isna(r_val) else "N/A",
                    icon="insights"
                )
            with c_corr2:
                render_metric_card(
                    "Độ lệch AQI TB (Bias)" if lang == "vi" else "Avg AQI Bias",
                    f"{avg_bias:+.1f} AQI" if not pd.isna(avg_bias) else "N/A",
                    icon="compare_arrows"
                )
            with c_corr3:
                render_metric_card(
                    "Sai số tuyệt đối (MAE)" if lang == "vi" else "Mean Absolute Error",
                    f"{avg_mae:.1f} AQI" if not pd.isna(avg_mae) else "N/A",
                    icon="summarize"
                )
            with c_corr4:
                render_metric_card(
                    "Đồng thuận Category" if lang == "vi" else "Category Agreement",
                    f"{agree_pct:.0f}%" if len(both_sources_df) > 0 else "N/A",
                    icon="fact_check"
                )
                
            st.markdown("---")
            
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.subheader("📈 Xu hướng chất lượng không khí đồng hành" if lang == "vi" else "📈 Temporal Overlay Trend")
                timeline_df = corr_df.groupby("date")[["aqiin_aqi", "ow_aqi"]].mean().reset_index()
                
                fig_timeline = px.line(
                    timeline_df, x="date", y=["aqiin_aqi", "ow_aqi"],
                    labels={
                        "value": "AQI VN",
                        "variable": "Nguồn" if lang == "vi" else "Source",
                        "date": "Ngày" if lang == "vi" else "Date"
                    },
                    color_discrete_map={"aqiin_aqi": "#2563eb", "ow_aqi": "#f97316"}
                )
                fig_timeline.data[0].name = "📡 Trạm mặt đất" if lang == "vi" else "📡 Ground Monitors"
                fig_timeline.data[1].name = "🛰️ Vệ tinh (SILAM)" if lang == "vi" else "🛰️ Satellite (SILAM)"
                fig_timeline.update_layout(get_plotly_layout(height=400), hovermode="x unified")
                st.plotly_chart(fig_timeline, width="stretch")
                
            with col_chart2:
                st.subheader("🎯 Đồ thị phân tán PM2.5 đồng hồ đo" if lang == "vi" else "🎯 PM2.5 Scatter Plot")
                if both_sources_df.empty:
                    st.plotly_chart(render_empty_chart("Không có trạm song hành."), width="stretch")
                else:
                    fig_scatter = px.scatter(
                        both_sources_df, x="aqiin_pm25", y="ow_pm25",
                        hover_name="province", hover_data=["date", "aqiin_aqi", "ow_aqi"],
                        labels={
                            "aqiin_pm25": "PM2.5 Mặt đất (µg/m³)" if lang == "vi" else "Ground PM2.5 (µg/m³)",
                            "ow_pm25": "PM2.5 Vệ tinh (µg/m³)" if lang == "vi" else "Satellite PM2.5 (µg/m³)"
                        },
                        color="aqi_bias",
                        color_continuous_scale="RdBu_r"
                    )
                    fig_scatter.update_layout(get_plotly_layout(height=400))
                    st.plotly_chart(fig_scatter, width="stretch")
                    
            st.markdown("---")
            
            # Provincial bias bar chart
            st.subheader("📍 Độ lệch AQI và Tỷ lệ bao phủ trạm mặt đất theo Tỉnh thành" if lang == "vi" else "📍 Provincial AQI Bias & Station Coverage")
            col_bar1, col_bar2 = st.columns(2)
            with col_bar1:
                if both_sources_df.empty:
                    st.plotly_chart(render_empty_chart("Không có trạm song hành."), width="stretch")
                else:
                    bias_df = both_sources_df.groupby("province")["aqi_bias"].mean().reset_index()
                    bias_df = bias_df.sort_values("aqi_bias", ascending=False).head(15)
                    
                    fig_bias = px.bar(
                        bias_df, x="aqi_bias", y="province", orientation="h",
                        color="aqi_bias", color_continuous_scale="RdBu_r",
                        labels={
                            "aqi_bias": "Độ lệch (Mặt đất - Vệ tinh)" if lang == "vi" else "AQI Bias (Ground - Sat)",
                            "province": "Tỉnh thành" if lang == "vi" else "Province"
                        }
                    )
                    fig_bias.update_layout(get_plotly_layout(height=400))
                    st.plotly_chart(fig_bias, width="stretch")
            
            with col_bar2:
                cov_df = get_source_coverage()
                if cov_df.empty:
                    st.plotly_chart(render_empty_chart("Không có dữ liệu."), width="stretch")
                else:
                    cov_df_plot = cov_df.sort_values("aqiin_coverage_pct", ascending=True).head(15)
                    fig_cov = px.bar(
                        cov_df_plot, x="aqiin_coverage_pct", y="province", orientation="h",
                        color="aqiin_coverage_pct", color_continuous_scale="Blues",
                        labels={
                            "aqiin_coverage_pct": "% Xã/Phường có trạm" if lang == "vi" else "Wards with Monitors %",
                            "province": "Tỉnh thành" if lang == "vi" else "Province"
                        }
                    )
                    fig_cov.update_layout(get_plotly_layout(height=400))
                    st.plotly_chart(fig_cov, width="stretch")
                    
    with tab_stats:
        # Distribution box plot and Stats Table
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader(t("chart_aqi_distribution_by_source", lang))
            dist = get_source_distribution(date_range, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)
            if not dist.empty:
                dist_plot = dist.copy()
                dist_plot["Nguồn"] = dist_plot["source"].map({"aqiin": "📡 Mặt đất", "openweather": "🛰️ Vệ tinh"})
                fig = px.box(
                    dist_plot,
                    x="Nguồn",
                    y="avg_aqi",
                    color="Nguồn",
                    color_discrete_map={"📡 Mặt đất": "#2563eb", "🛰️ Vệ tinh": "#f97316"},
                    labels={"Nguồn": "Nguồn" if lang=="vi" else "Source", "avg_aqi": "AQI VN"},
                )
                fig.update_layout(height=350, showlegend=False, margin={"l": 0, "r": 0, "t": 10, "b": 30})
                st.plotly_chart(fig, width='stretch')
            else:
                st.plotly_chart(render_empty_chart("Không có dữ liệu phân bố."), width='stretch')
                
        with col_right:
            st.subheader(t("chart_aqi_stats_by_source", lang))
            stats = get_source_stats(date_range)
            if not stats.empty:
                stats_display = stats.copy()
                stats_display["Nguồn"] = stats_display["source"].map({"aqiin": "📡 Trạm mặt đất", "openweather": "🛰️ Vệ tinh (SILAM)"})
                stats_display["Trung bình AQI"] = stats_display["avg_aqi"]
                stats_display["AQI tối đa"] = stats_display["max_aqi"]
                stats_display["AQI tối thiểu"] = stats_display["min_aqi"]
                stats_display["Ngày"] = stats_display["day_count"]
                stats_display["Ngày tốt (AQI≤50)"] = stats_display["good_days"]
                stats_display["% Tốt"] = (
                    stats_display["good_days"] / stats_display["day_count"] * 100
                ).round(1).astype(str) + "%"
                st.dataframe(
                    stats_display[[
                        "Nguồn", "Trung bình AQI", "AQI tối đa", "AQI tối thiểu",
                        "Ngày", "Ngày tốt (AQI≤50)", "% Tốt",
                    ]].set_index("Nguồn"),
                    width='stretch',
                    use_container_width=True,
                )
            else:
                st.info("Chưa có thống kê nguồn trong khoảng thời gian đã chọn.")
                
    with tab_freshness:
        # Data freshness and health indicators
        st.subheader(t("chart_data_freshness_by_source", lang))
        freshness = get_data_freshness()
        if not freshness.empty:
            freshness_display = freshness.copy()
            freshness_display["Nguồn"] = freshness_display["source"].map({"aqiin": "📡 Trạm mặt đất", "openweather": "🛰️ Vệ tinh (SILAM)"})
            freshness_display["latest_label"] = freshness_display["latest_lag_hours"].round(1).astype(str) + "h"
            
            fig = px.bar(
                freshness_display,
                x="Nguồn",
                y="reliable_pct",
                color="Nguồn",
                color_discrete_map={"📡 Trạm mặt đất": "#2563eb", "🛰️ Vệ tinh (SILAM)": "#f97316"},
                text="latest_label",
                labels={
                    "reliable_pct": t("reliable_coverage", lang) if lang == "vi" else "Reliability %",
                    "Nguồn": "Nguồn" if lang == "vi" else "Source",
                },
                hover_data={
                    "latest_label": False,
                    "latest_lag_hours": ":.1f",
                    "latest_ingest_lag_hours": ":.1f",
                    "stale_count": True,
                    "offline_count": True,
                },
            )
            fig.update_yaxes(range=[0, 100], ticksuffix="%")
            fig.update_layout(height=320, showlegend=False, margin={"l": 0, "r": 0, "t": 10, "b": 30})
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, width='stretch')
            st.caption("Thời gian lag hiển thị trên cột thể hiện độ trễ thu thập trung bình của dữ liệu (giờ).")
        else:
            st.plotly_chart(render_empty_chart("Không có dữ liệu độ tươi."), width='stretch')

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
