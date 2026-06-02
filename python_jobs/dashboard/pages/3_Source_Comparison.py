"""
Source Comparison & Platform Health.
Provides comparative analytics and platform performance metrics.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib.clickhouse_client import query_df
from lib.data_service import (
    build_where_clause,
    get_source_correlation,
    get_source_coverage,
)
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider, render_info_banner
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.style import render_metric_card

SOURCE_LABELS = {"aqiin": "📡 Quan trắc mặt đất", "openweather": "🛰️ Mô hình vệ tinh"}
SOURCE_COLORS = {"aqiin": "#0891B2", "openweather": "#F59E0B"}

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

@page_wrapper("source_comparison", "📡 Source Comparison & Correlation", icon="📡")
def main(lang: str):
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_top_filters()

    # ── Methodology expandable card ─────────────────────────────────────────────
    with st.expander("ℹ️  Phương pháp so sánh dữ liệu (Quan trắc vs Vệ tinh)", expanded=False):
        st.markdown(
            "**Tại sao chỉ số AQI lại khác nhau giữa 2 nguồn dữ liệu?**\n"
            "- **📡 Trạm đo mặt đất (AQI.in)**: Phản ánh nồng độ thực tế đo tại một vị trí địa lý cố định. "
            "Cực kỳ nhạy bén với luồng khí thải trực tiếp (như giao thông, đô thị) nhưng bị giới hạn về bán kính phủ sóng.\n"
            "- **🛰️ Mô hình lưới vệ tinh (OpenWeather/SILAM)**: Sử dụng các mô phỏng toán học khí quyển lưới ô ~25km. "
            "Cung cấp độ phủ sóng liên tục 100% lãnh thổ Việt Nam nhưng các đỉnh ô nhiễm cục bộ thường bị làm mịn nên thấp hơn trạm thực tế.\n"
            "- **Đánh giá tương quan**: Sử dụng hệ số Pearson (r), độ lệch trung bình (Bias) và sai số tuyệt đối MAE để làm cơ sở hiệu chuẩn mô hình tốt hơn."
            if lang == "vi" else
            "**Why do AQI values differ between observed and simulated sources?**\n"
            "- **📡 Ground Monitors (AQI.in)**: High fidelity local observations. Captures immediate ground-level emissions "
            "but lacks nationwide spatial coverage.\n"
            "- **🛰️ Satellite Model (SILAM)**: Grid simulation (~25km resolution) providing complete spatial coverage. "
            "Typically smooths out local peak concentrations, resulting in underestimations.\n"
            "- **Analytics Integration**: Computes Pearson correlation (r), Bias and Mean Absolute Error (MAE) for scientific calibration."
        )
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]
    pollutant = filters["pollutant"]
    standard = filters["standard"]

    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()

    # ── Render tabs ──────────────────────────────────────────────────────────────
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
            st.plotly_chart(create_empty_state("Không có dữ liệu tương quan cho phạm vi này."), use_container_width=True)
        else:
            both_sources_df = corr_df[corr_df["aqiin_aqi"].notnull() & corr_df["ow_aqi"].notnull()]

            # KPI card row
            c_corr = st.columns(4)
            avg_bias = both_sources_df["aqi_bias"].mean() if not both_sources_df.empty else float("nan")
            avg_mae = both_sources_df["aqi_mae"].mean() if not both_sources_df.empty else float("nan")
            r_val = both_sources_df["aqiin_pm25"].corr(both_sources_df["ow_pm25"]) if not both_sources_df.empty else float("nan")
            agree_rows = both_sources_df[both_sources_df["category_agreement"].isin(["both_good", "both_unhealthy"])]
            agree_pct = (len(agree_rows) * 100.0 / len(both_sources_df)) if not both_sources_df.empty else 0

            with c_corr[0]:
                render_metric_card("Tương quan Pearson (r)", f"{r_val:.2f}" if not pd.isna(r_val) else "N/A", icon="insights")
            with c_corr[1]:
                render_metric_card("Độ lệch AQI TB (Bias)", f"{avg_bias:+.1f} AQI" if not pd.isna(avg_bias) else "N/A", icon="star")
            with c_corr[2]:
                render_metric_card("Sai số MAE", f"{avg_mae:.1f} AQI" if not pd.isna(avg_mae) else "N/A", icon="error")
            with c_corr[3]:
                render_metric_card("Đồng thuận Category", f"{agree_pct:.0f}%" if len(both_sources_df) > 0 else "N/A", icon="schedule")

            render_section_divider()

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.markdown("##### 📈 Diễn biến xu hướng thời gian song hành")
                timeline_df = corr_df.groupby("date")[["aqiin_aqi", "ow_aqi"]].mean().reset_index()
                fig_timeline = px.line(
                    timeline_df, x="date", y=["aqiin_aqi", "ow_aqi"],
                    labels={"value": "AQI VN", "variable": "Nguồn", "date": "Ngày"},
                    color_discrete_map={"aqiin_aqi": "#0891B2", "ow_aqi": "#F59E0B"}
                )
                fig_timeline.data[0].name = "📡 Trạm mặt đất"
                fig_timeline.data[1].name = "🛰️ Vệ tinh"
                fig_timeline.update_layout(get_plotly_layout(height=360, compact=True), hovermode="x unified")
                st.plotly_chart(fig_timeline, use_container_width=True)

            with col_chart2:
                st.markdown("##### 🎯 Tương quan phân tán nồng độ PM2.5")
                if both_sources_df.empty:
                    st.plotly_chart(create_empty_state("Không có trạm song hành."), use_container_width=True)
                else:
                    fig_scatter = px.scatter(
                        both_sources_df, x="aqiin_pm25", y="ow_pm25",
                        hover_name="province", hover_data=["date", "aqiin_aqi", "ow_aqi"],
                        labels={"aqiin_pm25": "Mặt đất (µg/m³)", "ow_pm25": "Vệ tinh (µg/m³)"},
                        color="aqi_bias",
                        color_continuous_scale="RdBu_r"
                    )
                    fig_scatter.update_layout(get_plotly_layout(height=360, compact=True))
                    st.plotly_chart(fig_scatter, use_container_width=True)

            render_section_divider()

            st.markdown("##### 📍 Độ lệch AQI và Tỷ lệ bao phủ theo Tỉnh thành")
            col_bar1, col_bar2 = st.columns(2)
            with col_bar1:
                if both_sources_df.empty:
                    st.plotly_chart(create_empty_state("Không có trạm song hành."), use_container_width=True)
                else:
                    bias_df = both_sources_df.groupby("province")["aqi_bias"].mean().reset_index()
                    bias_df = bias_df.sort_values("aqi_bias", ascending=False).head(10)

                    fig_bias = px.bar(
                        bias_df, x="aqi_bias", y="province", orientation="h",
                        color="aqi_bias", color_continuous_scale="RdBu_r",
                        labels={"aqi_bias": "Độ lệch (Mặt đất - Vệ tinh)", "province": "Tỉnh thành"}
                    )
                    fig_bias.update_layout(get_plotly_layout(height=340, compact=True))
                    st.plotly_chart(fig_bias, use_container_width=True)

            with col_bar2:
                cov_df = get_source_coverage()
                if cov_df.empty:
                    st.plotly_chart(create_empty_state("Không có dữ liệu."), use_container_width=True)
                else:
                    cov_df_plot = cov_df.sort_values("aqiin_coverage_pct", ascending=True).head(10)
                    fig_cov = px.bar(
                        cov_df_plot, x="aqiin_coverage_pct", y="province", orientation="h",
                        color="aqiin_coverage_pct", color_continuous_scale="Blues",
                        labels={"aqiin_coverage_pct": "% Xã/Phường có trạm", "province": "Tỉnh thành"}
                    )
                    fig_cov.update_layout(get_plotly_layout(height=340, compact=True))
                    st.plotly_chart(fig_cov, use_container_width=True)

    with tab_stats:
        trend_df = get_source_trend(date_range, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)
        stats_df = get_source_stats(date_range)

        if trend_df.empty:
            st.plotly_chart(create_empty_state("Không có dữ liệu thống kê cho lựa chọn này."), use_container_width=True)
        else:
            c_stat = st.columns(2)
            with c_stat[0]:
                st.markdown("##### 📈 Xu hướng so sánh trung bình")
                trend_df["Nguồn"] = trend_df["source"].map(SOURCE_LABELS)
                fig_trend = px.line(
                    trend_df, x="date", y="avg_aqi", color="Nguồn",
                    labels={"avg_aqi": "Chỉ số AQI", "date": "Ngày"},
                    color_discrete_map={"📡 Quan trắc mặt đất": "#0891B2", "🛰️ Mô hình vệ tinh": "#F59E0B"}
                )
                fig_trend.update_layout(get_plotly_layout(height=380, compact=True), hovermode="x unified")
                st.plotly_chart(fig_trend, use_container_width=True)

            with c_stat[1]:
                st.markdown("##### 📊 Biểu đồ phân bổ mật độ xác suất (Boxplot)")
                fig_box = px.box(
                    trend_df, x="Nguồn", y="avg_aqi", color="Nguồn",
                    labels={"avg_aqi": "Phân bố AQI"},
                    color_discrete_map={"📡 Quan trắc mặt đất": "#0891B2", "🛰️ Mô hình vệ tinh": "#F59E0B"}
                )
                fig_box.update_layout(get_plotly_layout(height=380, compact=True))
                st.plotly_chart(fig_box, use_container_width=True)

            render_section_divider()

            st.markdown("##### 📋 Thống kê tổng hợp")
            if not stats_df.empty:
                stats_df["Nguồn"] = stats_df["source"].map(SOURCE_LABELS)
                st.dataframe(
                    stats_df[["Nguồn", "avg_aqi", "max_aqi", "min_aqi", "day_count", "good_days"]]
                    .rename(columns={
                        "avg_aqi": "AQI Trung bình",
                        "max_aqi": "AQI Lớn nhất",
                        "min_aqi": "AQI Nhỏ nhất",
                        "day_count": "Tổng số ngày",
                        "good_days": "Số ngày Tốt (≤ 50)"
                    }),
                    hide_index=True,
                    use_container_width=True
                )

    with tab_freshness:
        st.markdown("##### ⚡ Sức khỏe & Độ tươi dữ liệu nạp")
        health_df = get_data_freshness()

        if health_df.empty:
            st.plotly_chart(create_empty_state("Chưa có thông tin sức khỏe hệ thống."), use_container_width=True)
        else:
            for _, row in health_df.iterrows():
                src_name = SOURCE_LABELS.get(row.source, row.source)
                st.markdown(f"<h4 style='margin-top:1.5rem; margin-bottom:0.5rem; font-family:Outfit;'>{src_name}</h4>", unsafe_allow_html=True)
                
                # Render the status banner
                if row.reliable_pct >= 95:
                    render_info_banner(f"Hệ thống hoạt động ổn định: Độ tin cậy đạt {row.reliable_pct:.1f}%" if lang == "vi" else f"System is stable: Reliability is {row.reliable_pct:.1f}%", type="success")
                elif row.reliable_pct >= 80:
                    render_info_banner(f"Hệ thống cảnh báo: Độ tin cậy đạt {row.reliable_pct:.1f}%" if lang == "vi" else f"System warning: Reliability is {row.reliable_pct:.1f}%", type="warning")
                else:
                    render_info_banner(f"Hệ thống gặp sự cố: Độ tin cậy chỉ đạt {row.reliable_pct:.1f}%" if lang == "vi" else f"System incident: Reliability is {row.reliable_pct:.1f}%", type="error")
                
                # Metric columns
                mc1, mc2, mc3, mc4 = st.columns(4)
                with mc1:
                    render_metric_card("Độ trễ API" if lang == "vi" else "API Lag", f"{row.latest_lag_hours:.1f}h", icon="schedule")
                with mc2:
                    render_metric_card("Độ trễ nạp (DB)" if lang == "vi" else "Ingestion Lag", f"{row.latest_ingest_lag_hours:.1f}h", icon="upload")
                with mc3:
                    render_metric_card("Số phường trễ" if lang == "vi" else "Stale Wards", f"{int(row.stale_count)}", icon="biotech")
                with mc4:
                    render_metric_card("Số phường ngoại tuyến" if lang == "vi" else "Offline Wards", f"{int(row.offline_count)}", icon="error")

if __name__ == "__main__":
    main()
