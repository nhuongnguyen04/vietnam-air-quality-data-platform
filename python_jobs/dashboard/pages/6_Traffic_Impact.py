"""
Traffic Impact page.
Analyzes vehicle congestion correlations with PM2.5 levels and ranks hotspots.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider, render_info_banner
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state

@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(dates, grain, scope, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"

    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg(avg_congestion) as avg_congestion,
        avg({target_col}) as avg_p,
        avg(traffic_coverage_ratio) as traffic_coverage_ratio,
        sum(traffic_ward_count) as traffic_ward_count
    FROM air_quality.dm_traffic_hourly_trend
    WHERE {where_clause}
    GROUP BY datetime_hour
    ORDER BY datetime_hour
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_stats(grain: str, scope: str | None = None, dates=None):
    where_clause = build_where_clause(grain, scope, dates)
    q = f"""
    SELECT
        avg(pm25_daily_avg) as avg_pm25,
        avg(congestion_daily_avg) as avg_congestion,
        avg(pm25_congestion_uplift) as avg_pm25_uplift,
        avg(traffic_pollution_impact_score) as avg_comovement_score,
        avg(avg_traffic_coverage_ratio) as avg_traffic_coverage_ratio,
        sum(total_hours) as observed_hours,
        sum(low_congestion_hours) as low_congestion_hours,
        sum(high_congestion_hours) as high_congestion_hours,
        countIf(pm25_congestion_uplift IS NOT NULL) as uplift_sample_days
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause}
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_ranking_data(grain: str, scope: str | None = None, dates=None, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"

    if grain in ["Tỉnh", "Phường"] and scope:
        ward_where_clause = build_where_clause(grain, scope, dates, date_col="t.date").replace("province =", "a.province =")
        q = f"""
        WITH ward_traffic AS (
            SELECT
                ward_code,
                toStartOfHour(timestamp_utc) as datetime_hour,
                toDate(timestamp_utc) as date,
                avg(value) as avg_congestion
            FROM air_quality.stg_tomtom__flow
            GROUP BY ward_code, datetime_hour, date
        )
        SELECT
            any(a.ward_name) as label_col,
            any(case
                when a.province IN ('Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
                when a.province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
                else 'Rural'
            end) as location_type,
            avg(h.{target_col} * t.avg_congestion) as impact_score
        FROM air_quality.dim_administrative_units a
        INNER JOIN ward_traffic t
            ON a.ward_code = t.ward_code
        INNER JOIN air_quality.dm_traffic_hourly_trend h
            ON a.province = h.province
            AND t.datetime_hour = h.datetime_hour
        WHERE {ward_where_clause}
            AND a.province != ''
            AND a.ward_code != ''
            AND h.{target_col} IS NOT NULL
            AND t.avg_congestion IS NOT NULL
            AND t.avg_congestion > 0
        GROUP BY a.ward_code
        ORDER BY impact_score DESC
        LIMIT 12
        """
        return query_df(q)

    q = f"""
    SELECT
        province as label_col,
        any(location_type) as location_type,
        avg({target_col}_daily_avg * congestion_daily_avg) as impact_score
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause} AND province != ''
    GROUP BY label_col
    ORDER BY impact_score DESC
    LIMIT 12
    """
    return query_df(q)

@page_wrapper("traffic", "🚦 Traffic Impact Analysis", icon="🚦")
def main(lang: str):
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]
    pollutant = filters.get("pollutant", "pm25")

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
            "Lưu ý: Các chỉ số thể hiện tương quan quan sát từ dữ liệu TomTom Flow và AQI.in, không phải quan hệ nhân quả trực tiếp."
            if lang == "vi" else
            "Note: Observational correlations between TomTom Flow traffic and AQI.in data, not direct causal estimates.",
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
        c1, c2, c3, c4 = st.columns(4)
        traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"
        uplift_display = f"{pm25_uplift:.1f} µg/m³" if has_uplift else "N/A"
        coverage_display = f"{avg_coverage:.1%}" if not pd.isna(avg_coverage) else "N/A"

        with c1:
            render_metric_card(t("traffic_congestion", lang), traffic_display, icon="traffic")
        with c2:
            render_metric_card(t("traffic_contribution", lang), uplift_display, icon="air")
        with c3:
            render_metric_card(t("traffic_impact", lang), f"{comovement_score:.2f}", icon="insights")
        with c4:
            render_metric_card("Độ phủ dữ liệu" if lang == "vi" else "TomTom Coverage", coverage_display, icon="location")

        render_section_divider()

        # ── Hourly Correlation & Hotspot Ranking (2-column layout) ─────────
        c_left, c_right = st.columns([1.0, 1.0], gap="large")
        
        with c_left:
            st.markdown(f"#### 📈 {t('traffic_hourly_correlation', lang)}")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Scatter(
                    x=df_hourly.datetime_hour, y=df_hourly.avg_congestion, 
                    name="Tắc nghẽn giao thông" if lang == "vi" else "TomTom Congestion Index",
                    line={"color": '#0891B2', "width": 3}
                ),
                secondary_y=True,
            )
            fig.add_trace(
                go.Scatter(
                    x=df_hourly.datetime_hour, y=df_hourly.avg_p, 
                    name=target_poll.upper(),
                    fill='tozeroy', line={"color": '#F59E0B', "width": 2}
                ),
                secondary_y=False,
            )
            fig.update_layout(
                get_plotly_layout(height=320, compact=True), 
                margin={"l": 50, "r": 65, "t": 40, "b": 40}, 
                hovermode="x unified"
            )
            fig.update_yaxes(title_text=f"{target_poll.upper()} (µg/m³)", secondary_y=False)
            fig.update_yaxes(title_text="Tắc nghẽn" if lang == "vi" else "Congestion", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)
 
        with c_right:
            st.markdown(f"#### 🏆 {t('traffic_hotspot_ranking', lang)}")
            if not df_rank.empty:
                is_ward_ranking = spatial_grain in ["Tỉnh", "Phường"] and bool(scope_val)
                area_label = "Phường/Xã" if is_ward_ranking and lang == "vi" else ("Ward" if is_ward_ranking else t("chart_label_area", lang))
                
                df_rank_plot = df_rank.sort_values('impact_score', ascending=True)
 
                loc_map = {
                    "Urban": t("location_urban", lang),
                    "Industrial": t("location_industrial", lang),
                    "Rural": t("location_rural", lang)
                }
                df_rank_plot["loc_label"] = df_rank_plot["location_type"].map(loc_map).fillna(df_rank_plot["location_type"])
 
                color_map = {
                    t("location_urban", lang): "#0891B2",      # Cyan
                    t("location_industrial", lang): "#EF4444", # Red
                    t("location_rural", lang): "#10B981"       # Emerald
                }
 
                fig_rank = px.bar(
                    df_rank_plot,
                    x="impact_score",
                    y="label_col",
                    color="loc_label",
                    orientation='h',
                    labels={"impact_score": t("traffic_impact", lang), "label_col": area_label, "loc_label": t("chart_label_type", lang)},
                    color_discrete_map=color_map,
                )
                fig_rank.update_layout(
                    get_plotly_layout(height=320, compact=True),
                    margin={"l": 80, "r": 20, "t": 40, "b": 45}
                )
                st.plotly_chart(fig_rank, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu xếp hạng giao thông cho vùng này.", height=320), use_container_width=True)
    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu KPI/xu hướng giao thông cho vùng này."), use_container_width=True)

if __name__ == "__main__":
    main()
