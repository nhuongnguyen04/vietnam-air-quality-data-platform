"""Source Comparison page — AQI.in vs OpenWeather comparison."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import EPA_COLORS, render_empty_chart
from lib.filters import render_sidebar_filters
from lib.data_service import build_where_clause
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("source_comparison_title", lang))
st.caption(t("source_comparison_caption", lang))

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters["pollutant"]
standard = filters["standard"]

# ── helpers ─────────────────────────────────────────────────────────────────────

SOURCE_LABELS = {"aqiin": "AQI.in", "openweather": "OpenWeather"}
SOURCE_COLORS = {"aqiin": "#00A8E8", "openweather": "#FF7E00"}


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
        lag_hours,
        health_status
    FROM air_quality.dm_platform_data_health
    WHERE source IN ('aqiin', 'openweather')
    ORDER BY source
    """
    return query_df(q)


# ── page body ─────────────────────────────────────────────────────────────────────

try:
    # ── source trend (full width) ────────────────────────────────────────────────
    st.subheader(t("chart_aqi_by_source", lang))
    with st.spinner(t("loading", lang) if lang=="en" else "Đang tải so sánh nguồn..."):
        trend = get_source_trend(date_range, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)
    if not trend.empty:
        fig = px.line(
            trend,
            x="date",
            y="avg_aqi",
            color="source",
            color_discrete_map=SOURCE_COLORS,
            labels={
                "date": t("chart_label_date", lang),
                "avg_aqi": "AQI US",
                "source": t("nav_status", lang), # "Nguồn" or "Source" is not exactly nav_status but let's use status or status_label
            },
            custom_data=["source"],
        )
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu so sánh nguồn."), use_container_width=True)

    # ── box plot + stats table (col2) ──────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(t("chart_aqi_distribution_by_source", lang))
        dist = get_source_distribution(date_range, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)
        if not dist.empty:
            fig = px.box(
                dist,
                x="source",
                y="avg_aqi",
                color="source",
                color_discrete_map=SOURCE_COLORS,
                labels={"source": t("nav_status", lang) if lang=="en" else "Nguồn", "avg_aqi": "AQI US"},
                custom_data=["source"],
            )
            fig.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu phân bố."), use_container_width=True)

    with col_right:
        st.subheader(t("chart_aqi_stats_by_source", lang))
        stats = get_source_stats(date_range)
        if not stats.empty:
            stats_display = stats.copy()
            stats_display["Nguồn"] = stats_display["source"].map(SOURCE_LABELS)
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
                use_container_width=True,
            )
        else:
            st.info(t("no_data", lang) if lang=="en" else "Chưa có thống kê nguồn trong khoảng thời gian đã chọn.")

    # ── data freshness ──────────────────────────────────────────────────────────
    st.subheader(t("chart_data_freshness_by_source", lang))
    freshness = get_data_freshness()
    if not freshness.empty:
        freshness = freshness.copy()
        freshness["Nguồn"] = freshness["source"].map(SOURCE_LABELS)
        FRESHNESS_COLORS = {
            "Fresh": "#00E400",
            "Delayed": "#FFFF00",
            "Stale": "#FF7E00",
            "Offline": "#FF0000",
        }
        fig = px.bar(
            freshness,
            x="Nguồn",
            y="lag_hours",
            color="health_status",
            color_discrete_map=FRESHNESS_COLORS,
            text="lag_hours",
            labels={"lag_hours": t("chart_label_hour", lang), "health_status": t("chart_label_status", lang)},
        )
        fig.update_layout(height=260, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Nguồn Fresh (≤1h) | Delayed (≤3h) | Stale (≤24h) | Offline (>24h)")
    else:
        st.plotly_chart(render_empty_chart(t("no_data", lang) if lang=="en" else "Không có dữ liệu độ tươi."), use_container_width=True)

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
