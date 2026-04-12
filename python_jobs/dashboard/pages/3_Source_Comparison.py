"""Source Comparison page — AQI.in vs OpenWeather comparison."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import EPA_COLORS

st.set_page_config(title="So sánh nguồn", page_icon="🔗", layout="wide")

st.title("🔗 So sánh Nguồn dữ liệu")
st.caption("Độ tươi dữ liệu: AQI.in ~15 phút | OpenWeather ~60 phút")

# ── helpers ─────────────────────────────────────────────────────────────────────

SOURCE_LABELS = {"aqiin": "AQI.in", "openweather": "OpenWeather"}
SOURCE_COLORS = {"aqiin": "#00A8E8", "openweather": "#FF7E00"}


@st.cache_data(ttl=3600)
def get_provinces():
    q = """
    SELECT DISTINCT province
    FROM air_quality.dm_aqi_current_status
    WHERE province IS NOT NULL AND province != ''
    ORDER BY province
    """
    return query_df(q)["province"].tolist()


@st.cache_data(ttl=300)
def get_source_trend(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        date,
        source,
        round(avg(avg_aqi_us), 1) AS avg_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      AND source IN ('aqiin', 'openweather')
      {where_clause}
    GROUP BY date, source
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_distribution(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        source,
        avg_aqi_us AS avg_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      AND source IN ('aqiin', 'openweather')
      {where_clause}
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_stats(days: int):
    q = f"""
    SELECT
        source,
        round(avg(avg_aqi_us), 1)            AS avg_aqi,
        round(max(max_aqi_us), 0)             AS max_aqi,
        round(min(avg_aqi_us), 1)             AS min_aqi,
        count(*)                              AS day_count,
        sum(if(avg_aqi_us <= 50, 1, 0))       AS good_days
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
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


def render_empty_fig(message: str, height: int = 280):
    fig = px.bar()
    fig.update_layout(
        height=height,
        xaxis=dict(visible=False, showgrid=False),
        yaxis=dict(visible=False, showgrid=False),
        annotations=[dict(
            text=f"<b>{message}</b>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color="#9CA3AF"),
            xref="paper", yref="paper",
        )],
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=0, r=0, t=10, b=10),
    )
    return fig


# ── page body ─────────────────────────────────────────────────────────────────────

try:
    provinces = get_provinces()
    col_filter1, col_filter2 = st.columns([2, 1])
    with col_filter1:
        selected_province = st.selectbox(
            "Chọn tỉnh/thành phố",
            options=["Toàn quốc"] + provinces,
            index=0,
        )
    with col_filter2:
        TIME_OPTIONS = {7: "7 ngày", 30: "30 ngày", 90: "3 tháng", 365: "1 năm"}
        days = st.selectbox(
            "Khoảng thời gian",
            options=list(TIME_OPTIONS.keys()),
            format_func=lambda x: TIME_OPTIONS[x],
            index=1,
        )

    province_arg = selected_province if selected_province != "Toàn quốc" else None

    # ── source trend (full width) ────────────────────────────────────────────────
    st.subheader(f"AQI theo nguồn — {TIME_OPTIONS[days]}")
    with st.spinner("Đang tải so sánh nguồn..."):
        trend = get_source_trend(days, province_arg)
    if not trend.empty:
        fig = px.line(
            trend,
            x="date",
            y="avg_aqi",
            color="source",
            color_discrete_map=SOURCE_COLORS,
            labels={
                "date": "Ngày",
                "avg_aqi": "AQI US",
                "source": "Nguồn",
            },
            custom_data=["source"],
        )
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_fig("Không có dữ liệu so sánh nguồn trong khoảng thời gian đã chọn.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("fct_air_quality_summary_daily chưa có dữ liệu nguồn. Chạy dbt transform.")

    # ── box plot + stats table (col2) ──────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Phân bố AQI theo nguồn")
        dist = get_source_distribution(days, province_arg)
        if not dist.empty:
            fig = px.box(
                dist,
                x="source",
                y="avg_aqi",
                color="source",
                color_discrete_map=SOURCE_COLORS,
                labels={"source": "Nguồn", "avg_aqi": "AQI US"},
                custom_data=["source"],
            )
            fig.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu phân bố nguồn.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Chưa có đủ dữ liệu phân bố trong khoảng thời gian đã chọn.")

    with col_right:
        st.subheader("Thống kê theo nguồn")
        stats = get_source_stats(days)
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
            st.info("Chưa có thống kê nguồn trong khoảng thời gian đã chọn.")

    # ── data freshness ──────────────────────────────────────────────────────────
    st.subheader("Độ tươi dữ liệu theo nguồn")
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
            labels={"lag_hours": "Độ trễ (giờ)", "health_status": "Trạng thái"},
        )
        fig.update_layout(height=260, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Nguồn Fresh (≤1h) | Delayed (≤3h) | Stale (≤24h) | Offline (>24h)")
    else:
        fig = render_empty_fig("Không có dữ liệu độ tươi nguồn.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("dm_platform_data_health chưa có dữ liệu nguồn.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
