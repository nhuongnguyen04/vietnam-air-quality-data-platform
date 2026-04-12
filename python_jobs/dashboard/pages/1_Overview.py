"""Overview page — national KPI cards + 3 mini-charts."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import (
    EPA_COLORS,
    get_epa_color_for_value,
    get_epa_continuous_scale,
)

st.set_page_config(title="Tổng quan", page_icon="📊", layout="wide")

st.title("📊 Tổng quan Chất lượng Không khí Việt Nam")


# ── helpers ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_provinces():
    """Dynamic province list from ClickHouse."""
    q = """
    SELECT DISTINCT province
    FROM air_quality.dm_aqi_current_status
    WHERE province IS NOT NULL AND province != ''
    ORDER BY province
    """
    df = query_df(q)
    return df["province"].tolist() if not df.empty else []


@st.cache_data(ttl=300)
def get_national_kpi():
    """National AQI overview KPIs."""
    q = """
    SELECT
        round(avg(current_aqi_us), 0)                         AS national_avg,
        round(max(current_aqi_us), 0)                         AS worst_aqi,
        argMax(main_pollutant, current_aqi_us)                 AS dominant_pollutant,
        min(ingest_time)                                       AS latest_ingest
    FROM air_quality.dm_aqi_current_status
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_province_aqi():
    """Province-level current AQI, sorted worst-first."""
    q = """
    SELECT
        province,
        current_aqi_us,
        current_aqi_vn,
        pm25,
        pm10,
        main_pollutant
    FROM air_quality.dm_aqi_current_status
    ORDER BY current_aqi_us DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_aqi_distribution():
    """AQI category distribution — donut chart source."""
    q = """
    SELECT
        CASE
            WHEN current_aqi_us <= 50  THEN 'Good'
            WHEN current_aqi_us <= 100 THEN 'Moderate'
            WHEN current_aqi_us <= 150 THEN 'USG'
            WHEN current_aqi_us <= 200 THEN 'Unhealthy'
            WHEN current_aqi_us <= 300 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END AS aqi_category,
        count(*) AS cnt
    FROM air_quality.dm_aqi_current_status
    GROUP BY aqi_category
    ORDER BY cnt DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_comparison():
    """AQI.in vs OpenWeather comparison (30d avg)."""
    q = """
    SELECT
        source,
        round(avg(avg_aqi_us), 1) AS avg_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL 30 DAY
      AND source IN ('aqiin', 'openweather')
    GROUP BY source
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_health_risk_stats():
    """Health risk hours summary (last 30 days)."""
    q = """
    SELECT
        sum(high_risk_hours)           AS total_risk_hours,
        countIf(high_risk_hours > 0)    AS bad_days
    FROM air_quality.dm_aqi_health_impact_summary
    WHERE date >= today() - INTERVAL 30 DAY
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_data_freshness():
    """Platform data health summary."""
    q = """
    SELECT
        lag_hours,
        health_status
    FROM air_quality.dm_platform_data_health
    LIMIT 10
    """
    return query_df(q)


# ── layout helpers ─────────────────────────────────────────────────────────────

def render_empty_fig(message: str, height: int = 250):
    """Return an empty Plotly figure with a centered annotation."""
    fig = px.bar()
    fig.update_layout(
        height=height,
        xaxis=dict(visible=False, showgrid=False),
        yaxis=dict(visible=False, showgrid=False),
        annotations=[
            dict(
                text=f"<b>{message}</b>",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=13, color="#9CA3AF"),
                xref="paper", yref="paper",
            )
        ],
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=0, r=0, t=10, b=10),
    )
    return fig


# ── page body ─────────────────────────────────────────────────────────────────

try:
    # ── province filter ────────────────────────────────────────────────────────
    with st.spinner("Đang tải danh sách tỉnh/thành phố..."):
        provinces = get_provinces()

    selected_province = st.selectbox(
        "Chọn tỉnh/thành phố",
        options=["Toàn quốc"] + provinces,
        index=0,
        help="Lọc theo tỉnh/thành phố. 'Toàn quốc' = tất cả.",
    )

    # ── row 1: KPI cards ──────────────────────────────────────────────────────
    with st.spinner("Đang tải KPI..."):
        kpi = get_national_kpi()
        health = get_health_risk_stats()
        freshness = get_data_freshness()

    col1, col2, col3, col4 = st.columns(4)

    if not kpi.empty:
        row = kpi.iloc[0]
        col1.metric("AQI Quốc gia", f"{int(row.national_avg)}")
        col2.metric("Chỉ số nổi bật", row.dominant_pollutant.upper() if row.dominant_pollutant else "—")
    else:
        col1.metric("AQI Quốc gia", "—")
        col2.metric("Chỉ số nổi bật", "—")

    if not health.empty:
        hrow = health.iloc[0]
        delta_val = int(hrow.total_risk_hours)
        delta_color = "inverse" if delta_val > 0 else "normal"
        col3.metric(
            "Giờ rủi ro cao",
            f"{delta_val}h",
            delta=f"{int(hrow.bad_days)} ngày cao",
            delta_color=delta_color,
        )
    else:
        col3.metric("Giờ rủi ro cao", "—")

    if not freshness.empty:
        frow = freshness.iloc[0]
        col4.metric("Độ tươi dữ liệu", f"{int(frow.lag_hours)}h", delta=frow.health_status)
    else:
        col4.metric("Độ tươi dữ liệu", "—")

    st.divider()

    # ── row 2: mini-charts ──────────────────────────────────────────────────────
    col_chart1, col_chart2, col_chart3 = st.columns(3)

    with col_chart1:
        st.subheader("Phân bố AQI theo danh mục")
        dist = get_aqi_distribution()
        if not dist.empty:
            fig = px.pie(
                dist,
                names="aqi_category",
                values="cnt",
                color="aqi_category",
                color_discrete_map=EPA_COLORS,
                hole=0.5,
            )
            fig.update_layout(
                height=280,
                showlegend=True,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            fig.update_traces(textinfo="label+percent")
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu phân bố AQI")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Chưa có dữ liệu AQI — chạy dbt transform để cập nhật.")

    with col_chart2:
        st.subheader("So sánh nguồn dữ liệu (30d)")
        src = get_source_comparison()
        if not src.empty:
            fig = px.bar(
                src,
                x="source",
                y="avg_aqi",
                color="source",
                color_discrete_map={"aqiin": "#00A8E8", "openweather": "#FF7E00"},
                text="avg_aqi",
            )
            fig.update_layout(height=280, showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu so sánh nguồn")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Chưa có dữ liệu nguồn trong 30 ngày qua.")

    with col_chart3:
        st.subheader("Top 5 tỉnh có AQI cao nhất")
        prov = get_province_aqi()
        if not prov.empty:
            top5 = prov.head(5)
            fig = px.bar(
                top5,
                y="province",
                x="current_aqi_us",
                orientation="h",
                color="current_aqi_us",
                color_continuous_scale=get_epa_continuous_scale(),
                text="current_aqi_us",
            )
            fig.update_layout(
                height=280,
                xaxis_title="AQI US",
                yaxis_title=None,
                showlegend=False,
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False,
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu tỉnh")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Chưa có dữ liệu tỉnh trong hệ thống.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse đang chạy và dbt transform đã hoàn thành.")
