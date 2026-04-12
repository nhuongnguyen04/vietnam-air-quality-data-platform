"""Pollutants page — pollutant trends, source fingerprint, compliance, heatmap."""
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

st.set_page_config(title="Chất ô nhiễm", page_icon="🧪", layout="wide")

st.title("🧪 Phân tích Chất ô nhiễm")

# ── helpers ─────────────────────────────────────────────────────────────────────

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
def get_pollutant_trend(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        date,
        round(avg(pm25_avg), 1)   AS pm25_avg,
        round(avg(pm10_avg), 1)   AS pm10_avg,
        round(avg(co_avg), 3)     AS co_avg,
        round(avg(no2_avg), 2)    AS no2_avg,
        round(avg(so2_avg), 2)    AS so2_avg,
        round(avg(o3_avg), 1)     AS o3_avg
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_source_fingerprint(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        province,
        probable_source,
        count(*) AS cnt,
        round(avg(pm25_avg), 1) AS pm25_avg
    FROM air_quality.dm_pollutant_source_fingerprint
    WHERE date >= today() - INTERVAL {days} DAY
      {where_clause}
    GROUP BY province, probable_source
    ORDER BY province, cnt DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_compliance_status(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        province,
        compliance_status,
        count(*) AS cnt
    FROM air_quality.dm_aqi_compliance_standards
    WHERE date >= today() - INTERVAL {days} DAY
      {where_clause}
    GROUP BY province, compliance_status
    ORDER BY province, cnt DESC
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_pollutant_heatmap(days: int):
    q = f"""
    SELECT
        province,
        toString(date) AS date_str,
        round(avg(pm25_avg), 1) AS pm25_avg
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      AND province IS NOT NULL AND province != ''
    GROUP BY province, date
    ORDER BY province, date
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

    # ── pollutant trend (full width) ────────────────────────────────────────────
    st.subheader(f"Nồng độ chất ô nhiễm theo thời gian — {TIME_OPTIONS[days]}")
    with st.spinner("Đang tải dữ liệu pollutants..."):
        trend = get_pollutant_trend(days, province_arg)
    if not trend.empty:
        pollutants = ["pm25_avg", "pm10_avg", "o3_avg", "no2_avg", "co_avg", "so2_avg"]
        fig = px.line(
            trend,
            x="date",
            y=pollutants,
            labels={
                "date": "Ngày",
                "value": "Nồng độ",
                "variable": "Chất ô nhiễm",
                "pm25_avg": "PM2.5 (µg/m³)",
                "pm10_avg": "PM10 (µg/m³)",
                "o3_avg": "O₃ (µg/m³)",
                "no2_avg": "NO₂ (µg/m³)",
                "co_avg": "CO (mg/m³)",
                "so2_avg": "SO₂ (µg/m³)",
            },
        )
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_fig("Không có dữ liệu pollutant trong khoảng thời gian đã chọn.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Chạy dbt transform để cập nhật fct_air_quality_summary_daily.")

    # ── compliance + fingerprint (col2) ────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Tuân thủ tiêu chuẩn")
        compliance = get_compliance_status(days, province_arg)
        if not compliance.empty:
            COMPLIANCE_COLORS = {
                "Good/Safe": "#00E400",
                "Warning (WHO Breach)": "#FF7E00",
                "Unhealthy (TCVN Breach)": "#FF0000",
            }
            fig = px.bar(
                compliance,
                x="province",
                y="cnt",
                color="compliance_status",
                color_discrete_map=COMPLIANCE_COLORS,
                barmode="group",
            )
            fig.update_layout(height=300, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu tuân thủ tiêu chuẩn.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("dm_aqi_compliance_standards chưa có dữ liệu.")

    with col_right:
        st.subheader("Nguồn ô nhiễm (fingerprint)")
        fingerprint = get_source_fingerprint(days, province_arg)
        if not fingerprint.empty:
            FINGERPRINT_COLORS = {
                "Combustion/Traffic": "#FF7E00",
                "Dust/Construction": "#8F3F97",
                "Mixed": "#FF0000",
            }
            fig = px.bar(
                fingerprint,
                x="province",
                y="cnt",
                color="probable_source",
                color_discrete_map=FINGERPRINT_COLORS,
                barmode="stack",
            )
            fig.update_layout(height=300, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu nguồn ô nhiễm.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("dm_pollutant_source_fingerprint chưa có dữ liệu.")

    # ── heatmap (province × date) ───────────────────────────────────────────────
    st.subheader("Bản đồ nhiệt PM2.5 — tỉnh × ngày")
    heatmap_data = get_pollutant_heatmap(days)
    if not heatmap_data.empty:
        top_provinces = heatmap_data.groupby("province")["pm25_avg"].mean().nlargest(15).index.tolist()
        filtered = heatmap_data[heatmap_data["province"].isin(top_provinces)]
        fig = px.density_heatmap(
            filtered,
            x="date_str",
            y="province",
            z="pm25_avg",
            color_continuous_scale=get_epa_continuous_scale(),
            labels={"date_str": "Ngày", "province": "Tỉnh", "pm25_avg": "PM2.5 µg/m³"},
        )
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_fig("Không có dữ liệu heatmap PM2.5.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Chưa có dữ liệu tổng hợp theo ngày cho bản đồ nhiệt.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
