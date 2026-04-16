"""Historical Trend page — daily/monthly AQI trends, province comparison, heatmap."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart

st.title("📈 Xu hướng Chất lượng Không khí — Lịch sử")

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
def get_national_daily_trend(days: int):
    q = f"""
    SELECT
        date,
        round(avg(avg_aqi_us), 1)  AS avg_aqi,
        round(max(max_aqi_us), 0)  AS max_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_province_daily_trend(days: int, province: str):
    q = f"""
    SELECT
        date,
        round(avg(avg_aqi_us), 1)  AS avg_aqi,
        round(max(max_aqi_us), 0)  AS max_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      AND province = '{province}'
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_monthly_trend(days: int):
    months_back = max(1, days // 30)
    q = f"""
    SELECT
        month,
        round(avg(avg_aqi_us), 1)  AS avg_aqi,
        round(max(max_aqi_us), 0)  AS max_aqi
    FROM air_quality.fct_air_quality_summary_monthly
    WHERE month >= today() - INTERVAL {months_back} MONTH
    GROUP BY month
    ORDER BY month
    """
    df = query_df(q)
    if not df.empty:
        return df, "monthly"
    # Fallback: aggregate daily data by month
    fallback_q = f"""
    SELECT
        toStartOfMonth(date) AS month,
        round(avg(avg_aqi_us), 1)  AS avg_aqi,
        round(max(max_aqi_us), 0)  AS max_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
    GROUP BY month
    ORDER BY month
    """
    df = query_df(fallback_q)
    return df, "daily_fallback"


@st.cache_data(ttl=300)
def get_heatmap_data(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        province,
        toString(date)           AS date_str,
        round(avg(pm25_avg), 1) AS pm25_avg
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
      AND province IS NOT NULL AND province != ''
      {where_clause}
    GROUP BY province, date
    ORDER BY province, date
    """
    return query_df(q)

@st.cache_data(ttl=3600)
def get_temporal_patterns(province: str | None = None):
    where_clause = ""
    if province:
        where_clause = f"WHERE province = '{province}'"
    
    q = f"""
    SELECT
        hour_of_day,
        day_of_week,
        avg(avg_aqi_us) as avg_aqi
    FROM air_quality.dm_aqi_temporal_patterns
    {where_clause}
    GROUP BY hour_of_day, day_of_week
    ORDER BY day_of_week, hour_of_day
    """
    df = query_df(q)
    if not df.empty:
        # Map day numbers to names
        day_map = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
        df["day_name"] = df["day_of_week"].map(day_map)
        # Ensure correct ordering in plot
        df["day_name"] = pd.Categorical(df["day_name"], categories=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], ordered=True)
    return df

@st.cache_data(ttl=300)
def get_overall_stats(days: int):
    q = f"""
    SELECT
        count(distinct date)            AS total_days,
        round(avg(avg_aqi_us), 1)      AS overall_avg,
        round(min(avg_aqi_us), 1)      AS overall_min,
        round(max(max_aqi_us), 0)      AS overall_max
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= today() - INTERVAL {days} DAY
    """
    return query_df(q)


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

    # ── KPI stats row ─────────────────────────────────────────────────────────
    with st.spinner("Đang tải thống kê..."):
        stats = get_overall_stats(days)
    if not stats.empty:
        row = stats.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Ngày theo dõi", f"{int(row.total_days)} ngày")
        col2.metric(f"Avg AQI ({TIME_OPTIONS[days]})", f"{row.overall_avg:.0f}")
        col3.metric("AQI tối thiểu", f"{row.overall_min:.0f}")
        col4.metric("AQI tối đa", f"{row.overall_max:.0f}")

    # ── national daily trend ───────────────────────────────────────────────────
    st.subheader(f"AQI quốc gia — {TIME_OPTIONS[days]}")
    national = get_national_daily_trend(days)
    if not national.empty:
        fig = px.line(
            national,
            x="date",
            y=["avg_aqi", "max_aqi"],
            labels={"date": "Ngày", "value": "AQI", "variable": "Chỉ số"},
            color_discrete_map={"avg_aqi": "#00A8E8", "max_aqi": "#FF0000"},
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_chart("Không có dữ liệu xu hướng quốc gia.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("fct_air_quality_summary_daily chưa có dữ liệu. Chạy dbt transform.")

    # ── Temporal Patterns Heatmap (New) ──────────────────────────────────────
    st.markdown("---")
    st.subheader("Phân tích theo thời gian (Diurnal Patterns)")
    with st.spinner("Đang tải bản đồ nhiệt thời gian..."):
        df_temporal = get_temporal_patterns(province_arg)
    
    if not df_temporal.empty:
        fig_temp = px.density_heatmap(
            df_temporal,
            x="hour_of_day",
            y="day_name",
            z="avg_aqi",
            color_continuous_scale=get_epa_continuous_scale(),
            labels={"hour_of_day": "Giờ trong ngày", "day_name": "Thứ", "avg_aqi": "AQI US"},
            category_orders={"day_name": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}
        )
        fig_temp.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig_temp, use_container_width=True)
        st.info("Bản đồ nhiệt cho thấy các khung giờ ô nhiễm cao điểm (ví dụ: giờ đi làm/về làm).")
    else:
        st.caption("Chưa có dữ liệu temporal patterns.")

    # ── province daily trend (khi chọn tỉnh) ─────────────────────────────────
    if province_arg:
        st.markdown("---")
        st.subheader(f"AQI {province_arg} — {TIME_OPTIONS[days]}")
        prov_trend = get_province_daily_trend(days, province_arg)
        if not prov_trend.empty:
            fig = px.line(
                prov_trend,
                x="date",
                y=["avg_aqi", "max_aqi"],
                labels={"date": "Ngày", "value": "AQI", "variable": "Chỉ số"},
                color_discrete_map={"avg_aqi": "#00A8E8", "max_aqi": "#FF0000"},
            )
            fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=40))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_chart(f"Không có dữ liệu cho {province_arg}.")
            st.plotly_chart(fig, use_container_width=True)

    # ── monthly trend ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Xu hướng hàng tháng")
    monthly_df, monthly_source = get_monthly_trend(days)
    if not monthly_df.empty:
        if monthly_source == "daily_fallback":
            st.info("Dữ liệu monthly chưa có sẵn — hiển thị tổng hợp từ daily data.")
        fig = px.bar(
            monthly_df,
            x="month",
            y="avg_aqi",
            text="avg_aqi",
            labels={"month": "Tháng", "avg_aqi": "AQI US trung bình"},
            color="avg_aqi",
            color_continuous_scale=get_epa_continuous_scale(),
        )
        fig.update_layout(height=280, showlegend=False, margin=dict(l=0, r=0, t=10, b=40))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── heatmap ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Bản đồ nhiệt PM2.5 — tỉnh × ngày")
    heatmap_data = get_heatmap_data(days, province_arg)
    if not heatmap_data.empty:
        top_provs = (
            heatmap_data.groupby("province")["pm25_avg"]
            .mean()
            .nlargest(15)
            .index.tolist()
        )
        filtered = heatmap_data[heatmap_data["province"].isin(top_provs)]
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

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
