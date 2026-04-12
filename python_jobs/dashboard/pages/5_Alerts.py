"""Alerts page — compliance timeline, WHO/TCVN breaches, high-risk heatmap, platform health."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import get_epa_continuous_scale

st.title("🚨 Cảnh báo & Tuân thủ Tiêu chuẩn")
st.caption("Dữ liệu dựa trên WHO (2021) và TCVN 05:2023-BKHCN daily standards.")

# ── constants ─────────────────────────────────────────────────────────────────

COMPLIANCE_COLORS = {
    "Good/Safe": "#00E400",
    "Warning (WHO Breach)": "#FF7E00",
    "Unhealthy (TCVN Breach)": "#FF0000",
}

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
def get_compliance_timeline(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        date,
        compliance_status,
        count(*) AS cnt
    FROM air_quality.dm_aqi_compliance_standards
    WHERE date >= today() - INTERVAL {days} DAY
      {where_clause}
    GROUP BY date, compliance_status
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_breach_by_province(days: int):
    q = f"""
    SELECT
        province,
        sum(who_pm25_breach)   AS who_breach_days,
        sum(tcvn_pm25_breach)  AS tcvn_breach_days
    FROM air_quality.dm_aqi_compliance_standards
    WHERE date >= today() - INTERVAL {days} DAY
      AND province IS NOT NULL AND province != ''
    GROUP BY province
    ORDER BY (who_breach_days + tcvn_breach_days) DESC
    LIMIT 20
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_high_risk_heatmap(days: int, province: str | None):
    if province and province != "Toàn quốc":
        where_clause = f"AND province = '{province}'"
    else:
        where_clause = ""
    q = f"""
    SELECT
        province,
        toString(date)               AS date_str,
        high_risk_hours
    FROM air_quality.dm_aqi_health_impact_summary
    WHERE date >= today() - INTERVAL {days} DAY
      AND province IS NOT NULL AND province != ''
      {where_clause}
    ORDER BY province, date
    """
    return query_df(q)


@st.cache_data(ttl=60)
def get_platform_health():
    q = """
    SELECT
        province,
        source,
        lag_hours,
        health_status
    FROM air_quality.dm_platform_data_health
    ORDER BY province, source
    LIMIT 50
    """
    return query_df(q)


def render_empty_fig(message: str, height: int = 260):
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
            index=0,   # default: 7 days (alerts = recent focus)
        )

    province_arg = selected_province if selected_province != "Toàn quốc" else None

    # ── compliance timeline ────────────────────────────────────────────────────
    st.subheader(f"Timeline tuân thủ tiêu chuẩn — {TIME_OPTIONS[days]}")
    with st.spinner("Đang tải timeline tuân thủ..."):
        timeline = get_compliance_timeline(days, province_arg)
    if not timeline.empty:
        fig = px.bar(
            timeline,
            x="date",
            y="cnt",
            color="compliance_status",
            color_discrete_map=COMPLIANCE_COLORS,
            barmode="stack",
            labels={"date": "Ngày", "cnt": "Số ngày", "compliance_status": "Trạng thái"},
        )
        fig.update_layout(height=280, showlegend=True, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_fig("Không có dữ liệu tuân thủ. Mức AQI đang ở ngưỡng an toàn.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("dm_aqi_compliance_standards chưa có dữ liệu trong khoảng thời gian này.")

    # ── WHO/TCVN breach bars ───────────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Vi phạm WHO vs TCVN theo tỉnh")
        breach = get_breach_by_province(days)
        if not breach.empty:
            # Melt to long form for grouped bar
            breach_melted = pd.melt(
                breach,
                id_vars=["province"],
                value_vars=["who_breach_days", "tcvn_breach_days"],
                var_name="standard",
                value_name="days",
            )
            breach_melted["standard"] = breach_melted["standard"].map({
                "who_breach_days": "WHO (PM2.5>15µg)",
                "tcvn_breach_days": "TCVN (PM2.5>50µg)",
            })
            fig = px.bar(
                breach_melted,
                x="province",
                y="days",
                color="standard",
                barmode="group",
                color_discrete_map={
                    "WHO (PM2.5>15µg)": "#FF7E00",
                    "TCVN (PM2.5>50µg)": "#FF0000",
                },
                labels={"province": "Tỉnh", "days": "Ngày vi phạm"},
            )
            fig.update_layout(height=300, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_fig("Không có dữ liệu vi phạm tiêu chuẩn.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Không có vi phạm WHO/TCVN trong khoảng thời gian đã chọn.")

    with col_right:
        st.subheader("Độ tươi dữ liệu hệ thống")
        health = get_platform_health()
        if not health.empty:
            HEALTH_COLORS = {
                "Fresh": "#00E400",
                "Delayed": "#FFFF00",
                "Stale": "#FF7E00",
                "Offline": "#FF0000",
            }
            fig = px.bar(
                health,
                x="province",
                y="lag_hours",
                color="health_status",
                color_discrete_map=HEALTH_COLORS,
                text="lag_hours",
                labels={"province": "Tỉnh", "lag_hours": "Độ trễ (giờ)", "health_status": "Trạng thái"},
                custom_data=["source"],
            )
            fig.update_layout(height=300, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Fresh≤1h | Delayed≤3h | Stale≤24h | Offline>24h")
        else:
            fig = render_empty_fig("Không có dữ liệu sức khỏe hệ thống.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("dm_platform_data_health chưa có dữ liệu.")

    # ── high-risk heatmap ─────────────────────────────────────────────────────
    st.subheader("Giờ rủi ro cao — tỉnh × ngày")
    risk = get_high_risk_heatmap(days, province_arg)
    if not risk.empty:
        # Limit to top provinces by total high-risk hours
        top_provs = (
            risk.groupby("province")["high_risk_hours"]
            .sum()
            .nlargest(15)
            .index.tolist()
        )
        filtered = risk[risk["province"].isin(top_provs)]
        fig = px.density_heatmap(
            filtered,
            x="date_str",
            y="province",
            z="high_risk_hours",
            color_continuous_scale=[
                [0.0, "#00E400"],
                [0.25, "#FFFF00"],
                [0.5, "#FF7E00"],
                [0.75, "#FF0000"],
                [1.0, "#7E0023"],
            ],
            labels={
                "date_str": "Ngày",
                "province": "Tỉnh",
                "high_risk_hours": "Giờ rủi ro cao",
            },
        )
        fig.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_fig("Không có giờ rủi ro cao. Chất lượng không khí đang tốt.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("dm_aqi_health_impact_summary chưa có dữ liệu trong khoảng thời gian này.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")