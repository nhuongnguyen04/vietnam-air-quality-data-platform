"""Alerts page — compliance timeline, WHO/TCVN breaches, high-risk heatmap, platform health."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

"""
Trang Cảnh báo (Alerts) theo dõi và thông báo về các sự kiện ô nhiễm vượt ngưỡng quy định. 
Hệ thống cung cấp lịch sử các đợt cảnh báo và chi tiết về mức độ rủi ro tại từng khu vực.
"""
import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(f"🚨 {t('alert_title', lang)}")
st.caption(t("alert_caption", lang))

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
        source,
        reliable_pct,
        latest_lag_hours,
        stale_count,
        offline_count,
        attention_count
    FROM air_quality.dm_platform_source_health
    ORDER BY source
    """
    return query_df(q)


# ── page body ─────────────────────────────────────────────────────────────────────

try:
    provinces = get_provinces()
    col_filter1, col_filter2 = st.columns([2, 1])
    with col_filter1:
        selected_province = st.selectbox(
            t("filter_province_select", lang),
            options=["Toàn quốc"] + provinces,
            index=0,
        )
    with col_filter2:
        TIME_OPTIONS = {
            7: t("time_7days", lang),
            30: t("time_30days", lang),
            90: t("time_3months", lang),
            365: t("time_1year", lang)
        }
        days = st.selectbox(
            t("filter_time_range", lang),
            options=list(TIME_OPTIONS.keys()),
            format_func=lambda x: TIME_OPTIONS[x],
            index=0,   # default: 7 days (alerts = recent focus)
        )

    province_arg = selected_province if selected_province != "Toàn quốc" else None

    # ── compliance timeline ────────────────────────────────────────────────────
    st.subheader(f"{t('chart_compliance_timeline', lang)} — {TIME_OPTIONS[days]}")
    with st.spinner(t("loading", lang) if lang=="en" else "Đang tải timeline tuân thủ..."):
        timeline = get_compliance_timeline(days, province_arg)
    if not timeline.empty:
        # Map compliance status
        comp_map = {
            "Good/Safe": t("compliance_good", lang),
            "Warning (WHO Breach)": t("compliance_who", lang),
            "Unhealthy (TCVN Breach)": t("compliance_tcvn", lang)
        }
        timeline["status_label"] = timeline["compliance_status"].map(comp_map).fillna(timeline["compliance_status"])
        
        # Color map with localized labels
        localized_colors = {t(k, lang) if k in ["Good/Safe", "Warning (WHO Breach)", "Unhealthy (TCVN Breach)"] else k: v for k, v in COMPLIANCE_COLORS.items()}
        # Handle cases where COMPLIANCE_COLORS uses original keys
        color_map = {
            t("compliance_good", lang): COMPLIANCE_COLORS["Good/Safe"],
            t("compliance_who", lang): COMPLIANCE_COLORS["Warning (WHO Breach)"],
            t("compliance_tcvn", lang): COMPLIANCE_COLORS["Unhealthy (TCVN Breach)"]
        }

        fig = px.bar(
            timeline,
            x="date",
            y="cnt",
            color="status_label",
            color_discrete_map=color_map,
            barmode="stack",
            labels={"date": t("chart_label_date", lang), "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)},
        )
        fig.update_layout(height=280, showlegend=True, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_chart("Không có dữ liệu tuân thủ. Mức AQI đang ở ngưỡng an toàn.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("dm_aqi_compliance_standards chưa có dữ liệu trong khoảng thời gian này.")

    # ── WHO/TCVN breach bars ───────────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(t("chart_breach_comparison", lang))
        breach = get_breach_by_province(days)
        if not breach.empty:
            # Melt to long form for grouped bar
            breach_melted = pd.melt(
                breach,
                id_vars=["province"],
                value_vars=["who_breach_days", "tcvn_breach_days"],
                var_name="standard_key",
                value_name="days_val",
            )
            standard_map = {
                "who_breach_days": "WHO (PM2.5>15µg)",
                "tcvn_breach_days": "TCVN (PM2.5>50µg)",
            }
            breach_melted["standard"] = breach_melted["standard_key"].map(standard_map)
            fig = px.bar(
                breach_melted,
                x="province",
                y="days_val",
                color="standard",
                barmode="group",
                color_discrete_map={
                    "WHO (PM2.5>15µg)": "#FF7E00",
                    "TCVN (PM2.5>50µg)": "#FF0000",
                },
                labels={"province": t("province", lang), "days_val": t("chart_label_days", lang)},
            )
            fig.update_layout(height=300, showlegend=True, margin=dict(l=0, r=0, t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = render_empty_chart("Không có dữ liệu vi phạm tiêu chuẩn.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Không có vi phạm WHO/TCVN trong khoảng thời gian đã chọn.")

    with col_right:
        st.subheader(t("chart_data_freshness", lang))
        health = get_platform_health()
        if not health.empty:
            health["source_label"] = health["source"].str.upper()
            health["attention_label"] = health["attention_count"].astype(int).astype(str)
            fig = px.bar(
                health,
                x="source_label",
                y="attention_count",
                color="source_label",
                text="attention_label",
                labels={
                    "source_label": t("chart_label_source", lang),
                    "attention_count": t("attention_needed", lang),
                },
                hover_data={
                    "source_label": False,
                    "attention_label": False,
                    "reliable_pct": ":.1f",
                    "latest_lag_hours": ":.1f",
                    "stale_count": True,
                    "offline_count": True,
                },
            )
            fig.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=10, b=30))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(t("ops_dashboard_note", lang))
        else:
            fig = render_empty_chart("Không có dữ liệu sức khỏe hệ thống.")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("dm_platform_source_health chưa có dữ liệu.")

    # ── high-risk heatmap ─────────────────────────────────────────────────────
    st.subheader(t("chart_high_risk_hours", lang))
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
            range_color=[0, 24],
            labels={
                "date_str": t("chart_label_date", lang),
                "province": t("province", lang),
                "high_risk_hours": t("chart_label_hour", lang),
            },
        )
        fig.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = render_empty_chart("Không có giờ rủi ro cao. Chất lượng không khí đang tốt.")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("dm_aqi_health_impact_summary chưa có dữ liệu trong khoảng thời gian này.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
