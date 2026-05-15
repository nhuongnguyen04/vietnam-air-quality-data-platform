"""Alerts page — compliance timeline, WHO/TCVN breaches, and high-risk exposure heatmap."""
from __future__ import annotations

# ruff: noqa: E402
import sys

sys.path.insert(0, "..")

"""
Trang Cảnh báo (Alerts) theo dõi và thông báo về các sự kiện ô nhiễm vượt ngưỡng quy định.
Hệ thống cung cấp lịch sử các đợt cảnh báo và chi tiết về mức độ rủi ro tại từng khu vực.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
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

EN_MONTH_ABBR = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


# ── helpers ─────────────────────────────────────────────────────────────────────

def format_date_label(value, lang: str) -> str:
    date_value = pd.to_datetime(value)
    if pd.isna(date_value):
        return str(value)

    if lang == "vi":
        return f"{date_value.day} Thg {date_value.month}<br>{date_value.year}"

    month = EN_MONTH_ABBR[date_value.month]
    return f"{month} {date_value.day}<br>{date_value.year}"


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
    WITH province_hours AS (
        SELECT
            province,
            date,
            datetime_hour,
            max(avg_aqi_us) AS hourly_aqi_us
        FROM air_quality.fct_air_quality_province_level_hourly
        WHERE date >= today() - INTERVAL {days} DAY
          AND province IS NOT NULL AND province != ''
          {where_clause}
        GROUP BY province, date, datetime_hour
    )
    SELECT
        province,
        date,
        least(24, countIf(hourly_aqi_us > 150)) AS high_risk_hours
    FROM province_hours
    GROUP BY province, date
    ORDER BY province, date
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
        timeline["date_label"] = timeline["date"].apply(lambda value: format_date_label(value, lang))
        date_order = (
            timeline[["date", "date_label"]]
            .drop_duplicates()
            .sort_values("date")["date_label"]
            .tolist()
        )

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
            x="date_label",
            y="cnt",
            color="status_label",
            color_discrete_map=color_map,
            category_orders={"date_label": date_order},
            barmode="stack",
            labels={"date_label": t("chart_label_date", lang), "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)},
        )
        fig.update_layout(height=280, showlegend=True, margin={"l": 0, "r": 0, "t": 10, "b": 40})
        st.plotly_chart(fig, width='stretch')
    else:
        fig = render_empty_chart("Không có dữ liệu tuân thủ. Mức AQI đang ở ngưỡng an toàn.")
        st.plotly_chart(fig, width='stretch')
        st.caption("dm_aqi_compliance_standards chưa có dữ liệu trong khoảng thời gian này.")

    # ── WHO/TCVN breach bars ───────────────────────────────────────────────────
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
        fig.update_layout(height=320, showlegend=True, margin={"l": 0, "r": 0, "t": 10, "b": 30})
        st.plotly_chart(fig, width='stretch')
    else:
        fig = render_empty_chart("Không có dữ liệu vi phạm tiêu chuẩn.")
        st.plotly_chart(fig, width='stretch')
        st.caption("Không có vi phạm WHO/TCVN trong khoảng thời gian đã chọn.")

    # ── high-risk heatmap ─────────────────────────────────────────────────────
    st.subheader(t("chart_high_risk_hours", lang))
    risk = get_high_risk_heatmap(days, province_arg)
    if not risk.empty:
        risk["date"] = pd.to_datetime(risk["date"])
        risk["date_label"] = risk["date"].apply(lambda value: format_date_label(value, lang))
        risk["high_risk_hours"] = pd.to_numeric(risk["high_risk_hours"], errors="coerce").clip(0, 24)

        date_order = (
            risk[["date", "date_label"]]
            .drop_duplicates()
            .sort_values("date")["date_label"]
            .tolist()
        )

        # Limit to top provinces by high-risk duration after province/day aggregation.
        top_provs = (
            risk.groupby("province")["high_risk_hours"]
            .sum()
            .nlargest(15)
            .index.tolist()
        )
        filtered = risk[risk["province"].isin(top_provs)]
        heatmap = (
            filtered.pivot(index="province", columns="date_label", values="high_risk_hours")
            .reindex(index=top_provs, columns=date_order)
        )

        hovertemplate = (
            f"{t('province', lang)}: %{{y}}<br>"
            f"{t('chart_label_date', lang)}: %{{x}}<br>"
            f"{t('chart_label_hour', lang)}: %{{z:.0f}}<extra></extra>"
        )
        fig = go.Figure(
            data=go.Heatmap(
                x=heatmap.columns.tolist(),
                y=heatmap.index.tolist(),
                z=heatmap.to_numpy(),
                zmin=0,
                zmax=24,
                colorscale=[
                    [0.0, "#00E400"],
                    [0.25, "#FFFF00"],
                    [0.5, "#FF7E00"],
                    [0.75, "#FF0000"],
                    [1.0, "#7E0023"],
                ],
                colorbar={
                    "title": {"text": t("chart_label_hour", lang), "side": "top"},
                    "thickness": 16,
                    "len": 0.72,
                    "x": 1.01,
                    "xanchor": "left",
                },
                hovertemplate=hovertemplate,
            )
        )
        fig.update_layout(
            height=360,
            margin={"l": 0, "r": 90, "t": 10, "b": 45},
            xaxis_title=t("chart_label_date", lang),
            yaxis_title=t("province", lang),
            yaxis={"autorange": "reversed"},
        )
        st.plotly_chart(fig, width='stretch')
    else:
        fig = render_empty_chart("Không có giờ rủi ro cao. Chất lượng không khí đang tốt.")
        st.plotly_chart(fig, width='stretch')
        st.caption("fct_air_quality_province_level_hourly chưa có dữ liệu trong khoảng thời gian này.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
