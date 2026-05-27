"""
Alerts & Standards Compliance page.
Provides standard compliance timelines, WHO/TCVN breach comparative bars, and risk heatmaps.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import escape_value
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state

COMPLIANCE_COLORS = {
    "Good/Safe": "#10B981",
    "Warning (WHO Breach)": "#F59E0B",
    "Unhealthy (TCVN Breach)": "#EF4444",
}

EN_MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

def format_date_label(value, lang: str) -> str:
    date_value = pd.to_datetime(value)
    if pd.isna(date_value):
        return str(value)
    if lang == "vi":
        return f"{date_value.day}/{date_value.month}<br>{date_value.year}"
    month = EN_MONTH_ABBR[date_value.month]
    return f"{month} {date_value.day}<br>{date_value.year}"

@st.cache_data(ttl=300)
def get_compliance_timeline(days: int, province: str | None):
    days = int(days)
    if province:
        escaped_province = escape_value(province)
        where_clause = f"AND province = '{escaped_province}'"
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
    days = int(days)
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
    LIMIT 15
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_high_risk_heatmap(days: int, province: str | None):
    days = int(days)
    if province:
        escaped_province = escape_value(province)
        where_clause = f"AND province = '{escaped_province}'"
    else:
        where_clause = ""
    q = f"""
    WITH province_hours AS (
        SELECT
            province,
            date,
            datetime_hour,
            max(avg_aqi_us) AS hourly_aqi_us
        FROM air_quality.fct_aqiin__province_hourly
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

@page_wrapper("alert", "🚨 Alerts & Standards Compliance", icon="🚨")
def main(lang: str):
    # ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
    filters = render_sidebar_filters()
    spatial_grain = filters["spatial_grain"]
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]

    # Calculate days based on selected filter range
    delta = date_range[1] - date_range[0]
    days = max(1, delta.days)

    province_arg = scope_val if spatial_grain in ["Tỉnh", "Phường"] else None

    # Fetch Data
    with st.spinner(t("loading", lang) if lang == "en" else "Đang tải dữ liệu tuân thủ..."):
        timeline = get_compliance_timeline(days, province_arg)
        breach = get_breach_by_province(days)
        risk = get_high_risk_heatmap(days, province_arg)

    # ── 1. Alerts Summary Metrics ──────────────────────────────────────────────
    c_alert = st.columns(4)
    if not timeline.empty:
        total_breach = timeline[timeline["compliance_status"] != "Good/Safe"]["cnt"].sum()
        total_days = timeline["cnt"].sum()
        rate = ((total_days - total_breach) * 100.0 / total_days) if total_days > 0 else 100.0
        
        with c_alert[0]:
            render_metric_card("Tỷ lệ tuân thủ", f"{rate:.1f}%", icon="star")
        with c_alert[1]:
            render_metric_card("Số ngày vi phạm", f"{int(total_breach)} ngày", icon="error")
        with c_alert[2]:
            worst_p = breach.iloc[0].province if not breach.empty else "N/A"
            render_metric_card("Điểm nóng vi phạm", worst_p, icon="location")
        with c_alert[3]:
            render_metric_card("Tổng ngày phân tích", f"{int(total_days)} ngày", icon="schedule")
    else:
        for idx in range(4):
            with c_alert[idx]:
                render_metric_card("Hệ thống", "N/A", icon="star")

    render_section_divider()

    # ── 2. Compliance Timeline ──────────────────────────────────────────────────
    st.markdown(f"#### 📈 {t('chart_compliance_timeline', lang)}")
    if not timeline.empty:
        comp_map = {
            "Good/Safe": t("compliance_good", lang),
            "Warning (WHO Breach)": t("compliance_who", lang),
            "Unhealthy (TCVN Breach)": t("compliance_tcvn", lang)
        }
        timeline["status_label"] = timeline["compliance_status"].map(comp_map).fillna(timeline["compliance_status"])
        timeline["date_label"] = timeline["date"].apply(lambda val: format_date_label(val, lang))
        date_order = timeline[["date", "date_label"]].drop_duplicates().sort_values("date")["date_label"].tolist()

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
        fig.update_layout(get_plotly_layout(height=300, compact=True), showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu tuân thủ trong phạm vi này."), use_container_width=True)

    render_section_divider()

    # ── 3. WHO/TCVN Breach Comparative Bars ─────────────────────────────────────
    st.markdown(f"#### 📊 {t('chart_breach_comparison', lang)}")
    if not breach.empty:
        breach_melted = pd.melt(
            breach,
            id_vars=["province"],
            value_vars=["who_breach_days", "tcvn_breach_days"],
            var_name="standard_key",
            value_name="days_val",
        )
        standard_map = {
            "who_breach_days": "WHO (PM2.5 > 15 µg/m³)",
            "tcvn_breach_days": "TCVN (PM2.5 > 50 µg/m³)",
        }
        breach_melted["standard"] = breach_melted["standard_key"].map(standard_map)
        fig = px.bar(
            breach_melted,
            x="province",
            y="days_val",
            color="standard",
            barmode="group",
            color_discrete_map={
                "WHO (PM2.5 > 15 µg/m³)": "#F59E0B",
                "TCVN (PM2.5 > 50 µg/m³)": "#EF4444",
            },
            labels={"province": t("province", lang), "days_val": t("chart_label_days", lang)},
        )
        fig.update_layout(get_plotly_layout(height=320, compact=True), showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu vi phạm tiêu chuẩn."), use_container_width=True)

    render_section_divider()

    # ── 4. High-risk Heatmap ───────────────────────────────────────────────────
    st.markdown(f"#### 🌡️ {t('chart_high_risk_hours', lang)}")
    if not risk.empty:
        risk["date"] = pd.to_datetime(risk["date"])
        risk["date_label"] = risk["date"].apply(lambda val: format_date_label(val, lang))
        risk["high_risk_hours"] = pd.to_numeric(risk["high_risk_hours"], errors="coerce").clip(0, 24)

        date_order = risk[["date", "date_label"]].drop_duplicates().sort_values("date")["date_label"].tolist()
        top_provs = risk.groupby("province")["high_risk_hours"].sum().nlargest(12).index.tolist()
        
        if top_provs:
            filtered = risk[risk["province"].isin(top_provs)]
            heatmap = filtered.pivot(index="province", columns="date_label", values="high_risk_hours").reindex(index=top_provs, columns=date_order)

            hovertemplate = (
                f"{t('province', lang)}: %{{y}}<br>"
                f"{t('chart_label_date', lang)}: %{{x}}<br>"
                f"{t('chart_label_hour', lang)}: %{{z:.0f}} hrs<extra></extra>"
            )
            fig = go.Figure(
                data=go.Heatmap(
                    x=heatmap.columns.tolist(),
                    y=heatmap.index.tolist(),
                    z=heatmap.to_numpy(),
                    zmin=0,
                    zmax=24,
                    colorscale=[
                        [0.0, "#10B981"],
                        [0.25, "#EAB308"],
                        [0.5, "#F97316"],
                        [0.75, "#EF4444"],
                        [1.0, "#7F1D1D"],
                    ],
                    colorbar={
                        "title": {"text": t("chart_label_hour", lang), "side": "top"},
                        "thickness": 16,
                        "len": 0.84,
                    },
                    hovertemplate=hovertemplate,
                )
            )
            fig.update_layout(
                height=350,
                margin={"l": 0, "r": 90, "t": 10, "b": 45},
                xaxis_title=t("chart_label_date", lang),
                yaxis_title=t("province", lang),
                yaxis={"autorange": "reversed"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.plotly_chart(create_empty_state("Không đủ dữ liệu giờ rủi ro cao."), use_container_width=True)
    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu giờ rủi ro cao."), use_container_width=True)

if __name__ == "__main__":
    main()
