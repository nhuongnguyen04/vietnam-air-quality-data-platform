import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

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
    provinces = get_provinces()
    if province and province not in provinces:
        province = None
    where_clause = f"AND province = '{province}'" if province else ""
    q = f"""
    SELECT
        date,
        round(avg(pm25_aqi), 1)   AS pm25_aqi,
        round(avg(pm10_aqi), 1)   AS pm10_aqi,
        round(avg(co_aqi), 1)     AS co_aqi,
        round(avg(no2_aqi), 1)    AS no2_aqi,
        round(avg(so2_aqi), 1)    AS so2_aqi,
        round(avg(o3_aqi), 1)     AS o3_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE date >= (SELECT max(date) FROM air_quality.fct_air_quality_summary_daily) - INTERVAL {days} DAY
      {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_compliance_status(days: int, province: str | None):
    provinces = get_provinces()
    if province and province not in provinces:
        province = None
    where_clause = f"AND province = '{province}'" if province else ""
    q = f"""
    SELECT
        province,
        compliance_status,
        count(*) AS cnt
    FROM air_quality.dm_aqi_compliance_standards
    WHERE date >= (SELECT max(date) FROM air_quality.dm_aqi_compliance_standards) - INTERVAL {days} DAY
      {where_clause}
    GROUP BY province, compliance_status
    ORDER BY province, cnt DESC
    """
    return query_df(q)

# ── UI Header ─────────────────────────────────────────────────────────────────
st.title(t("nav_pollutants", lang))

# ── Filters (Glass Card Style) ────────────────────────────────────────────────
with st.container():
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    c1, c2 = st.columns([2, 1])
    provinces = get_provinces()
    national_label = "National" if lang == "en" else "Toàn quốc"
    with c1:
        selected_province = st.selectbox(
            "Select Province/City" if lang == "en" else "Chọn tỉnh/thành phố",
            options=[national_label] + provinces,
            index=0,
        )
    with c2:
        TIME_OPTIONS = {7: "7d", 30: "30d", 90: "3m", 365: "1y"}
        days = st.selectbox(
            "Time Interval" if lang == "en" else "Khoảng thời gian",
            options=list(TIME_OPTIONS.keys()),
            format_func=lambda x: TIME_OPTIONS[x],
            index=1,
        )
    st.markdown('</div>', unsafe_allow_html=True)

province_arg = selected_province if selected_province != national_label else None

# ── Chart 1: Trends ──────────────────────────────────────────────────────────
st.subheader(f"{t('nav_pollutants', lang)} (AQI VN) — {TIME_OPTIONS[days]}")
trend = get_pollutant_trend(days, province_arg)

if not trend.empty:
    # Map raw column names to localized display names
    col_map = {
        "pm25_aqi": t("pollutant_pm25", lang),
        "pm10_aqi": t("pollutant_pm10", lang),
        "o3_aqi": t("pollutant_o3", lang),
        "no2_aqi": t("pollutant_no2", lang),
        "so2_aqi": t("pollutant_so2", lang),
        "co_aqi": t("pollutant_co", lang)
    }
    
    # Rename columns in the dataframe for automatic Plotly localization
    plot_df = trend.rename(columns=col_map)
    display_pollutants = list(col_map.values())
    
    labels = {
        "variable": t("chart_label_variable", lang),
        "value": t("chart_label_aqi", lang),
        "date": t("chart_label_date", lang)
    }
    
    fig = px.line(plot_df, x="date", y=display_pollutants, labels=labels)
    fig.update_layout(get_plotly_layout(height=400))
    fig.update_layout(
        yaxis_title=t("chart_label_aqi", lang), 
        xaxis_title=t("chart_label_date", lang),
        hovermode="x unified"
    )
    
    # Simple and clean localized tooltip
    fig.update_traces(
        hovertemplate=f"<b>{t('chart_label_aqi', lang)}</b>: %{{y:.1f}}<extra></extra>"
    )
    
    st.plotly_chart(fig, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu xu hướng chất ô nhiễm trong khoảng thời gian đã chọn."), use_container_width=True)
st.subheader(t("status_title", lang) if lang=="en" else "Tuân thủ tiêu chuẩn")
compliance = get_compliance_status(days, province_arg)
if not compliance.empty:
    fig_comp = px.bar(compliance, x="province", y="cnt", color="compliance_status",
                     color_discrete_map={"Good/Safe": "#09ab3b", "Warning (WHO Breach)": "#ffa500", "Unhealthy (TCVN Breach)": "#ff4b4b"},
                     category_orders={"compliance_status": ["Good/Safe", "Warning (WHO Breach)", "Unhealthy (TCVN Breach)"]},
                     labels={"province": "", "cnt": "", "compliance_status": ""},
                     barmode="stack")
    
    fig_comp.update_layout(get_plotly_layout(height=450))
    fig_comp.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        legend_title_text=None,
        margin={"r":10, "t":30, "l":10, "b":150}, # Increased bottom margin for rotated province names
        bargap=0.15, # Ensures consistent bar widths
        barnorm="percent", # Correctly set normalization in layout
    )
    st.plotly_chart(fig_comp, use_container_width=True)
else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu tuân thủ tiêu chuẩn."), use_container_width=True)
