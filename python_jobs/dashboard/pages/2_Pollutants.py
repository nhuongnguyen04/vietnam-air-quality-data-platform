import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import get_plotly_layout
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
    where_clause = f"AND province = '{province}'" if province else ""
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
def get_compliance_status(days: int, province: str | None):
    where_clause = f"AND province = '{province}'" if province else ""
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
st.subheader(f"{t('nav_pollutants', lang)} — {TIME_OPTIONS[days]}")
trend = get_pollutant_trend(days, province_arg)

if not trend.empty:
    pollutants = ["pm25_avg", "pm10_avg", "o3_avg", "no2_avg", "co_avg", "so2_avg"]
    fig = px.line(trend, x="date", y=pollutants, title=None)
    fig.update_layout(get_plotly_layout(height=400))
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: Compliance & Health ───────────────────────────────────────────────
c_compl, c_health = st.columns(2)

with c_compl:
    st.subheader(t("status_title", lang) if lang=="en" else "Tuân thủ tiêu chuẩn")
    compliance = get_compliance_status(days, province_arg)
    if not compliance.empty:
        fig_comp = px.bar(compliance, x="province", y="cnt", color="compliance_status",
                         color_discrete_map={"Good/Safe": "#09ab3b", "Warning (WHO Breach)": "#ffa500", "Unhealthy (TCVN Breach)": "#ff4b4b"})
        fig_comp.update_layout(get_plotly_layout(height=350))
        st.plotly_chart(fig_comp, use_container_width=True)

with c_health:
    st.subheader(t("health_title", lang) if lang=="en" else "Rủi ro Sức khỏe")
    # Using existing logic placeholder or similar
    st.markdown('<div class="glass-card"><h4>Health Context</h4><p>Detailed regional health risk analysis based on cumulative pollutant exposure.</p></div>', unsafe_allow_html=True)
