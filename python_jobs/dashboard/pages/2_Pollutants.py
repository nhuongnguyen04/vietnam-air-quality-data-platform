import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

# ── helpers ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_provinces():
    q = """
    SELECT DISTINCT province
    FROM air_quality.dm_aqi_current_status
    WHERE province IS NOT NULL AND province != ''
    ORDER BY province
    """
    df = query_df(q)
    return df["province"].tolist() if not df.empty else []

@st.cache_data(ttl=3600)
def get_districts(province: str):
    q = f"""
    SELECT DISTINCT district
    FROM air_quality.dm_aqi_current_status
    WHERE province = '{province}' AND district IS NOT NULL AND district != ''
    ORDER BY district
    """
    df = query_df(q)
    return df["district"].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_pollutant_trend(days: int, province: str | None, district: str | None = None):
    where_clause = ""
    if province:
        where_clause += f"AND province = '{province}' "
    if district:
        where_clause += f"AND district = '{district}' "

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
def get_source_fingerprint(days: int, province: str | None):
    where_clause = f"WHERE date >= today() - INTERVAL {days} DAY"
    if province:
        where_clause += f" AND province = '{province}'"
    
    q = f"""
    SELECT
        probable_source,
        count(*) as cnt
    FROM air_quality.dm_pollutant_source_fingerprint
    {where_clause}
    GROUP BY probable_source
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_compliance_status(days: int, province: str | None, district: str | None = None):
    where_clause = ""
    if province:
        where_clause += f"AND province = '{province}' "
    if district:
        where_clause += f"AND district = '{district}' "

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
    c1, c2, c3 = st.columns([1, 1, 1])
    provinces = get_provinces()
    national_label = "National" if lang == "en" else "Toàn quốc"
    with c1:
        selected_province = st.selectbox(
            "Select Province/City" if lang == "en" else "Chọn tỉnh/thành phố",
            options=[national_label] + provinces,
            index=0,
        )
    
    province_arg = selected_province if selected_province != national_label else None
    district_arg = None
    
    with c2:
        if province_arg:
            districts = get_districts(province_arg)
            all_district_label = "All Districts" if lang == "en" else "Tất cả các huyện"
            selected_district = st.selectbox(
                "Select District" if lang == "en" else "Chọn quận/huyện",
                options=[all_district_label] + districts,
                index=0,
            )
            district_arg = selected_district if selected_district != all_district_label else None
        else:
            st.selectbox(
                "Select District" if lang == "en" else "Chọn quận/huyện",
                options=["-"],
                disabled=True
            )

    with c3:
        TIME_OPTIONS = {7: "7d", 30: "30d", 90: "3m", 365: "1y"}
        days = st.selectbox(
            "Time Interval" if lang == "en" else "Khoảng thời gian",
            options=list(TIME_OPTIONS.keys()),
            format_func=lambda x: TIME_OPTIONS[x],
            index=1,
        )
    st.markdown('</div>', unsafe_allow_html=True)

# ── Row 1: Trends ─────────────────────────────────────────────────────────────
st.subheader(f"{t('nav_pollutants', lang)} (AQI VN) — {TIME_OPTIONS[days]}")
trend = get_pollutant_trend(days, province_arg, district_arg)

if not trend.empty:
    col_map = {
        "pm25_aqi": t("pollutant_pm25", lang), "pm10_aqi": t("pollutant_pm10", lang),
        "o3_aqi": t("pollutant_o3", lang), "no2_aqi": t("pollutant_no2", lang),
        "so2_aqi": t("pollutant_so2", lang), "co_aqi": t("pollutant_co", lang)
    }
    plot_df = trend.rename(columns=col_map)
    display_pollutants = list(col_map.values())
    fig = px.line(plot_df, x="date", y=display_pollutants)
    fig.update_layout(get_plotly_layout(height=400), hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Row 2: Source Fingerprint (New) ───────────────────────────────────────────
c1, c2 = st.columns([1, 1.5])
with c1:
    st.subheader("Source Attribution" if lang=="en" else "Nguồn gốc Ô nhiễm")
    df_source = get_source_fingerprint(days, province_arg)
    if not df_source.empty:
        fig_pie = px.pie(
            df_source, 
            values='cnt', 
            names='probable_source',
            color='probable_source',
            hole=0.4,
            color_discrete_map={
                'Combustion/Traffic': '#ff7f0e', 
                'Dust/Construction': '#8c564b', 
                'Mixed': '#7f7f7f'
            }
        )
        fig_pie.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=350)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.caption("Chưa có dữ liệu phân tích nguồn.")

with c2:
    st.subheader("Compliance Status" if lang=="en" else "Tuân thủ tiêu chuẩn")
    compliance = get_compliance_status(days, province_arg, district_arg)
    if not compliance.empty:
        fig_comp = px.bar(compliance, x="province", y="cnt", color="compliance_status",
                         color_discrete_map={"Good/Safe": "#09ab3b", "Warning (WHO Breach)": "#ffa500", "Unhealthy (TCVN Breach)": "#ff4b4b"},
                         category_orders={"compliance_status": ["Good/Safe", "Warning (WHO Breach)", "Unhealthy (TCVN Breach)"]},
                         barmode="stack")
        fig_comp.update_layout(get_plotly_layout(height=350), barnorm="percent")
        st.plotly_chart(fig_comp, use_container_width=True)

st.info("Phân tích nguồn dựa trên tỷ lệ PM2.5/PM10. Tỷ lệ > 0.6 gợi ý hoạt động đốt cháy/giao thông, < 0.4 gợi ý bụi/xây dựng.")
