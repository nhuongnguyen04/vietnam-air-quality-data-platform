"""
Trang Chất ô nhiễm (Pollutants) phân tích chi tiết nồng độ các chất gây ô nhiễm chính 
(PM2.5, PM10, NO2, O3, CO, SO2) theo thời gian và địa điểm, giúp người dùng hiểu sâu 
về thành phần ô nhiễm tại khu vực.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.data_service import get_source_table, build_where_clause
from lib.style import get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
from lib.filters import render_sidebar_filters

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_grain = filters["time_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters["pollutant"]
standard = filters["standard"]

# ── helpers ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_pollutant_trend(table: str, grain: str, scope_val: str | None, dates):
    where_clause = build_where_clause(grain, scope_val, dates)

    # Use fct_air_quality_summary_daily if table is _daily for individual pollutant AQIs
    source_table = f"air_quality.{table}"
    if table.endswith("_daily"):
        source_table = "air_quality.fct_air_quality_summary_daily"
    elif table.endswith("_hourly"):
        source_table = f"air_quality.{table}"

    q = f"""
    SELECT
        date,
        round(avg(pm25_daily_aqi), 1)   AS pm25_aqi,
        round(avg(pm10_daily_aqi), 1)   AS pm10_aqi,
        round(avg(co_daily_aqi), 1)     AS co_aqi,
        round(avg(no2_daily_aqi), 1)    AS no2_aqi,
        round(avg(so2_daily_aqi), 1)   AS so2_aqi,
        round(avg(o3_daily_aqi), 1)     AS o3_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_source_fingerprint(grain: str, scope_val: str | None, dates):
    where_clause = build_where_clause(grain, scope_val, dates)
    
    q = f"""
    SELECT
        probable_source,
        count(*) as cnt
    FROM air_quality.dm_pollutant_source_fingerprint
    WHERE {where_clause}
    GROUP BY probable_source
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_compliance_status(grain: str, scope_val: str | None, dates):
    where_clause = build_where_clause(grain, scope_val, dates)

    q = f"""
    SELECT
        province,
        compliance_status,
        count(*) AS cnt
    FROM air_quality.dm_aqi_compliance_standards
    WHERE {where_clause}
    GROUP BY province, compliance_status
    ORDER BY province, cnt DESC
    """
    return query_df(q)

# ── UI Header ─────────────────────────────────────────────────────────────────
st.title(t("nav_pollutants", lang))

# Determine Source Table
table_name = get_source_table(spatial_grain, time_grain)

# ── Row 1: Trends ─────────────────────────────────────────────────────────────
st.subheader(f"{t('nav_pollutants', lang)} (AQI VN)")
trend = get_pollutant_trend(table_name, spatial_grain, scope_val, date_range)

if not trend.empty:
    col_map = {
        "pm25_aqi": t("pollutant_pm25", lang), "pm10_aqi": t("pollutant_pm10", lang),
        "o3_aqi": t("pollutant_o3", lang), "no2_aqi": t("pollutant_no2", lang),
        "so2_aqi": t("pollutant_so2", lang), "co_aqi": t("pollutant_co", lang)
    }
    plot_df = trend.rename(columns=col_map)
    display_pollutants = list(col_map.values())
    
    # Highlight selected pollutant if applicable
    highlight_poll = pollutant.upper() if pollutant != "aqi" else None
    
    fig = px.line(plot_df, x="date", y=display_pollutants)
    fig.update_layout(get_plotly_layout(height=400), hovermode="x unified")
    
    # If a specific pollutant is selected, make its line thicker
    if highlight_poll:
        for trace in fig.data:
            if highlight_poll in trace.name:
                trace.line.width = 5
            else:
                trace.line.width = 1.5
                trace.opacity = 0.6
                
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Row 2: Source Fingerprint ─────────────────────────────────────────────────
c1, c2 = st.columns([1, 1.5])
with c1:
    st.subheader(t("source_attribution", lang))
    df_source = get_source_fingerprint(spatial_grain, scope_val, date_range)
    if not df_source.empty:
        # Map source strings to localized versions
        source_map = {
            'Combustion/Traffic': t('source_traffic', lang),
            'Dust/Construction': t('source_dust', lang),
            'Mixed': t('source_mixed', lang)
        }
        df_source['source_label'] = df_source['probable_source'].map(source_map).fillna(df_source['probable_source'])
        
        # Color mapping needs to use the same keys as the data passed to plotly
        color_map = {
            t('source_traffic', lang): '#ff7f0e',
            t('source_dust', lang): '#8c564b',
            t('source_mixed', lang): '#7f7f7f'
        }

        fig_pie = px.pie(
            df_source, 
            values='cnt', 
            names='source_label',
            color='source_label',
            hole=0.4,
            color_discrete_map=color_map,
            labels={'source_label': t('chart_label_type', lang), 'cnt': t('chart_label_count', lang)}
        )
        fig_pie.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=350)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.caption(t("no_data", lang) if lang=="en" else "Chưa có dữ liệu phân tích nguồn.")

with c2:
    st.subheader(t("compliance_status_title", lang))
    compliance = get_compliance_status(spatial_grain, scope_val, date_range)
    if not compliance.empty:
        # Map compliance status to localized versions
        comp_map = {
            'Good/Safe': t('compliance_good', lang),
            'Warning (WHO Breach)': t('compliance_who', lang),
            'Unhealthy (TCVN Breach)': t('compliance_tcvn', lang)
        }
        compliance['status_label'] = compliance['compliance_status'].map(comp_map).fillna(compliance['compliance_status'])
        
        color_map = {
            t('compliance_good', lang): "#09ab3b",
            t('compliance_who', lang): "#ffa500",
            t('compliance_tcvn', lang): "#ff4b4b"
        }
        cat_orders = [t('compliance_good', lang), t('compliance_who', lang), t('compliance_tcvn', lang)]

        fig_comp = px.bar(compliance, x="province", y="cnt", color="status_label",
                         color_discrete_map=color_map,
                         category_orders={"status_label": cat_orders},
                         labels={"province": t("province", lang), "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)},
                         barmode="stack")
        fig_comp.update_layout(get_plotly_layout(height=350), barnorm="percent")
        st.plotly_chart(fig_comp, use_container_width=True)

st.info("Phân tích nguồn dựa trên tỷ lệ PM2.5/PM10. Tỷ lệ > 0.6 gợi ý hoạt động đốt cháy/giao thông, < 0.4 gợi ý bụi/xây dựng.")
