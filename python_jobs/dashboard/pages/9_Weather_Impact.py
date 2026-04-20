import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
from lib.filters import render_sidebar_filters
from lib.data_service import build_where_clause

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("weather_title", lang))

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters.get("pollutant", "pm25")

# ── Dynamic Mapping ───────────────────────────────────────────────────────────
# Analytics table currently supports particulate matter (pm25, pm10)
target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
p_col = f"{target_poll}_daily_avg"
sum_col = f"sum_{target_poll}"
stagnant_sum_col = f"stagnant_{target_poll}_sum"
dispersive_sum_col = f"dispersive_{target_poll}_sum"

if pollutant not in ["pm25", "pm10"]:
    st.warning(f"⚠️ Weather analysis for **{pollutant.upper()}** is currently calculated based on **{target_poll.upper()}** correlations.")

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_weather_summary_stats(grain: str, scope: str | None = None, dates=None, p_stag="p_stag", p_disp="p_disp"):
    where_clause = build_where_clause(grain, scope, dates)
    
    q = f"""
    WITH stats_cte AS (
        SELECT
            sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
            sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp,
            avg(stagnant_air_probability) as stagnant_prob,
            avg(wind_daily_avg) as avg_wind
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause}
    )
    SELECT
        *,
        (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
    FROM stats_cte
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_ranking_data(grain: str, scope: str | None = None, dates=None, p_stag="p_stag", p_disp="p_disp"):
    where_clause = build_where_clause(grain, scope, dates)
    y_col = "ward_code" if grain == "Phường" else "province"
    
    q = f"""
    WITH rank_cte AS (
        SELECT
            {y_col} as label_col,
            avg({p_col} / nullif(wind_daily_avg, 0)) as risk_index,
            sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
            sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause} AND {y_col} != ''
        GROUP BY label_col
    )
    SELECT
        *,
        (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
    FROM rank_cte
    ORDER BY risk_index DESC
    LIMIT 15
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_hourly_trend(grain: str, scope: str | None = None, dates=None, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    # Map pollutant to column name for hourly trend
    target_col = col if col in ["pm25", "pm10", "aqi_vn", "aqi_us"] else "pm25"
            
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg({target_col}) as avg_val,
        avg(wind_speed) as avg_wind,
        avg(humidity) as avg_hum
    FROM air_quality.dm_weather_hourly_trend
    WHERE {where_clause}
    GROUP BY datetime_hour
    ORDER BY datetime_hour
    """
    return query_df(q)

# ── Data Fetching (Optimized SQL) ─────────────────────────────────────────────
df_summary = get_weather_summary_stats(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)
df_hourly = get_weather_hourly_trend(spatial_grain, scope_val, date_range, col=target_poll)

if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_stag):
    stats = df_summary.iloc[0]
    influence_pct = stats.influence_pct
    stagnant_prob = stats.stagnant_prob
    avg_wind = stats.avg_wind

    # ── Row 1: KPI Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    
    with c1:
        render_metric_card(t("weather_influence", lang), f"{influence_pct:.1f}%", icon="cloud")
    with c2:
        render_metric_card(t("weather_stagnant_risk", lang), f"{stagnant_prob:.1%}", icon="ac_unit")
    with c3:
        render_metric_card(t("weather_wind_speed", lang), f"{avg_wind:.1f} m/s", icon="wind")

    st.markdown("---")

    # ── Row 2: Weather Influence Analysis ─────────────────────────────────────
    st.subheader(t("weather_question", lang))
    col_gauge, col_text = st.columns([1, 1])
    
    with col_gauge:
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = influence_pct,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "%"},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "#1f77b4"},
                'steps': [
                    {'range': [0, 20], 'color': "#00CC96"},
                    {'range': [20, 50], 'color': "#FECB52"},
                    {'range': [50, 100], 'color': "#EF553B"}
                ],
            }
        ))
        fig_gauge.update_layout(height=250, margin=dict(t=50, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_text:
        st.write("")
        st.write("")
        location_name = scope_val if scope_val else (t("national", lang) if lang=="en" else "Toàn quốc")
        if influence_pct > 30:
            msg = f"Độ nhạy cảm tại {location_name} rất cao. Thời tiết đóng vai trò then chốt trong ô nhiễm." if lang == "vi" else f"Weather sensitivity in {location_name} is very high. Weather plays a key role in pollution."
            st.warning(msg)
        else:
            msg = f"Độ nhạy cảm tại {location_name} thấp. Ô nhiễm chủ yếu do nguồn phát thải tại chỗ." if lang == "vi" else f"Weather sensitivity in {location_name} is low. Pollution is mainly from local emission sources."
            st.info(msg)
        
        st.caption("Phương pháp tính: So sánh nồng độ bụi khi lặng gió (<1m/s) và khi có gió (>2m/s) tại khu vực này.")

    st.markdown("---")

    # ── Row 3: Vulnerability Ranking (SQL Aggregated) ─────────────────────────
    st.subheader(t("weather_sensitivity_ranking", lang))
    
    df_rank = get_weather_ranking_data(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)

    if not df_rank.empty:
        fig_rank = px.bar(
            df_rank, 
            x="risk_index", 
            y="label_col", 
            color="influence_pct",
            orientation='h',
            labels={
                "risk_index": t("chart_label_type", lang) if lang=="en" else "Chỉ số rủi ro tích tụ", 
                "influence_pct": t("weather_influence", lang),
                "label_col": t("chart_label_area", lang)
            },
            title=f"{t('chart_top_polluted', lang)} ({target_poll.upper()})",
            color_continuous_scale="RdBu_r",
            color_continuous_midpoint=0
        )
        fig_rank.update_layout(get_plotly_layout(height=500))
        st.plotly_chart(fig_rank, use_container_width=True)

    st.markdown("---")

    # ── Row 4: Detailed Dispersal Analysis ────────────────────────────────────
    st.subheader(t("weather_dispersal_analysis", lang))
    if not df_hourly.empty:
        fig_scatter = px.scatter(
            df_hourly, x="avg_wind", y="avg_val", color="avg_hum",
            trendline="lowess",
            labels={
                "avg_wind": t("weather_wind_speed", lang), 
                "avg_val": f"{target_poll.upper()}", 
                "avg_hum": t("chart_label_area", lang) if lang=="en" else "Độ ẩm (%)"
            }
        )
        fig_scatter.update_traces(marker=dict(size=10))
        fig_scatter.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_scatter, use_container_width=True)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu thời tiết cho lựa chọn này."), use_container_width=True)
