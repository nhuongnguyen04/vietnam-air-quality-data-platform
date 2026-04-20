import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
from lib.filters import render_sidebar_filters

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("traffic_title", lang))

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters.get("pollutant", "pm25")

# ── Dynamic Mapping ───────────────────────────────────────────────────────────
target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
p_col = f"{target_poll}_daily_avg"
sum_col = f"sum_{target_poll}"
bg_sum_col = f"background_{target_poll}_sum"

if pollutant not in ["pm25", "pm10"]:
    st.warning(f"⚠️ Traffic impact for **{pollutant.upper()}** is currently calculated based on **{target_poll.upper()}** correlations.")

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(dates, grain, scope, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"
    
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg(congestion_index) as avg_congestion,
        avg({target_col}) as avg_p
    FROM air_quality.dm_traffic_hourly_trend
    WHERE {where_clause}
    GROUP BY datetime_hour
    ORDER BY datetime_hour
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_stats(grain: str, scope: str | None = None, dates=None, p_sum="sum_pm25", p_bg="background_pm25_sum"):
    where_clause = build_where_clause(grain, scope, dates)
    
    q = f"""
    WITH stats_cte AS (
        SELECT
            sum({p_sum}) / nullif(sum(total_hours), 0) as avg_total,
            sum({p_bg}) / nullif(sum(background_hours), 0) as avg_bg,
            avg(congestion_daily_avg) as avg_congestion,
            avg(traffic_pollution_impact_score) as avg_impact
        FROM air_quality.dm_traffic_pollution_correlation_daily
        WHERE {where_clause}
    )
    SELECT
        *,
        (avg_total - avg_bg) / nullif(avg_total, 0) * 100 as contribution_pct
    FROM stats_cte
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_ranking_data(grain: str, scope: str | None = None, dates=None):
    where_clause = build_where_clause(grain, scope, dates)
    y_col = "ward_code" if grain == "Phường" else "province"
    
    q = f"""
    SELECT
        {y_col} as label_col,
        any(location_type) as location_type,
        avg(traffic_pollution_impact_score) as impact_score
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause} AND {y_col} != ''
    GROUP BY label_col
    ORDER BY impact_score DESC
    LIMIT 15
    """
    return query_df(q)

df_hourly = get_traffic_correlation_hourly(date_range, spatial_grain, scope_val, col=target_poll)
df_summary = get_traffic_summary_stats(spatial_grain, scope_val, date_range, p_sum=sum_col, p_bg=bg_sum_col)
df_rank = get_traffic_ranking_data(spatial_grain, scope_val, date_range)

if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_total):
    stats = df_summary.iloc[0]
    avg_traffic = stats.avg_congestion
    contrib_pct = stats.contribution_pct
    impact_score = stats.avg_impact

    # ── Row 1: KPI Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    
    traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"

    with c1:
        render_metric_card(t("nav_traffic", lang), traffic_display, icon="traffic")
    with c2:
        render_metric_card(t("traffic_contribution", lang), f"{contrib_pct:.1f}%", icon="pie_chart")
    with c3:
        render_metric_card(t("traffic_impact", lang), f"{impact_score:.2f}", icon="analytics")

    st.markdown("---")
    
    # ── Row 2: Hourly Correlation Charts ──────────────────────────────────────
    st.subheader(t("traffic_hourly_correlation", lang))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_congestion, name=t("nav_traffic", lang),
                  line=dict(color='#1f77b4', width=3)),
        secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_p, name=target_poll.upper(),
                  fill='tozeroy', line=dict(color='#ff7f0e', width=2)),
        secondary_y=False,
    )
    fig.update_layout(get_plotly_layout(height=450), margin=dict(l=60, r=60, t=20, b=80), hovermode="x unified")
    fig.update_yaxes(title_text=target_poll.upper(), secondary_y=False)
    fig.update_yaxes(title_text=t("nav_traffic", lang), secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: Hotspot Ranking ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(t("traffic_hotspot_ranking", lang))
    
    if not df_rank.empty:
        # Sort for display
        df_rank_plot = df_rank.sort_values('impact_score', ascending=True)
        
        # Map location types
        loc_map = {
            "Urban": t("location_urban", lang),
            "Industrial": t("location_industrial", lang),
            "Rural": t("location_rural", lang)
        }
        df_rank_plot["loc_label"] = df_rank_plot["location_type"].map(loc_map).fillna(df_rank_plot["location_type"])
        
        color_map = {
            t("location_urban", lang): "#00CC96",
            t("location_industrial", lang): "#EF553B",
            t("location_rural", lang): "#636EFA"
        }

        fig_rank = px.bar(
            df_rank_plot, 
            x="impact_score", 
            y="label_col", 
            color="loc_label",
            orientation='h',
            labels={"impact_score": t("traffic_impact", lang), "label_col": t("chart_label_area", lang), "loc_label": t("chart_label_type", lang)},
            color_discrete_map=color_map,
            title=f"{t('chart_top_polluted', lang)} ({target_poll.upper()})"
        )
        fig_rank.update_layout(get_plotly_layout(height=500))
        st.plotly_chart(fig_rank, use_container_width=True)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu cho lựa chọn này."), use_container_width=True)
