import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib.clickhouse_client import query_df
from lib.data_service import get_hierarchy_metadata, build_where_clause
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("traffic_title", lang))

# ── Filters (Hierarchical) ────────────────────────────────────────────────────
hierarchy_df = get_hierarchy_metadata()

st.sidebar.markdown(f"### 📍 {t('nav_traffic', lang)} Filters")

spatial_grain = st.sidebar.selectbox(
    "Cấp độ hiển thị" if lang == "vi" else "Spatial Grain",
    ["Toàn quốc", "Vùng", "Khu vực", "Tỉnh"],
    index=0
)

scope_val = None
if spatial_grain == "Vùng":
    scope_val = st.sidebar.selectbox("Chọn miền", sorted(hierarchy_df['region_3'].unique()))
elif spatial_grain == "Khu vực":
    scope_val = st.sidebar.selectbox("Chọn khu vực", sorted(hierarchy_df['region_8'].unique()))
elif spatial_grain == "Tỉnh":
    scope_val = st.sidebar.selectbox("Chọn tỉnh/thành", sorted(hierarchy_df['province'].unique()))

TIME_OPTIONS = {7: "7d", 30: "30d", 90: "3m"}
days = st.sidebar.selectbox(
    "Khoảng thời gian" if lang == "vi" else "Time Interval",
    options=list(TIME_OPTIONS.keys()),
    format_func=lambda x: TIME_OPTIONS[x],
    index=0,
)

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(days: int, where: str):
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg(congestion_index) as avg_congestion,
        avg(pm25) as avg_pm25,
        avg(co) as avg_co
    FROM air_quality.fct_aqi_weather_traffic_unified
    WHERE {where} AND datetime_hour >= now() - INTERVAL {days} DAY
    GROUP BY datetime_hour
    ORDER BY datetime_hour
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_daily(where: str):
    q = f"""
    SELECT
        province,
        location_type,
        avg(pm25_daily_avg) as pm25_daily_avg,
        avg(congestion_daily_avg) as congestion_daily_avg,
        avg(traffic_pollution_impact_score) as traffic_pollution_impact_score,
        avg(traffic_contribution_pct) as traffic_contribution_pct
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where}
    GROUP BY province, location_type
    ORDER BY traffic_pollution_impact_score DESC
    """
    return query_df(q)

where_clause = build_where_clause(spatial_grain, scope_val)
df_hourly = get_traffic_correlation_hourly(days, where_clause)
df_daily = get_traffic_summary_daily(where_clause)

if not df_hourly.empty:
    # ── Row 1: KPI Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    
    avg_traffic = df_hourly.avg_congestion.mean()
    traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"

    # Contribution %
    contrib_pct = df_daily['traffic_contribution_pct'].mean() if not df_daily.empty else 0.0
    
    # Impact Score
    impact_score = df_daily['traffic_pollution_impact_score'].mean() if not df_daily.empty else 0.0

    with c1:
        render_metric_card(t("nav_traffic", lang), traffic_display, icon="traffic")
    with c2:
        contrib_label = t("traffic_contribution", lang) if lang=="en" else "Tỷ lệ đóng góp Giao thông"
        render_metric_card(contrib_label, f"{contrib_pct:.1f}%", icon="pie_chart")
    with c3:
        impact_label = t("traffic_impact", lang) if lang=="en" else "Điểm ảnh hưởng Giao thông"
        render_metric_card(impact_label, f"{impact_score:.2f}", icon="analytics")

    st.markdown("---")
    
    # ── Row 2: Hourly Correlation Charts ──────────────────────────────────────
    st.subheader("Hourly Trend Correlation" if lang=="en" else "Tương quan theo giờ")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_congestion, name=t("nav_traffic", lang),
                  line=dict(color='#1f77b4', width=3)),
        secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_pm25, name="PM2.5",
                  fill='tozeroy', line=dict(color='#ff7f0e', width=2)),
        secondary_y=False,
    )
    fig.update_layout(get_plotly_layout(height=450), margin=dict(l=60, r=60, t=20, b=80), hovermode="x unified")
    fig.update_yaxes(title_text="PM2.5", secondary_y=False)
    fig.update_yaxes(title_text="Traffic", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: Hotspot Ranking ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Traffic-Pollution Hotspot Ranking" if lang == "en" else "Xếp hạng điểm nóng Ô nhiễm Giao thông")
    
    if not df_daily.empty:
        df_rank = df_daily.sort_values('traffic_pollution_impact_score', ascending=False).head(15)
        df_rank = df_rank.sort_values('traffic_pollution_impact_score', ascending=True)
        
        fig_rank = px.bar(
            df_rank, 
            x="traffic_pollution_impact_score", 
            y="province", 
            color="location_type",
            orientation='h',
            labels={"traffic_pollution_impact_score": "Impact Score", "province": ""},
            color_discrete_map={"Urban": "#00CC96", "Industrial": "#EF553B", "Rural": "#636EFA"}
        )
        fig_rank.update_layout(get_plotly_layout(height=500))
        st.plotly_chart(fig_rank, use_container_width=True)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu cho lựa chọn này."), use_container_width=True)
