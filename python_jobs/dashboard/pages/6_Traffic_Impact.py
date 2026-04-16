import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("traffic_title", lang))

@st.cache_data(ttl=3600)
def get_provinces():
    q = "SELECT DISTINCT province FROM air_quality.dm_aqi_weather_traffic_unified ORDER BY province"
    df = query_df(q)
    return df["province"].tolist() if not df.empty else []

@st.cache_data(ttl=3600)
def get_districts_traffic(province: str):
    q = f"SELECT DISTINCT district FROM air_quality.dm_aqi_weather_traffic_unified WHERE province = '{province}' AND district != '' ORDER BY district"
    df = query_df(q)
    return df["district"].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(days: int, province: str | None = None, district: str | None = None):
    where_clause = f"WHERE datetime_hour >= now() - INTERVAL {days} DAY"
    if province:
        where_clause += f" AND province = '{province}'"
        if district:
            where_clause += f" AND district = '{district}'"
    
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        { " 'Toàn quốc' as province " if not province else " province " },
        { " 'Tất cả' as district " if not district else " district " },
        avg(congestion_index) as avg_congestion,
        avg(pm25) as avg_pm25,
        avg(co) as avg_co
    FROM air_quality.dm_aqi_weather_traffic_unified
    {where_clause}
    GROUP BY datetime_hour, province, district
    ORDER BY datetime_hour
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_daily(province: str | None = None, district: str | None = None):
    where_clause = ""
    if province:
        where_clause = f"WHERE province = '{province}'"
        if district:
            where_clause += f" AND district = '{district}'"
    
    q = f"""
    SELECT
        province,
        district,
        location_type,
        pm25_daily_avg,
        congestion_daily_avg,
        traffic_pollution_impact_score,
        traffic_contribution_pct,
        overall_traffic_impact_rank
    FROM air_quality.fct_traffic_pollution_correlation_daily
    {where_clause}
    ORDER BY traffic_pollution_impact_score DESC
    """
    return query_df(q)

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
            districts = get_districts_traffic(province_arg)
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
                disabled=True,
                key="traffic_dist_disabled"
            )
    
    with c3:
        TIME_OPTIONS = {7: "7d", 30: "30d", 90: "3m"}
        days = st.selectbox(
            "Time Interval" if lang == "en" else "Khoảng thời gian",
            options=list(TIME_OPTIONS.keys()),
            format_func=lambda x: TIME_OPTIONS[x],
            index=0,
        )
    st.markdown('</div>', unsafe_allow_html=True)

# ── Data Fetching ─────────────────────────────────────────────────────────────
df_hourly = get_traffic_correlation_hourly(days, province_arg, district_arg)
df_daily = get_traffic_summary_daily(province_arg, district_arg)

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
        contrib_label = "Traffic Contribution %" if lang == "en" else "Tỷ lệ đóng góp Giao thông"
        render_metric_card(contrib_label, f"{contrib_pct:.1f}%", icon="pie_chart")
    with c3:
        impact_label = "Traffic Impact Score" if lang == "en" else "Điểm ảnh hưởng Giao thông"
        render_metric_card(impact_label, f"{impact_score:.2f}", icon="analytics")

    st.markdown("---")
    
    # ── Row 2: Contribution Analysis (Gauge/Visual) ───────────────────────────
    st.subheader(contrib_label)
    col_gauge, col_text = st.columns([1, 1])
    
    with col_gauge:
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = contrib_pct,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "%"},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "#EF553B"},
                'steps': [
                    {'range': [0, 10], 'color': "#00CC96"},
                    {'range': [10, 30], 'color': "#FECB52"},
                    {'range': [30, 100], 'color': "#FF6692"}
                ],
            }
        ))
        fig_gauge.update_layout(height=250, margin=dict(t=50, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_text:
        st.write("")
        st.write("")
        location_name = district_arg if district_arg else (province_arg if province_arg else "Toàn quốc")
        if contrib_pct > 20:
            msg = f"Giao thông tại {location_name} đóng góp đáng kể vào nồng độ bụi mịn." if lang == "vi" else f"Traffic in {location_name} significantly contributes to PM2.5."
            st.warning(msg)
        elif contrib_pct > 0:
            msg = f"Giao thông tại {location_name} có ảnh hưởng một phần." if lang == "vi" else f"Traffic in {location_name} has some impact."
            st.info(msg)
        else:
            msg = f"Không phát hiện đóng góp rõ rệt từ giao thông tại {location_name}." if lang == "vi" else f"No significant traffic contribution in {location_name}."
            st.info(msg)
        
        st.caption("Phương pháp tính: So sánh nồng độ bụi trung bình ngày với nồng độ nền (2AM-4AM) tại khu vực này.")

    st.markdown("---")
    
    # ── Row 3: Hourly Correlation Charts ──────────────────────────────────────
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
    fig.update_layout(get_plotly_layout(height=400), margin=dict(l=60, r=60, t=20, b=80), hovermode="x unified")
    fig.update_yaxes(title_text="PM2.5", secondary_y=False)
    fig.update_yaxes(title_text="Traffic", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 4: Smart Ranking ──────────────────────────────────────────────────
    st.markdown("---")
    rank_title = "Traffic-Pollution Hotspot Ranking" if lang == "en" else "Xếp hạng điểm nóng Ô nhiễm Giao thông"
    st.subheader(rank_title)
    
    full_ranking = get_traffic_summary_daily()
    if not full_ranking.empty:
        # Smart Ranking: show districts if province selected
        rank_df = df_daily.head(15) if province_arg else full_ranking.head(15)
        y_col = "district" if province_arg else "province"
        
        fig_rank = px.bar(
            rank_df, 
            x="traffic_pollution_impact_score", 
            y=y_col, 
            color="location_type",
            orientation='h',
            labels={"traffic_pollution_impact_score": "Impact Score", y_col: ""},
            title="Top 15 Locations by Traffic-Pollution Impact" if lang == "en" else "Top 15 khu vực có tổng ảnh hưởng Giao thông cao nhất",
            color_discrete_map={"Urban": "#00CC96", "Industrial": "#EF553B", "Rural": "#636EFA"}
        )
        fig_rank.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_rank, use_container_width=True)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu cho lựa chọn này."), use_container_width=True)
