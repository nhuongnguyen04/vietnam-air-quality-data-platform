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

st.title(t("weather_title", lang))

@st.cache_data(ttl=3600)
def get_provinces():
    q = "SELECT DISTINCT province FROM air_quality.fct_aqi_weather_traffic_unified ORDER BY province"
    df = query_df(q)
    return df["province"].tolist() if not df.empty else []

@st.cache_data(ttl=3600)
def get_wards_weather(province: str):
    q = f"SELECT DISTINCT ward_code, ward_name FROM air_quality.stg_core__administrative_units WHERE province = '{province}' AND ward_code != '' ORDER BY ward_name"
    df = query_df(q)
    return df if not df.empty else pd.DataFrame(columns=["ward_code", "ward_name"])

@st.cache_data(ttl=300)
def get_weather_impact_daily(province: str | None = None, ward_code: str | None = None):
    where_clause = ""
    if province:
        where_clause = f"WHERE province = '{province}'"
        if ward_code:
            where_clause += f" AND ward_code = '{ward_code}'"
    
    q = f"""
    SELECT
        province,
        ward_code,
        pm25_daily_avg,
        temp_daily_avg,
        humidity_daily_avg,
        wind_daily_avg,
        wind_dispersal_risk_index,
        weather_influence_pct,
        stagnant_air_probability
    FROM air_quality.dm_weather_pollution_correlation_daily
    {where_clause}
    ORDER BY wind_dispersal_risk_index DESC
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_hourly_trend(days: int, province: str | None = None, ward_code: str | None = None):
    where_clause = f"WHERE datetime_hour >= now() - INTERVAL {days} DAY"
    if province:
        where_clause += f" AND province = '{province}'"
        if ward_code:
            where_clause += f" AND ward_code = '{ward_code}'"
            
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg(pm25) as avg_pm25,
        avg(wind_speed) as avg_wind,
        avg(humidity) as avg_hum
    FROM air_quality.dm_weather_hourly_trend
    {where_clause}
    GROUP BY datetime_hour
    ORDER BY datetime_hour
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
    ward_code_arg = None
    
    with c2:
        if province_arg:
            wards_df = get_wards_weather(province_arg)
            all_ward_label = "All Wards" if lang == "en" else "Tất cả các phường"
            # Create a display list of "Name (Code)" or just Name
            ward_options = [all_ward_label] + wards_df["ward_name"].tolist()
            selected_ward_name = st.selectbox(
                "Select Ward" if lang == "en" else "Chọn phường/xã",
                options=ward_options,
                index=0,
            )
            if selected_ward_name != all_ward_label:
                ward_code_arg = wards_df[wards_df["ward_name"] == selected_ward_name]["ward_code"].iloc[0]
        else:
            st.selectbox(
                "Select Ward" if lang == "en" else "Chọn phường/xã",
                options=["-"],
                disabled=True,
                key="weather_ward_disabled"
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
df_daily = get_weather_impact_daily(province_arg, ward_code_arg)
df_hourly = get_weather_hourly_trend(days, province_arg, ward_code_arg)

if not df_daily.empty:
    # ── Row 1: KPI Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    
    influence_pct = df_daily['weather_influence_pct'].mean()
    stagnant_prob = df_daily['stagnant_air_probability'].mean()
    avg_wind = df_daily['wind_daily_avg'].mean()

    with c1:
        render_metric_card("Weather Influence %" if lang=="en" else "Tỷ lệ ảnh hưởng Thời tiết", 
                          f"{influence_pct:.1f}%", icon="cloud")
    with c2:
        render_metric_card("Stagnant Air Risk" if lang=="en" else "Rủi ro Lặng gió", 
                          f"{stagnant_prob:.1%}", icon="ac_unit")
    with c3:
        render_metric_card("Wind Speed (Avg)" if lang=="en" else "Tốc độ gió (TB)", 
                          f"{avg_wind:.1f} m/s", icon="wind")

    st.markdown("---")

    # ── Row 2: Weather Influence Analysis ─────────────────────────────────────
    st.subheader("How does weather contribute to Air Quality?" if lang=="en" else "Thời tiết đóng góp thế nào vào chất lượng không khí?")
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
        location_name = selected_ward_name if ward_code_arg else (province_arg if province_arg else "Toàn quốc")
        if influence_pct > 30:
            msg = f"Độ nhạy cảm tại {location_name} rất cao. Thời tiết đóng vai trò then chốt trong ô nhiễm." if lang == "vi" else f"Weather sensitivity in {location_name} is very high."
            st.warning(msg)
        else:
            msg = f"Độ nhạy cảm tại {location_name} thấp. Ô nhiễm chủ yếu do nguồn phát thải tại chỗ." if lang == "vi" else f"Weather sensitivity in {location_name} is low."
            st.info(msg)
        
        st.caption("Phương pháp tính: So sánh nồng độ bụi khi lặng gió (<1m/s) và khi có gió (>2m/s) tại khu vực này.")

    st.markdown("---")

    # ── Row 3: Vulnerability Ranking ──────────────────────────────────────────
    st.subheader("Weather Sensitivity Ranking" if lang=="en" else "Xếp hạng điểm nóng tích tụ Ô nhiễm")
    
    full_ranking = get_weather_impact_daily()
    if not full_ranking.empty:
        # If province selected, show wards. If national, show top provinces nationwide.
        rank_df = df_daily.head(15) if province_arg else full_ranking.head(15)
        
        # When province is selected, we want to show ward names, but dm only has ward_code
        # Join with administrative units if necessary, or just use ward_code for now
        y_col = "ward_code" if province_arg else "province"
        
        fig_rank = px.bar(
            rank_df, 
            x="wind_dispersal_risk_index", 
            y=y_col, 
            color="weather_influence_pct",
            orientation='h',
            labels={"wind_dispersal_risk_index": "Dispersal Risk Index", "weather_influence_pct": "Weather Influence %"},
            title=f"Top 15 Locations by Weather Sensitivity" if lang == "en" else f"Top 15 khu vực nhạy cảm thời tiết nhất",
            color_continuous_scale="RdBu_r"
        )
        fig_rank.update_layout(get_plotly_layout(height=500))
        st.plotly_chart(fig_rank, use_container_width=True)

    st.markdown("---")

    # ── Row 4: Detailed Dispersal Analysis ────────────────────────────────────
    st.subheader("Wind Dispersal Depth Analysis" if lang=="en" else "Phân tích tác động của Gió")
    if not df_hourly.empty:
        fig_scatter = px.scatter(
            df_hourly, x="avg_wind", y="avg_pm25", color="avg_hum",
            trendline="lowess",
            labels={"avg_wind": "Wind Speed (m/s)", "avg_pm25": "PM2.5", "avg_hum": "Humidity %"}
        )
        fig_scatter.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_scatter, use_container_width=True)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu thời tiết cho lựa chọn này."), use_container_width=True)
