import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("weather_title", lang))

@st.cache_data(ttl=3600)
def get_provinces():
    q = "SELECT DISTINCT province FROM air_quality.dm_aqi_weather_traffic_unified WHERE temp > 0 AND humidity > 0 ORDER BY province"
    return query_df(q)["province"].tolist()

@st.cache_data(ttl=300)
def get_weather_correlation(province: str | None = None):
    where_clause = "WHERE datetime_hour >= now() - INTERVAL 7 DAY AND temp > 0 AND humidity > 0 AND wind_speed > 0"
    if province:
        where_clause += f" AND province = '{province}'"
    
    q = f"""
    SELECT
        datetime_hour,
        province,
        avg(temp) as avg_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind,
        avg(pm25) as avg_pm25
    FROM air_quality.dm_aqi_weather_traffic_unified
    {where_clause}
    GROUP BY datetime_hour, province
    ORDER BY datetime_hour
    """
    return query_df(q)

def add_global_trendline(fig, df, x_col, y_col):
    """Add a single global OLS trendline to a colored scatter plot."""
    clean_df = df.dropna(subset=[x_col, y_col])
    if len(clean_df) < 2:
        return fig
    
    x = clean_df[x_col]
    y = clean_df[y_col]
    
    # Simple linear regression
    m, b = np.polyfit(x, y, 1)
    
    x_range = np.array([x.min(), x.max()])
    y_range = m * x_range + b
    
    fig.add_trace(go.Scatter(
        x=x_range, y=y_range,
        mode='lines',
        name='National Trend' if lang == 'en' else 'Xu hướng chung',
        line=dict(color='red' if st.session_state.get("theme") == "light" else "white", width=3, dash='dash'),
        showlegend=True
    ))
    return fig

# ── Filters ──────────────────────────────────────────────────────────────────
provinces = get_provinces()
national_label = "National" if lang == "en" else "Toàn quốc"
selected_province = st.selectbox(
    "Select Province/City" if lang == "en" else "Chọn tỉnh/thành phố",
    options=[national_label] + provinces,
    index=0,
)
province_arg = selected_province if selected_province != national_label else None

df = get_weather_correlation(province_arg)

if not df.empty:
    # Aggregated metrics for display cards
    avg_temp = df.avg_temp.mean()
    avg_hum = df.avg_humidity.mean()
    avg_wind = df.avg_wind.mean()

    # Top Metrics
    c1, c2, c3 = st.columns(3)
    
    with c1:
        temp_label = "Avg Temp" if lang == "en" else "Nhiệt độ TB"
        render_metric_card(temp_label, f"{avg_temp:.1f}°C", icon="device_thermostat")
    with c2:
        hum_label = "Humidity" if lang == "en" else "Độ ẩm TB"
        render_metric_card(hum_label, f"{avg_hum:.1f}%", icon="humidity_percentage")
    with c3:
        wind_label = "Wind Speed" if lang == "en" else "Tốc độ Gió"
        render_metric_card(wind_label, f"{avg_wind:.1f} m/s", icon="air")

    st.markdown("---")
    
    # --- Visualization Prep ---
    plot_df = df.copy()
    if province_arg is None:
        top_provinces = plot_df.groupby('province')['avg_pm25'].count().nlargest(10).index
        plot_df['province_display'] = plot_df['province'].apply(lambda x: x if x in top_provinces else "Others")
    else:
        plot_df['province_display'] = plot_df['province']

    # Section 1: Wind Dispersal Analysis
    wind_title = "Wind Speed vs PM2.5 (Dispersal Effect)" if lang == "en" else "Tác động của Gió: Phân tán ô nhiễm"
    st.subheader(wind_title)
    
    valid_wind = df.dropna(subset=['avg_wind', 'avg_pm25'])
    wind_corr = valid_wind[['avg_wind', 'avg_pm25']].corr().iloc[0,1] if len(valid_wind) > 2 else 0
    
    fig_wind = px.scatter(
        plot_df, x="avg_wind", y="avg_pm25", color="province_display",
        labels={"avg_wind": "Wind Speed (m/s)", "avg_pm25": "PM2.5", "province_display": ""}
    )
    
    if province_arg is None:
        fig_wind = add_global_trendline(fig_wind, df, 'avg_wind', 'avg_pm25')
    else:
        fig_wind.update_traces(selector=dict(mode='markers'), trendline="ols") # px.scatter handles OLS differently if called after
        # Re-creating with trendline for single province to be safe
        fig_wind = px.scatter(
            plot_df, x="avg_wind", y="avg_pm25", color="province_display",
            trendline="ols",
            labels={"avg_wind": "Wind Speed (m/s)", "avg_pm25": "PM2.5", "province_display": ""}
        )

    fig_wind.update_layout(get_plotly_layout())
    st.plotly_chart(fig_wind, use_container_width=True)
    
    # Dynamic Insight for Wind
    if wind_corr < -0.1:
        wind_msg = "Gió giúp phân tán bụi mịn hiệu quả." if lang == "vi" else "Wind effectively disperses PM2.5."
        wind_icon = "ℹ️"
    elif wind_corr > 0.1:
        wind_msg = "Hiện tượng bất thường: Gió mạnh kèm bụi (có thể do bụi thô/bão)." if lang == "vi" else "Anomalous: Strong wind with dust (possible coarse dust/storms)."
        wind_icon = "⚠️"
    else:
        wind_msg = "Tốc độ gió không ảnh hưởng rõ rệt đến nồng độ bụi." if lang == "vi" else "Wind speed has no significant impact on dust levels."
        wind_icon = "⚪"
        
    st.info(wind_msg, icon=wind_icon)
    st.write(f"Correlation: **{wind_corr:.2f}**")

    st.markdown("---")

    # Section 2: Humidity Analysis
    hum_title = "Humidity vs PM2.5 (Trap vs Scavenging)" if lang == "en" else "Độ ẩm vs PM2.5: Tụ khói bụi vs Gột rửa"
    st.subheader(hum_title)
    
    valid_hum = df.dropna(subset=['avg_humidity', 'avg_pm25'])
    hum_corr = valid_hum[['avg_humidity', 'avg_pm25']].corr().iloc[0,1] if len(valid_hum) > 2 else 0
    
    fig_hum = px.scatter(
        plot_df, x="avg_humidity", y="avg_pm25", color="province_display",
        labels={"avg_humidity": "Humidity (%)", "avg_pm25": "PM2.5", "province_display": ""}
    )
    
    if province_arg is None:
        fig_hum = add_global_trendline(fig_hum, df, 'avg_humidity', 'avg_pm25')
    else:
        fig_hum = px.scatter(
            plot_df, x="avg_humidity", y="avg_pm25", color="province_display",
            trendline="ols",
            labels={"avg_humidity": "Humidity (%)", "avg_pm25": "PM2.5", "province_display": ""}
        )

    fig_hum.update_layout(get_plotly_layout())
    st.plotly_chart(fig_hum, use_container_width=True)
    
    # Dynamic Insight for Humidity
    if hum_corr > 0.1:
        hum_msg = "Bẫy ô nhiễm: Độ ẩm cao đang làm bụi tích tụ." if lang == "vi" else "Pollution Trap: High humidity is causing dust accumulation."
        hum_type = "warning"
        hum_icon = "⚠️"
    elif hum_corr < -0.1:
        hum_msg = "Hiệu ứng gột rửa: Hơi nước và mưa giúp giảm nồng độ bụi." if lang == "vi" else "Scavenging Effect: Moisture and rain are reducing dust levels."
        hum_type = "info"
        hum_icon = "🌧️"
    else:
        hum_msg = "Độ ẩm không có tác động rõ rệt đến bụi mịn." if lang == "vi" else "Humidity has no significant impact on PM2.5."
        hum_type = "info"
        hum_icon = "⚪"
        
    if hum_type == "warning":
        st.warning(hum_msg, icon=hum_icon)
    else:
        st.info(hum_msg, icon=hum_icon)
        
    st.write(f"Correlation: **{hum_corr:.2f}**")

else:
    st.info(t("status_no_data", lang))
