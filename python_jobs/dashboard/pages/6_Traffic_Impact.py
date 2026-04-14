import streamlit as st
import pandas as pd
import plotly.express as px
from lib.clickhouse_client import query_df
from lib.style import render_metric_card, get_plotly_layout
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.lang

st.title(t("traffic_title", lang))

@st.cache_data(ttl=3600)
def get_provinces():
    q = "SELECT DISTINCT province FROM air_quality.dm_aqi_weather_traffic_unified ORDER BY province"
    return query_df(q)["province"].tolist()

@st.cache_data(ttl=300)
def get_traffic_correlation(province: str | None = None):
    where_clause = f"WHERE province = '{province}'" if province else ""
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        { " 'National' as province " if not province else " province " },
        avg(congestion_index) as avg_congestion,
        avg(pm25) as avg_pm25,
        avg(co) as avg_co
    FROM air_quality.dm_aqi_weather_traffic_unified
    {where_clause}
    GROUP BY datetime_hour, province
    ORDER BY datetime_hour
    """
    return query_df(q)

# ── Filters ──────────────────────────────────────────────────────────────────
provinces = get_provinces()
national_label = "National" if lang == "en" else "Toàn quốc"
selected_province = st.selectbox(
    "Select Province/City" if lang == "en" else "Chọn tỉnh/thành phố",
    options=[national_label] + provinces,
    index=0,
)
province_arg = selected_province if selected_province != national_label else None

df = get_traffic_correlation(province_arg)

if not df.empty:
    # Top Metrics
    c1, c2, c3 = st.columns(3)
    
    # Robust correlation calculation to avoid NAN (check variance)
    valid_corr_df = df.dropna(subset=['avg_congestion', 'avg_pm25'])
    has_variance = len(valid_corr_df) > 5 and valid_corr_df['avg_congestion'].std() > 0 and valid_corr_df['avg_pm25'].std() > 0
    
    if has_variance:
        corr = valid_corr_df[['avg_congestion', 'avg_pm25']].corr().iloc[0,1]
    else:
        corr = 0.0

    # Traffic Density (%)
    avg_traffic = df.avg_congestion.mean()
    traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"

    with c1:
        render_metric_card(t("nav_traffic", lang), traffic_display, icon="traffic")
    with c2:
        corr_label = "Correlation" if lang == "en" else "Tương quan"
        render_metric_card(corr_label, f"{corr:.2f}" if has_variance else "N/A", icon="insights")
    with c3:
        peak_label = "Peak PM2.5" if lang == "en" else "PM2.5 Đỉnh điểm"
        render_metric_card(peak_label, f"{df.avg_pm25.max():.1f}", icon="error")

    st.markdown("---")
    
    # Chart 1: Time Series Correlation (Dual Y-Axis)
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    timeseries_label = "Hourly Trend: Traffic & PM2.5" if lang == "en" else "Tương quan theo giờ: Giao thông & PM2.5"
    st.subheader(timeseries_label)
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Traffic (Line)
    fig.add_trace(
        go.Scatter(x=df.datetime_hour, y=df.avg_congestion, name=t("nav_traffic", lang),
                  line=dict(color='#1f77b4', width=3)),
        secondary_y=True,
    )

    # PM2.5 (Area)
    fig.add_trace(
        go.Scatter(x=df.datetime_hour, y=df.avg_pm25, name="PM2.5",
                  fill='tozeroy', line=dict(color='#ff7f0e', width=2)),
        secondary_y=False,
    )

    # Use a custom height and more generous margins
    fig.update_layout(get_plotly_layout(height=400))
    fig.update_layout(
        margin=dict(l=60, r=60, t=80, b=100), # Increased bottom margin for dates
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,  # Push legend further up
            xanchor="center",
            x=0.5    # Center the legend
        )
    )
    
    fig.update_yaxes(title_text="PM2.5 (µg/m³)", secondary_y=False)
    fig.update_yaxes(title_text=t("nav_traffic", lang), secondary_y=True, 
                     range=[0, max(df.avg_congestion.max() * 1.2, 0.2)])
    
    # Ensure X-axis doesn't have a rangeslider and labels are clear
    fig.update_xaxes(rangeslider_visible=False)
    
    st.plotly_chart(fig, use_container_width=True, config={'responsive': True, 'displayModeBar': False})

    # Chart 2: Scatter Plot
    analysis_label = "Correlation Depth Analysis" if lang == "en" else "Phân tích mức độ tương quan"
    st.subheader(analysis_label)
    
    # Only show trendline if we have enough data to avoid px error
    fig_scatter = px.scatter(
        df, x="avg_congestion", y="avg_pm25", color="province",
        trendline="ols" if has_variance else None,
        labels={"avg_congestion": t("nav_traffic", lang), "avg_pm25": "PM2.5"}
    )
    fig_scatter.update_layout(get_plotly_layout(height=400))
    fig_scatter.update_layout(margin=dict(l=50, r=50, t=50, b=50))
    st.plotly_chart(fig_scatter, use_container_width=True, config={'displayModeBar': False})
else:
    st.info(t("status_no_data", lang))
