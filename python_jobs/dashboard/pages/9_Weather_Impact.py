"""
Trang Ảnh hưởng Thời tiết (Weather Impact) phân tích sự tác động của các yếu tố khí tượng
(nhiệt độ, độ ẩm, tốc độ gió) lên nồng độ chất ô nhiễm. Giúp hiểu rõ cơ chế phát tán
hoặc tích tụ bụi mịn trong các điều kiện thời tiết khác nhau.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.style import get_plotly_layout, render_metric_card
from lib.text_to_sql_client import TextToSqlClient

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("weather_title", lang))

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_grain = filters.get("time_grain", "Ngày")
time_unit = filters.get("time_unit", "day")
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
    
    if grain == "Phường":
        q = f"""
        WITH rank_cte AS (
            SELECT
                w.ward_code as label_key,
                any(coalesce(nullIf(d.ward_name, ''), w.ward_code)) as label_col,
                avg({p_col} / nullif(wind_daily_avg, 0)) as risk_index,
                sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
                sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp
            FROM air_quality.dm_weather_pollution_correlation_daily w
            LEFT JOIN air_quality.dim_administrative_units d ON w.ward_code = d.ward_code
            WHERE {where_clause} AND w.ward_code != ''
            GROUP BY label_key
        )
        SELECT
            *,
            (avg_stag - avg_disp) / nullif(avg_stag, 0) * 100 as influence_pct
        FROM rank_cte
        ORDER BY risk_index DESC
        LIMIT 15
        """
    else:
        q = f"""
        WITH rank_cte AS (
            SELECT
                province as label_col,
                avg({p_col} / nullif(wind_daily_avg, 0)) as risk_index,
                sum({p_stag}) / nullif(sum(stagnant_hours), 0) as avg_stag,
                sum({p_disp}) / nullif(sum(dispersive_hours), 0) as avg_disp
            FROM air_quality.dm_weather_pollution_correlation_daily
            WHERE {where_clause} AND province != ''
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
def get_weather_trend_data(grain: str, scope: str | None = None, dates=None, col="pm25", time_grain="Ngày"):
    target_col = col if col in ["pm25", "pm10", "aqi_vn", "aqi_us"] else "pm25"
    
    if time_grain == "Giờ":
        where_clause = build_where_clause(grain, scope, dates, time_unit="hour")
        q = f"""
        SELECT
            toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as time_key,
            avg({target_col}) as avg_val,
            avg(wind_speed) as avg_wind,
            avg(humidity) as avg_hum,
            avg(temperature) as avg_temp
        FROM air_quality.dm_weather_hourly_trend
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    elif time_grain == "Ngày":
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(date as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(wind_daily_avg) as avg_wind,
            avg(humidity_daily_avg) as avg_hum,
            avg(temp_daily_avg) as avg_temp
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    else: # Tháng (Monthly)
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(toStartOfMonth(date) as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(wind_daily_avg) as avg_wind,
            avg(humidity_daily_avg) as avg_hum,
            avg(temp_daily_avg) as avg_temp
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    return query_df(q)

@st.cache_data(ttl=300)
def get_weather_correlation_data(grain: str, scope: str | None = None, dates=None, col="pm25", time_grain="Ngày"):
    target_col = col if col in ["pm25", "pm10", "aqi_vn", "aqi_us"] else "pm25"
    
    if time_grain == "Giờ":
        where_clause = build_where_clause(grain, scope, dates, time_unit="hour")
        q = f"""
        SELECT
            toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as time_key,
            avg({target_col}) as avg_val,
            avg(temperature) as avg_temp,
            avg(humidity) as avg_hum,
            avg(wind_speed) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_hourly_trend
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    elif time_grain == "Ngày":
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(date as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(temp_daily_avg) as avg_temp,
            avg(humidity_daily_avg) as avg_hum,
            avg(wind_daily_avg) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    else: # Tháng (Monthly)
        where_clause = build_where_clause(grain, scope, dates, time_unit="day")
        p_col = f"{target_col}_daily_avg" if target_col in ["pm25", "pm10"] else "pm25_daily_avg"
        q = f"""
        SELECT
            cast(toStartOfMonth(date) as DateTime) as time_key,
            avg({p_col}) as avg_val,
            avg(temp_daily_avg) as avg_temp,
            avg(humidity_daily_avg) as avg_hum,
            avg(wind_daily_avg) as avg_wind,
            avg(stagnant_air_probability) as stagnant_prob
        FROM air_quality.dm_weather_pollution_correlation_daily
        WHERE {where_clause}
        GROUP BY time_key
        HAVING avg_temp IS NOT NULL
        ORDER BY time_key
        """
    return query_df(q)

# ── Data Fetching (Optimized SQL) ─────────────────────────────────────────────
df_summary = get_weather_summary_stats(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)
df_trend = get_weather_trend_data(spatial_grain, scope_val, date_range, col=target_poll, time_grain=time_grain)
df_corr = get_weather_correlation_data(spatial_grain, scope_val, date_range, col=target_poll, time_grain=time_grain)

if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_stag):
    stats = df_summary.iloc[0]
    influence_pct = stats.influence_pct
    stagnant_prob = stats.stagnant_prob
    avg_wind = stats.avg_wind

    if not df_trend.empty:
        df_trend["time_key"] = pd.to_datetime(df_trend["time_key"])
    if not df_corr.empty:
        df_corr["time_key"] = pd.to_datetime(df_corr["time_key"])

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
        fig_gauge.update_layout(height=250, margin={"t": 50, "b": 20})
        st.plotly_chart(fig_gauge, width='stretch')

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
        st.plotly_chart(fig_rank, width='stretch')

    st.markdown("---")

    # ── Row 4: Detailed Dispersal Analysis ────────────────────────────────────
    st.subheader(t("weather_dispersal_analysis", lang))
    if not df_trend.empty:
        fig_scatter = px.scatter(
            df_trend, x="avg_wind", y="avg_val", color="avg_hum",
            trendline="lowess",
            labels={
                "avg_wind": t("weather_wind_speed", lang),
                "avg_val": f"{target_poll.upper()}",
                "avg_hum": t("chart_label_area", lang) if lang=="en" else "Độ ẩm (%)"
            }
        )
        fig_scatter.update_traces(marker={"size": 10})
        fig_scatter.update_layout(get_plotly_layout(height=450))
        st.plotly_chart(fig_scatter, width='stretch')

    st.markdown("---")

    # ── Section 5: Weather Parameters Timeseries Subplots ─────────────────────
    st.subheader(t("weather_param_charts", lang))
    if not df_trend.empty:
        fig_sub = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=[
                t("weather_temperature", lang),
                t("weather_humidity", lang),
                t("weather_wind_speed", lang)
            ]
        )
        
        # Temp (Row 1)
        fig_sub.add_trace(
            go.Scatter(
                x=df_trend["time_key"],
                y=df_trend["avg_temp"],
                name=t("weather_temperature", lang),
                line=dict(color="#f97316", width=3),
                mode="lines"
            ),
            row=1, col=1
        )
        
        # Humidity (Row 2)
        fig_sub.add_trace(
            go.Scatter(
                x=df_trend["time_key"],
                y=df_trend["avg_hum"],
                name=t("weather_humidity", lang),
                fill="tozeroy",
                fillcolor="rgba(14, 165, 233, 0.15)",
                line=dict(color="#0ea5e9", width=2.5),
                mode="lines"
            ),
            row=2, col=1
        )
        
        # Wind Speed (Row 3)
        fig_sub.add_trace(
            go.Scatter(
                x=df_trend["time_key"],
                y=df_trend["avg_wind"],
                name=t("weather_wind_speed", lang),
                line=dict(color="#10b981", width=2.5),
                mode="lines+markers",
                marker=dict(size=4)
            ),
            row=3, col=1
        )
        
        fig_layout = get_plotly_layout(height=650)
        fig_layout["hovermode"] = "x unified"
        fig_sub.update_layout(fig_layout)

        if time_grain == "Giờ":
            x_hover_format = "%H:%M %d/%m/%Y"
            x_tick_format = "%H:%M<br>%d/%m"
        elif time_grain == "Ngày":
            x_hover_format = "%d/%m/%Y"
            x_tick_format = "%d/%m<br>%Y"
        else: # Tháng
            x_hover_format = "%m/%Y"
            x_tick_format = "%m/%Y"

        fig_sub.update_xaxes(tickformat=x_tick_format, hoverformat=x_hover_format)
        st.plotly_chart(fig_sub, width='stretch')

    st.markdown("---")

    # ── Section 6: Weather - Air Quality Correlation Tabs ───────────────────
    st.subheader(t("weather_correlation_title", lang))
    
    tab1, tab2, tab3 = st.tabs([
        "📈 Tương quan Phân tán" if lang == "vi" else "Scatter Plots",
        "🌡️ Ma trận Tương quan" if lang == "vi" else "Correlation Matrix",
        "📊 Biểu đồ Kết hợp" if lang == "vi" else "Daily Overlay"
    ])
    
    if not df_trend.empty:
        with tab1:
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                fig_temp = px.scatter(
                    df_trend, x="avg_temp", y="avg_val",
                    trendline="lowess",
                    trendline_color_override="#ef4444",
                    labels={"avg_temp": t("weather_temperature", lang), "avg_val": f"{target_poll.upper()}"},
                    title=t("weather_temp_vs_aqi", lang)
                )
                fig_temp.update_traces(marker=dict(size=6, color="#f97316", opacity=0.7))
                fig_temp.update_layout(get_plotly_layout(height=350))
                st.plotly_chart(fig_temp, use_container_width=True)
                
            with sc2:
                fig_hum = px.scatter(
                    df_trend, x="avg_hum", y="avg_val",
                    trendline="lowess",
                    trendline_color_override="#ef4444",
                    labels={"avg_hum": t("weather_humidity", lang), "avg_val": f"{target_poll.upper()}"},
                    title=t("weather_hum_vs_aqi", lang)
                )
                fig_hum.update_traces(marker=dict(size=6, color="#0ea5e9", opacity=0.7))
                fig_hum.update_layout(get_plotly_layout(height=350))
                st.plotly_chart(fig_hum, use_container_width=True)
                
            with sc3:
                fig_wind = px.scatter(
                    df_trend, x="avg_wind", y="avg_val",
                    trendline="lowess",
                    trendline_color_override="#ef4444",
                    labels={"avg_wind": t("weather_wind_speed", lang), "avg_val": f"{target_poll.upper()}"},
                    title=t("weather_wind_vs_aqi", lang)
                )
                fig_wind.update_traces(marker=dict(size=6, color="#10b981", opacity=0.7))
                fig_wind.update_layout(get_plotly_layout(height=350))
                st.plotly_chart(fig_wind, use_container_width=True)
                
        with tab2:
            corr_cols = {
                "avg_val": target_poll.upper(),
                "avg_temp": "Temperature" if lang == "en" else "Nhiệt độ",
                "avg_hum": "Humidity" if lang == "en" else "Độ ẩm",
                "avg_wind": "Wind Speed" if lang == "en" else "Tốc độ gió"
            }
            corr_df = df_trend[["avg_val", "avg_temp", "avg_hum", "avg_wind"]].rename(columns=corr_cols).corr()
            
            fig_heat = px.imshow(
                corr_df,
                text_auto=".2f",
                aspect="auto",
                color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                labels=dict(color=t("weather_corr_coeff", lang)),
                title=t("weather_heatmap_title", lang)
            )
            fig_heat.update_layout(get_plotly_layout(height=400))
            st.plotly_chart(fig_heat, use_container_width=True)
            
        with tab3:
            if not df_corr.empty:
                fig_overlay = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig_overlay.add_trace(
                    go.Scatter(
                        x=df_corr["time_key"], y=df_corr["avg_temp"],
                        name=t("weather_temperature", lang),
                        line=dict(color="#f97316", width=2, dash="dash"),
                    ),
                    secondary_y=False
                )
                
                fig_overlay.add_trace(
                    go.Scatter(
                        x=df_corr["time_key"], y=df_corr["avg_wind"],
                        name=t("weather_wind_speed", lang),
                        line=dict(color="#10b981", width=2, dash="dot"),
                    ),
                    secondary_y=False
                )
                
                fig_overlay.add_trace(
                    go.Scatter(
                        x=df_corr["time_key"], y=df_corr["avg_val"],
                        name=target_poll.upper(),
                        line=dict(color="#ef4444", width=3),
                        fill="tozeroy",
                        fillcolor="rgba(239, 68, 68, 0.05)"
                    ),
                    secondary_y=True
                )
                
                fig_overlay.update_yaxes(title_text=f"{t('weather_temperature', lang)} / {t('weather_wind_speed', lang)}", secondary_y=False)
                fig_overlay.update_yaxes(title_text=f"{target_poll.upper()} (µg/m³)", secondary_y=True)
                
                fig_overlay_layout = get_plotly_layout(height=450)
                fig_overlay_layout["hovermode"] = "x unified"
                fig_overlay_layout["title"] = t("weather_daily_overlay", lang) if time_grain == "Ngày" else "Tương quan theo giờ" if time_grain == "Giờ" else "Tương quan theo tháng"
                fig_overlay.update_layout(fig_overlay_layout)
                
                if time_grain == "Giờ":
                    x_hover_format = "%H:%M %d/%m/%Y"
                    x_tick_format = "%H:%M<br>%d/%m"
                elif time_grain == "Ngày":
                    x_hover_format = "%d/%m/%Y"
                    x_tick_format = "%d/%m<br>%Y"
                else: # Tháng
                    x_hover_format = "%m/%Y"
                    x_tick_format = "%m/%Y"

                fig_overlay.update_xaxes(tickformat=x_tick_format, hoverformat=x_hover_format)
                st.plotly_chart(fig_overlay, use_container_width=True)
            else:
                st.info("Không có dữ liệu cho lựa chọn này." if lang == "vi" else "No data for this selection.")

    st.markdown("---")

    # ── Weather AI Chat Widget ───────────────────────────────────────────────
    st.subheader(t("weather_chat_title", lang))
    st.caption(t("weather_chat_intro", lang))

    st.session_state.setdefault("weather_question", "")
    st.session_state.setdefault("weather_preview", None)
    st.session_state.setdefault("weather_preview_stale", False)
    st.session_state.setdefault("weather_result", None)
    st.session_state.setdefault("weather_error", None)

    def _mark_weather_preview_stale():
        if st.session_state.get("weather_preview"):
            st.session_state.weather_preview_stale = True
            st.session_state.weather_result = None

    def _set_weather_question(q):
        st.session_state.weather_question = q
        _mark_weather_preview_stale()

    st.caption("💡 " + (t("ask_data_examples_label", lang) if lang == "vi" else "Suggested Questions:"))
    sc_col1, sc_col2 = st.columns(2)
    with sc_col1:
        if st.button(t("weather_chat_suggest1", lang), use_container_width=True):
            _set_weather_question(t("weather_chat_suggest1", lang))
            st.rerun()
    with sc_col2:
        if st.button(t("weather_chat_suggest2", lang), use_container_width=True):
            _set_weather_question(t("weather_chat_suggest2", lang))
            st.rerun()

    st.text_input(
        "Hỏi Trợ lý AI về thời tiết & ô nhiễm:" if lang == "vi" else "Ask Weather AI Assistant:",
        key="weather_question",
        placeholder=t("weather_chat_placeholder", lang),
        on_change=_mark_weather_preview_stale
    )

    client = TextToSqlClient()

    c_btn1, c_btn2 = st.columns(2)
    with c_btn1:
        if st.button("Xem trước SQL thời tiết" if lang == "vi" else "Preview Weather SQL", type="primary", use_container_width=True, disabled=not st.session_state.weather_question.strip()):
            try:
                preview = client.preview(
                    question=st.session_state.weather_question,
                    lang=lang,
                    standard=st.session_state.get("standard", "VN_AQI"),
                    session_id=st.session_state.get("text_to_sql_session_id", "weather_session")
                )
                st.session_state.weather_preview = preview
                st.session_state.weather_preview_stale = False
                st.session_state.weather_result = None
                st.session_state.weather_error = None
                st.rerun()
            except Exception as e:
                st.session_state.weather_error = str(e)
    
    with c_btn2:
        execute_disabled = not st.session_state.weather_preview or st.session_state.weather_preview_stale
        if st.button("Chạy truy vấn thời tiết" if lang == "vi" else "Execute Weather Query", use_container_width=True, disabled=execute_disabled):
            try:
                result = client.execute(
                    sql=st.session_state.weather_preview["sql"],
                    preview_token=st.session_state.weather_preview["preview_token"]
                )
                st.session_state.weather_result = result
                st.session_state.weather_error = None
                st.rerun()
            except Exception as e:
                st.session_state.weather_error = str(e)

    if st.session_state.weather_preview:
        st.subheader("Bản nháp SQL gợi ý" if lang == "vi" else "Draft SQL Suggestion")
        if st.session_state.weather_preview_stale:
            st.warning(t("ask_data_preview_stale", lang))
        st.write(st.session_state.weather_preview.get("explanation", ""))
        st.code(st.session_state.weather_preview.get("sql", ""), language="sql")

    if st.session_state.weather_result:
        st.subheader("Kết quả truy vấn" if lang == "vi" else "Query Results")
        rows = st.session_state.weather_result.get("rows", [])
        columns = st.session_state.weather_result.get("columns", [])
        if rows:
            st.dataframe(pd.DataFrame(rows, columns=columns), hide_index=True)
        else:
            st.info("Truy vấn không trả về dòng nào." if lang == "vi" else "Query returned no rows.")

    if st.session_state.weather_error:
        st.error(f"Lỗi: {st.session_state.weather_error}" if lang == "vi" else f"Error: {st.session_state.weather_error}")

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu thời tiết cho lựa chọn này."), width='stretch')
