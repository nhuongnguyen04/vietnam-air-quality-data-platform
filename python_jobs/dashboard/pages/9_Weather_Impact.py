"""
Weather Impact page.
Analyzes temperature, humidity, wind speed, and atmospheric stagnation correlations with PM2.5/PM10.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state
from lib.text_to_sql_client import TextToSqlClient

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
def get_weather_ranking_data(grain: str, scope: str | None = None, dates=None, p_stag="p_stag", p_disp="p_disp", p_col="pm25_daily_avg"):
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
        LIMIT 12
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
        LIMIT 12
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
    else:
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
    else:
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

@page_wrapper("weather", "🌤️ Weather & Meteorological Impact", icon="🌤️")
def main(lang: str):
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_sidebar_filters()
    spatial_grain = filters["spatial_grain"]
    time_grain = filters.get("time_grain", "Ngày")
    time_unit = filters.get("time_unit", "day")
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]
    pollutant = filters.get("pollutant", "pm25")

    target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
    p_col = f"{target_poll}_daily_avg"
    stagnant_sum_col = f"stagnant_{target_poll}_sum"
    dispersive_sum_col = f"dispersive_{target_poll}_sum"

    if pollutant not in ["pm25", "pm10"]:
        st.warning(
            f"⚠️ Weather analysis for **{pollutant.upper()}** is currently calculated based on **{target_poll.upper()}** correlations."
            if lang == "en" else
            f"⚠️ Phân tích tương quan thời tiết cho **{pollutant.upper()}** hiện được ước tính gián tiếp dựa trên nồng độ bụi **{target_poll.upper()}**."
        )

    # ── Data Fetching ─────────────────────────────────────────────────────────────
    with st.spinner(t("loading", lang) if lang == "en" else "Đang tải dữ liệu khí tượng..."):
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

        # ── KPI Cards ──────────────────────────────────────────────────────────
        c1, c2, c3 = st.columns(3)
        with c1:
            render_metric_card(t("weather_influence", lang), f"{influence_pct:.1f}%", icon="insights")
        with c2:
            render_metric_card(t("weather_stagnant_risk", lang), f"{stagnant_prob:.1%}", icon="device_thermostat")
        with c3:
            render_metric_card(t("weather_wind_speed", lang), f"{avg_wind:.1f} m/s", icon="humidity_percentage")

        render_section_divider()

        # ── Weather Influence Explanation ─────────────────────────────────────
        st.markdown(f"#### ⚡ {t('weather_question', lang)}")
        c_left, c_right = st.columns([1, 1])
        with c_left:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=influence_pct,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "%", "font": {"family": "Outfit"}},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "#0891B2"},
                    'steps': [
                        {'range': [0, 25], 'color': "#10B981"},
                        {'range': [25, 50], 'color': "#EAB308"},
                        {'range': [50, 100], 'color': "#EF4444"}
                    ],
                }
            ))
            fig_gauge.update_layout(height=230, paper_bgcolor="rgba(0,0,0,0)", margin={"t": 30, "b": 10})
            st.plotly_chart(fig_gauge, use_container_width=True)

        with c_right:
            st.write("")
            location_name = scope_val if scope_val else ("Toàn quốc" if lang == "vi" else "National")
            if influence_pct > 35:
                st.warning(
                    f"⚠️ **Độ nhạy cảm thời tiết cao:** Tại **{location_name}**, thời tiết lặng gió và độ ẩm đóng vai trò quyết định "
                    f"trong việc làm tích tụ nồng độ bụi cực đoan."
                    if lang == "vi" else
                    f"⚠️ **High Weather Sensitivity:** At **{location_name}**, stagnant weather and humidity act as critical "
                    f"drivers for particulate accumulation."
                )
            else:
                st.info(
                    f"ℹ️ **Độ nhạy cảm thời tiết thấp:** Tại **{location_name}**, chất lượng không khí biến đổi chủ yếu phụ thuộc "
                    f"vào phát thải cơ sở địa phương thay vì tác động khuếch tán khí quyển."
                    if lang == "vi" else
                    f"ℹ️ **Low Weather Sensitivity:** At **{location_name}**, local ground emission sources dictate air quality "
                    f"more than daily meteorological fluctuations."
                )
            st.caption("💡 Chỉ số chênh lệch được tính toán so sánh nồng độ trung bình giữa các ngày lặng gió (< 1m/s) và ngày lộng gió (> 2m/s).")

        render_section_divider()

        # ── Vulnerability Ranking ─────────────────────────────────────────────
        st.markdown(f"#### 🏆 {t('weather_sensitivity_ranking', lang)}")
        df_rank = get_weather_ranking_data(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col, p_col=p_col)
        if not df_rank.empty:
            fig_rank = px.bar(
                df_rank, x="risk_index", y="label_col", color="influence_pct",
                orientation='h',
                labels={
                    "risk_index": "Chỉ số tích tụ rủi ro" if lang == "vi" else "Accumulation Risk Index",
                    "influence_pct": t("weather_influence", lang),
                    "label_col": t("province", lang)
                },
                color_continuous_scale="RdBu_r"
            )
            fig_rank.update_layout(get_plotly_layout(height=380, compact=True))
            st.plotly_chart(fig_rank, use_container_width=True)

        render_section_divider()

        # ── Timeseries Subplots ───────────────────────────────────────────────
        st.markdown(f"#### 📈 {t('weather_param_charts', lang)}")
        if not df_trend.empty:
            fig_sub = make_subplots(
                rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                subplot_titles=[t("weather_temperature", lang), t("weather_humidity", lang), t("weather_wind_speed", lang)]
            )
            fig_sub.add_trace(go.Scatter(x=df_trend["time_key"], y=df_trend["avg_temp"], name="Temp", line={"color": "#F97316", "width": 3}), row=1, col=1)
            fig_sub.add_trace(go.Scatter(x=df_trend["time_key"], y=df_trend["avg_hum"], name="Humidity", line={"color": "#0ea5e9", "width": 2.5}, fill="tozeroy", fillcolor="rgba(14, 165, 233, 0.1)"), row=2, col=1)
            fig_sub.add_trace(go.Scatter(x=df_trend["time_key"], y=df_trend["avg_wind"], name="Wind Speed", line={"color": "#10b981", "width": 2.5}), row=3, col=1)
            
            fig_sub_layout = get_plotly_layout(height=520, compact=True)
            fig_sub_layout["hovermode"] = "x unified"
            fig_sub.update_layout(fig_sub_layout)
            st.plotly_chart(fig_sub, use_container_width=True)

        render_section_divider()

        # ── Correlation analysis tabs ─────────────────────────────────────────
        st.markdown(f"#### 🎯 {t('weather_correlation_title', lang)}")
        tab_scatter, tab_matrix, tab_overlay = st.tabs([
            "📈 Tương quan Phân tán" if lang == "vi" else "Scatter Plots",
            "🌡️ Ma trận Tương quan" if lang == "vi" else "Correlation Matrix",
            "📊 Biểu đồ Kết hợp" if lang == "vi" else "Overlay Charts"
        ])

        with tab_scatter:
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                fig_t = px.scatter(df_trend, x="avg_temp", y="avg_val", trendline="lowess", labels={"avg_temp": t("weather_temperature", lang), "avg_val": target_poll.upper()})
                fig_t.update_layout(get_plotly_layout(height=280, compact=True))
                st.plotly_chart(fig_t, use_container_width=True)
            with sc2:
                fig_h = px.scatter(df_trend, x="avg_hum", y="avg_val", trendline="lowess", labels={"avg_hum": t("weather_humidity", lang), "avg_val": target_poll.upper()})
                fig_h.update_layout(get_plotly_layout(height=280, compact=True))
                st.plotly_chart(fig_h, use_container_width=True)
            with sc3:
                fig_w = px.scatter(df_trend, x="avg_wind", y="avg_val", trendline="lowess", labels={"avg_wind": t("weather_wind_speed", lang), "avg_val": target_poll.upper()})
                fig_w.update_layout(get_plotly_layout(height=280, compact=True))
                st.plotly_chart(fig_wind if 'fig_wind' in locals() else fig_w, use_container_width=True)

        with tab_matrix:
            corr_cols = {"avg_val": target_poll.upper(), "avg_temp": "Temp", "avg_hum": "Humidity", "avg_wind": "Wind"}
            corr_df = df_trend[["avg_val", "avg_temp", "avg_hum", "avg_wind"]].rename(columns=corr_cols).corr()
            fig_heat = px.imshow(corr_df, text_auto=".2f", color_continuous_scale="RdBu_r", color_continuous_midpoint=0)
            fig_heat.update_layout(get_plotly_layout(height=350, compact=True))
            st.plotly_chart(fig_heat, use_container_width=True)

        with tab_overlay:
            if not df_corr.empty:
                fig_overlay = make_subplots(specs=[[{"secondary_y": True}]])
                fig_overlay.add_trace(go.Scatter(x=df_corr["time_key"], y=df_corr["avg_temp"], name=t("weather_temperature", lang), line={"color": "#F97316", "width": 2, "dash": "dash"}), secondary_y=False)
                fig_overlay.add_trace(go.Scatter(x=df_corr["time_key"], y=df_corr["avg_wind"], name=t("weather_wind_speed", lang), line={"color": "#10B981", "width": 2, "dash": "dot"}), secondary_y=False)
                fig_overlay.add_trace(go.Scatter(x=df_corr["time_key"], y=df_corr["avg_val"], name=target_poll.upper(), line={"color": "#EF4444", "width": 3}, fill="tozeroy", fillcolor="rgba(239, 68, 68, 0.05)"), secondary_y=True)
                
                fig_overlay_layout = get_plotly_layout(height=350, compact=True)
                fig_overlay_layout["hovermode"] = "x unified"
                fig_overlay.update_layout(fig_overlay_layout)
                st.plotly_chart(fig_overlay, use_container_width=True)

        render_section_divider()

        # ── Weather AI Chat Widget (Premium Redesign) ─────────────────────────
        st.markdown(f"#### 💬 {t('weather_chat_title', lang)}")
        
        # Premium prompt card wrapper
        st.markdown(f"""
        <div class="glass-card" style="padding: 1.25rem; border-left: 5px solid #0891B2;">
            <p style="margin:0; font-size:0.9rem; line-height:1.5;">{t('weather_chat_intro', lang)}</p>
        </div>
        """, unsafe_allow_html=True)

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

        # Quick templates in columns
        st.markdown(f"<div style='font-size:0.85rem; font-weight:600; margin:1rem 0 0.5rem 0;'>💡 Phím tắt nhanh gợi ý:</div>", unsafe_allow_html=True)
        q_cols = st.columns(2)
        with q_cols[0]:
            if st.button(t("weather_chat_suggest1", lang), use_container_width=True, key="weather_chat_suggest1_btn"):
                _set_weather_question(t("weather_chat_suggest1", lang))
                st.rerun()
        with q_cols[1]:
            if st.button(t("weather_chat_suggest2", lang), use_container_width=True, key="weather_chat_suggest2_btn"):
                _set_weather_question(t("weather_chat_suggest2", lang))
                st.rerun()

        st.text_input(
            "Đặt câu hỏi phân tích khí tượng:" if lang == "vi" else "Ask weather analytical prompt:",
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
            st.markdown("##### 💻 SQL gợi ý")
            if st.session_state.weather_preview_stale:
                st.warning(t("ask_data_preview_stale", lang))
            st.write(st.session_state.weather_preview.get("explanation", ""))
            st.code(st.session_state.weather_preview.get("sql", ""), language="sql")

        if st.session_state.weather_result:
            st.markdown("##### 📋 Kết quả truy vấn")
            rows = st.session_state.weather_result.get("rows", [])
            columns = st.session_state.weather_result.get("columns", [])
            if rows:
                st.dataframe(pd.DataFrame(rows, columns=columns), hide_index=True)
            else:
                st.info("Truy vấn không trả về dòng nào." if lang == "vi" else "Query returned no rows.")

        if st.session_state.weather_error:
            st.error(f"Lỗi: {st.session_state.weather_error}")

    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu thời tiết cho lựa chọn này."), use_container_width=True)

if __name__ == "__main__":
    main()
