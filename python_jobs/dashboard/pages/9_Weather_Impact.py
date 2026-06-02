"""
Weather Impact page.
Analyzes temperature, humidity, wind speed, and atmospheric stagnation correlations with PM2.5/PM10.

Layout matches the high-fidelity reference dashboard design:
- Page Header (dynamic, localized)
- 4 Premium KPI Cards Row (Weather Influence, Temp, Wind, Stagnant Risk)
- Custom Warning/Info Callout Banner
- 3-Column Visual Grid:
  - Column 1: Temp vs PM2.5 Scatter with red trendline
  - Column 2: Wind Speed vs PM2.5 Scatter with downward trendline
  - Column 3: Stacked Meteorological Correlation HTML Matrix & Sensitivity HTML Progress List
- Down Arrow
- Bottom Insights & Analytical Ask AI
- Analytical AI Ask box (collapsible)
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import render_page_hero, render_section_divider, clean_html, page_wrapper
from lib.chart_config import get_plotly_layout, create_empty_state
from lib.ui_components import render_kpi_card
from lib.text_to_sql_client import TextToSqlClient

# ── Data Fetching Functions ──────────────────────────────────────────────────

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
        LIMIT 10
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
        LIMIT 10
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

# ── Styling Helpers & HTML Renderers ─────────────────────────────────────────

def clean_label(label: str) -> str:
    """Strip standard administration prefixes for clean layout presentation."""
    if not label:
        return ""
    for prefix in ["phường ", "Phường ", "xã ", "Xã ", "thị trấn ", "Thị trấn "]:
        if label.startswith(prefix):
            return label[len(prefix):]
    return label

def render_warning_callout(location_name: str, influence_pct: float, pollutant: str, lang: str):
    """Render a premium warning callout banner matching the amber styling in the mock."""
    is_high = influence_pct > 35
    if is_high:
        if lang == "vi":
            text = f"<b>Độ nhạy cảm thời tiết cao tại {location_name}:</b> gió yếu &lt;1.5m/s dự báo thời gian tới &rarr; nguy cơ tích tụ {pollutant.upper()} cực đoan."
        else:
            text = f"<b>High weather sensitivity in {location_name}:</b> weak wind &lt;1.5m/s forecast &rarr; extreme {pollutant.upper()} accumulation risk."
        bg = "#FFFBEB"
        border_col = "#F59E0B"
        color = "#78350F"
        icon = "⚠️"
    else:
        if lang == "vi":
            text = f"<b>Độ nhạy cảm thời tiết thấp tại {location_name}:</b> chất lượng không khí biến đổi chủ yếu phụ thuộc vào phát thải cơ sở địa phương."
        else:
            text = f"<b>Low weather sensitivity in {location_name}:</b> air quality variations dictated mainly by local ground emissions."
        bg = "#F0FDF4"
        border_col = "#22C55E"
        color = "#14532D"
        icon = "ℹ️"

    html = f"""
    <div style="
        background: {bg};
        border: 1px solid {border_col}33;
        border-left: 5px solid {border_col};
        border-radius: 8px;
        padding: 0.8rem 1rem;
        color: {color};
        font-size: 0.85rem;
        font-weight: 500;
        margin-bottom: 1.2rem;
        display: flex;
        align-items: center;
        gap: 0.65rem;
    ">
        <span style="font-size: 1.15rem; display: flex; align-items: center;">{icon}</span>
        <div style="line-height: 1.4;">{text}</div>
    </div>
    """
    st.markdown(clean_html(html), unsafe_allow_html=True)

def create_correlation_scatter(df, x_col, y_col, x_label, y_label, point_color, trend_color="#EF4444"):
    """Create a high-fidelity Plotly scatter plot with dashed OLS trendline matching the design."""
    theme = st.session_state.get("theme", "light")
    
    # Filter empty rows
    df_clean = df[[x_col, y_col]].dropna()
    if df_clean.empty:
        return None
        
    fig = px.scatter(
        df_clean, x=x_col, y=y_col,
        trendline="ols",
        labels={x_col: x_label, y_col: y_label}
    )
    
    # Style scatter markers
    fig.update_traces(
        marker=dict(
            size=6.5, 
            color=point_color, 
            opacity=0.8, 
            line=dict(width=0.5, color="rgba(255,255,255,0.2)")
        ), 
        selector=dict(type='scatter', mode='markers')
    )
    
    # Style OLS trendline to match red dashed style
    fig.update_traces(
        line=dict(color=trend_color, width=2, dash='dash'), 
        selector=dict(type='scatter', mode='lines')
    )
    
    layout = get_plotly_layout(height=230, compact=True)
    layout.update({
        "margin": {"t": 10, "b": 35, "l": 45, "r": 15},
        "xaxis": {
            "title": x_label,
            "showgrid": True,
            "gridcolor": "rgba(255,255,255,0.06)" if theme == "dark" else "rgba(0,0,0,0.05)",
            "zeroline": False,
            "title_font": {"size": 10, "family": "Inter", "weight": "bold"},
            "tickfont": {"size": 9, "family": "Inter"}
        },
        "yaxis": {
            "title": y_label,
            "showgrid": True,
            "gridcolor": "rgba(255,255,255,0.06)" if theme == "dark" else "rgba(0,0,0,0.05)",
            "zeroline": False,
            "title_font": {"size": 10, "family": "Inter", "weight": "bold"},
            "tickfont": {"size": 9, "family": "Inter"}
        }
    })
    fig.update_layout(layout)
    return fig

def generate_correlation_matrix_html(corr_df, target_poll: str) -> str:
    """Generate a custom styled HTML table for the meteorological correlation matrix."""
    rows_keys = ["avg_val", "avg_temp", "avg_hum", "avg_wind"]
    cols_keys = ["avg_val", "avg_temp", "avg_hum", "avg_wind"]
    
    label_map = {
        "avg_val": target_poll.upper(),
        "avg_temp": "Temp",
        "avg_hum": "Hum",
        "avg_wind": "Wind"
    }
    
    theme = st.session_state.get("theme", "light")
    lbl_color = "#94a3b8" if theme == "dark" else "#64748b"
    
    html = f"""
    <style>
    table.corr-table, table.corr-table th, table.corr-table td {{
        border: none !important;
    }}
    </style>
    <table class="corr-table" style="width:100%; border-collapse: separate; border-spacing: 5px; font-size: 0.8rem; text-align: center; margin-top: 5px; border: none !important;">
        <thead>
            <tr>
                <th style="color: {lbl_color}; font-weight: 600; text-align: left; padding: 2px; border: none !important; background: transparent !important;"></th>
                <th style="color: {lbl_color}; font-weight: 600; font-size: 0.75rem; border: none !important; background: transparent !important;">{target_poll.upper()}</th>
                <th style="color: {lbl_color}; font-weight: 600; font-size: 0.75rem; border: none !important; background: transparent !important;">Temp</th>
                <th style="color: {lbl_color}; font-weight: 600; font-size: 0.75rem; border: none !important; background: transparent !important;">Hum</th>
                <th style="color: {lbl_color}; font-weight: 600; font-size: 0.75rem; border: none !important; background: transparent !important;">Wind</th>
            </tr>
        </thead>
        <tbody>
    """
    
    for row_k in rows_keys:
        html += f'<tr><td style="color: {lbl_color}; font-weight: 600; text-align: left; padding: 4px 0; font-size: 0.75rem; border: none !important; background: transparent !important;">{label_map[row_k]}</td>'
        for col_k in cols_keys:
            val = corr_df.loc[row_k, col_k] if row_k in corr_df.index and col_k in corr_df.columns else float("nan")
            if pd.isna(val):
                val_str = "N/A"
                bg = "rgba(255,255,255,0.05)" if theme == "dark" else "rgba(0,0,0,0.03)"
                color = "#94a3b8"
            else:
                if abs(val - 1.0) < 1e-4:
                    val_str = "1.0"
                    bg = "#E15759"
                    color = "#ffffff"
                else:
                    val_str = f"{val:.2f}"
                    if val_str.startswith("0."):
                        val_str = val_str[1:]
                    elif val_str.startswith("-0."):
                        val_str = "-" + val_str[2:]
                        
                    # Color coding logic matching visual dashboard cells
                    if val < -0.5:
                        bg = "#D1FAE5" if theme == "light" else "#064E3B"
                        color = "#065F46" if theme == "light" else "#A7F3D0"
                    elif val < 0:
                        bg = "#E0F2FE" if theme == "light" else "#172554"
                        color = "#0369A1" if theme == "light" else "#93C5FD"
                    elif val < 0.4:
                        bg = "#FEF3C7" if theme == "light" else "#78350F"
                        color = "#92400E" if theme == "light" else "#FDE047"
                    else:
                        bg = "#FEE2E2" if theme == "light" else "#7F1D1D"
                        color = "#B91C1C" if theme == "light" else "#FCA5A5"
                        
            html += f'<td style="background: {bg}; color: {color}; font-weight: 700; border-radius: 4px; padding: 5px 2px; width: 44px; text-align: center; font-size: 0.78rem;">{val_str}</td>'
        html += "</tr>"
        
    html += "</tbody></table>"
    return clean_html(html)

def generate_rankings_html(df_rank, lang: str) -> str:
    """Generate custom horizontal progress lists for weather sensitivity hotspot rankings."""
    theme = st.session_state.get("theme", "light")
    text_color = "#f8fafc" if theme == "dark" else "#0f172a"
    bar_bg = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(0, 0, 0, 0.05)"
    
    # Sort and take top 5 hotspots as requested
    df_top = df_rank.sort_values(by="risk_index", ascending=False).head(5)
    if df_top.empty:
        return f"<div style='color: gray; font-size: 0.8rem; padding: 8px 0;'>{'No ranking data' if lang == 'en' else 'Không có dữ liệu xếp hạng'}</div>"
        
    max_risk = df_top["risk_index"].max()
    if max_risk == 0 or pd.isna(max_risk):
        max_risk = 1.0
        
    html = "<div style='display: flex; flex-direction: column; gap: 8px; margin-top: 10px;'>"
    for idx, (_, row) in enumerate(df_top.iterrows()):
        name = clean_label(row["label_col"])
        score = row["risk_index"]
        
        # Scale to match visual 0-10 format nicely
        scaled_score = (score / max_risk) * 8.4
        
        # Color codes matching high/medium/low severity (fine-grained color gradient)
        if idx == 0:
            bar_color = "#EF4444" # red
        elif idx == 1:
            bar_color = "#EA580C" # orange-red
        elif idx == 2:
            bar_color = "#D97706" # dark gold
        elif idx == 3:
            bar_color = "#F59E0B" # amber
        else:
            bar_color = "#10B981" # green
            
        width_pct = (scaled_score / 10.0) * 100
        
        html += f"""
        <div style="display: flex; align-items: center; justify-content: space-between;">
            <div style="width: 84px; font-size: 0.82rem; font-weight: 600; color: {text_color}; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="{row['label_col']}">
                {name}
            </div>
            <div style="flex-grow: 1; margin: 0 12px; height: 7px; background: {bar_bg}; border-radius: 4px; overflow: hidden; position: relative;">
                <div style="width: {width_pct}%; height: 100%; background: {bar_color}; border-radius: 4px;"></div>
            </div>
            <div style="width: 32px; text-align: right; font-size: 0.82rem; font-weight: 700; color: {text_color};">
                {scaled_score:.1f}
            </div>
        </div>
        """
    html += "</div>"
    return clean_html(html)

def _render_ai_chat(lang):
    """Collapsible AI Chat Analyst."""
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

    # Suggestions
    st.markdown("<div style='font-size:0.85rem; font-weight:600; margin:0.75rem 0 0.5rem 0;'>💡 Gợi ý nhanh:</div>", unsafe_allow_html=True)
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
        if st.button("Xem trước SQL" if lang == "vi" else "Preview SQL", type="primary", use_container_width=True, disabled=not st.session_state.weather_question.strip()):
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
        if st.button("Chạy truy vấn" if lang == "vi" else "Execute Query", use_container_width=True, disabled=execute_disabled):
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

# ── Main Page Redesign ───────────────────────────────────────────────────────

@page_wrapper("weather", "Ảnh hưởng thời tiết", icon="🌤️", skip_hero=True)
def main(lang):
    # Initialize session state triggers
    st.session_state.setdefault("show_ai_chat", False)

    # ── Page Initialization ────────────────────────────────────────────────────────

    # ── Filters ABOVE Title ────────────────────────────────────────────────────────
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    time_grain = filters.get("time_grain", "Ngày")
    time_unit = filters.get("time_unit", "day")
    scope_val = filters["scope_val"]
    date_range = filters["date_range"]
    pollutant = filters.get("pollutant", "pm25")

    # ── Page Title & Caption BELOW Filters ─────────────────────────────────────────
    title_text = t("weather_title", lang)
    if not title_text or title_text == "weather_title":
        title_text = t("weather", lang)
    if not title_text or title_text == "weather":
        title_text = "Ảnh hưởng thời tiết" if lang == "vi" else "Weather Impact Analysis"

    caption_text = t("weather_caption", lang)
    if not caption_text or caption_text == "weather_caption":
        caption_text = "Nhiệt độ · Độ ẩm · Gió · Lặng gió" if lang == "vi" else "Temperature · Humidity · Wind · Stagnant"

    st.markdown("<div style='margin-top: 0.75rem;'></div>", unsafe_allow_html=True)
    render_page_hero(title_text, caption_text, icon="🌤️")

    target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
    p_col = f"{target_poll}_daily_avg"
    stagnant_sum_col = f"stagnant_{target_poll}_sum"
    dispersive_sum_col = f"dispersive_{target_poll}_sum"

    if pollutant not in ["pm25", "pm10"]:
        st.warning(
            f"⚠️ Phân tích tương quan thời tiết cho **{pollutant.upper()}** hiện được ước tính dựa trên nồng độ bụi **{target_poll.upper()}**."
            if lang == "vi" else
            f"⚠️ Weather analysis for **{pollutant.upper()}** is currently approximated based on **{target_poll.upper()}**."
        )

    # ── Data Fetching ────────────────────────────────────────────────────────
    with st.spinner(t("loading", lang) if lang == "en" else "Đang tải dữ liệu khí tượng..."):
        df_summary = get_weather_summary_stats(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)
        df_trend = get_weather_trend_data(spatial_grain, scope_val, date_range, col=target_poll, time_grain=time_grain)
        df_corr = get_weather_correlation_data(spatial_grain, scope_val, date_range, col=target_poll, time_grain=time_grain)
        df_rank = get_weather_ranking_data(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col, p_col=p_col)

    if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_stag):
        stats = df_summary.iloc[0]
        influence_pct = stats.influence_pct if not pd.isna(stats.influence_pct) else 0.0
        stagnant_prob = stats.stagnant_prob if not pd.isna(stats.stagnant_prob) else 0.0
        avg_wind = stats.avg_wind if not pd.isna(stats.avg_wind) else 0.0

        if not df_trend.empty:
            df_trend["time_key"] = pd.to_datetime(df_trend["time_key"])
        if not df_corr.empty:
            df_corr["time_key"] = pd.to_datetime(df_corr["time_key"])

        avg_temp = df_trend.avg_temp.mean() if not df_trend.empty and "avg_temp" in df_trend.columns else float("nan")
        avg_hum = df_trend.avg_hum.mean() if not df_trend.empty and "avg_hum" in df_trend.columns else float("nan")

        # ── 1. KPI Cards Row ─────────────────────────────────────────────────
        c_kpi1, c_kpi2, c_kpi3, c_kpi4 = st.columns(4)
        
        # Color codes matching the high-fidelity aesthetics
        influence_color = "#D97706" if influence_pct > 35 else None
        stagnant_color = "#EF4444" if stagnant_prob > 0.20 else None

        with c_kpi1:
            render_kpi_card(
                t("weather_influence", lang) if lang == "en" else "Ảnh hưởng thời tiết",
                f"{influence_pct:.1f}%",
                "stagnant/windy diff" if lang == "en" else "chênh lệch lặng/gió",
                val_color=influence_color
            )
        with c_kpi2:
            temp_display = f"{avg_temp:.1f}°C" if not pd.isna(avg_temp) else "N/A"
            render_kpi_card(
                "Avg Temperature" if lang == "en" else "Nhiệt độ TB",
                temp_display,
                "in selected period" if lang == "en" else "trong kỳ chọn"
            )
        with c_kpi3:
            wind_display = f"{avg_wind:.1f} m/s" if not pd.isna(avg_wind) else "N/A"
            # Dynamic contextual subtexts
            if avg_wind < 1.5:
                wind_sub = "weak &rarr; accumulation risk" if lang == "en" else "yếu &rarr; nguy cơ tích tụ"
            elif avg_wind < 3.0:
                wind_sub = "moderate &rarr; avg dispersion" if lang == "en" else "vừa &rarr; khuếch tán trung bình"
            else:
                wind_sub = "strong &rarr; fast dispersion" if lang == "en" else "mạnh &rarr; khuếch tán nhanh"
            
            render_kpi_card(
                t("weather_wind_speed", lang) if lang == "en" else "Tốc độ gió TB",
                wind_display,
                wind_sub
            )
        with c_kpi4:
            stagnant_pct = stagnant_prob * 100
            render_kpi_card(
                "Stagnant Air Risk" if lang == "en" else "Rủi ro lặng gió",
                f"{stagnant_pct:.1f}%",
                "probability in period" if lang == "en" else "xác suất ngày hiện tại",
                val_color=stagnant_color
            )

        # ── 2. Callout Warning Banner ────────────────────────────────────────
        loc_name = scope_val if scope_val else ("National" if lang == "en" else "Toàn quốc")
        render_warning_callout(loc_name, influence_pct, target_poll, lang)

        # ── 3. Main 3-Column Visual Grid ─────────────────────────────────────
        theme = st.session_state.get("theme", "light")
        text_color = "#f8fafc" if theme == "dark" else "#0f172a"
        sub_color = "#94a3b8" if theme == "dark" else "#64748b"
        card_bg = "rgba(15, 23, 42, 0.65)" if theme == "dark" else "rgba(255, 255, 255, 0.85)"
        border_color = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(226, 232, 240, 0.8)"
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.2)" if theme == "dark" else "0 4px 6px -1px rgba(0, 0, 0, 0.05)"
        glass_blur = "blur(12px)" if theme == "dark" else "blur(8px)"

        # Row 1: 3 Scatter Plots
        c_grid1, c_grid2, c_grid3 = st.columns([1.0, 1.0, 1.0], gap="medium")

        # ── Column 1: Temp vs Pollutant
        with c_grid1:
            st.markdown(f"##### Temp vs {target_poll.upper()} (scatter + trendline)")
            fig_t = create_correlation_scatter(
                df_trend, "avg_temp", "avg_val", 
                "Temp (°C)", f"{target_poll.upper()} (µg/m³)", 
                "#3B82F6"
            )
            if fig_t:
                st.plotly_chart(fig_t, use_container_width=True)
                r_temp = df_trend["avg_temp"].corr(df_trend["avg_val"]) if not df_trend.empty else float("nan")
                if not pd.isna(r_temp):
                    dir_text = "giảm" if r_temp < 0 else "tăng"
                    dir_text_en = "decreases" if r_temp < 0 else "increases"
                    cap_text = f"r = {r_temp:.2f} · Nhiệt độ cao &rarr; {target_poll.upper()} {dir_text}" if lang == "vi" else f"r = {r_temp:.2f} · High Temp &rarr; {target_poll.upper()} {dir_text_en}"
                else:
                    cap_text = "r = N/A"
                st.markdown(f"<div style='font-size: 0.82rem; font-weight: 600; color: {sub_color}; margin-top: -5px;'>{cap_text}</div>", unsafe_allow_html=True)
            else:
                st.plotly_chart(create_empty_state("No data", height=230), use_container_width=True)

        # ── Column 2: Wind vs Pollutant
        with c_grid2:
            st.markdown(f"##### Wind vs {target_poll.upper()}")
            fig_w = create_correlation_scatter(
                df_trend, "avg_wind", "avg_val", 
                "Wind (m/s)", f"{target_poll.upper()} (µg/m³)", 
                "#F59E0B"
            )
            if fig_w:
                st.plotly_chart(fig_w, use_container_width=True)
                r_wind = df_trend["avg_wind"].corr(df_trend["avg_val"]) if not df_trend.empty else float("nan")
                if not pd.isna(r_wind):
                    if r_wind < -0.5:
                        strength = "tương quan nghịch mạnh" if lang == "vi" else "strong negative correlation"
                    elif r_wind > 0.5:
                        strength = "tương quan thuận mạnh" if lang == "vi" else "strong positive correlation"
                    else:
                        strength = "tương quan yếu" if lang == "vi" else "weak correlation"
                    
                    cap_text = f"r = {r_wind:.2f} · {strength.capitalize()}"
                else:
                    cap_text = "r = N/A"
                st.markdown(f"<div style='font-size: 0.82rem; font-weight: 600; color: {sub_color}; margin-top: -5px;'>{cap_text}</div>", unsafe_allow_html=True)
            else:
                st.plotly_chart(create_empty_state("No data", height=230), use_container_width=True)

        # ── Column 3: Humidity vs Pollutant
        with c_grid3:
            st.markdown(f"##### Hum vs {target_poll.upper()} (scatter + trendline)" if lang == "en" else f"##### Độ ẩm vs {target_poll.upper()}")
            fig_h = create_correlation_scatter(
                df_trend, "avg_hum", "avg_val", 
                "Humidity (%)" if lang == "en" else "Độ ẩm (%)", f"{target_poll.upper()} (µg/m³)", 
                "#0EA5E9"
            )
            if fig_h:
                st.plotly_chart(fig_h, use_container_width=True)
                r_hum = df_trend["avg_hum"].corr(df_trend["avg_val"]) if not df_trend.empty else float("nan")
                if not pd.isna(r_hum):
                    dir_text = "tăng" if r_hum > 0 else "giảm"
                    dir_text_en = "increases" if r_hum > 0 else "decreases"
                    cap_text = f"r = {r_hum:.2f} · Độ ẩm cao &rarr; {target_poll.upper()} {dir_text}" if lang == "vi" else f"r = {r_hum:.2f} · High Hum &rarr; {target_poll.upper()} {dir_text_en}"
                else:
                    cap_text = "r = N/A"
                st.markdown(f"<div style='font-size: 0.82rem; font-weight: 600; color: {sub_color}; margin-top: -5px;'>{cap_text}</div>", unsafe_allow_html=True)
            else:
                st.plotly_chart(create_empty_state("No data", height=230), use_container_width=True)

        st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

        # Row 2: Hotspot ranking (left, spans 2 columns) and Correlation Matrix (right, spans 1 column)
        c_row2_left, c_row2_right = st.columns([2.0, 1.0], gap="medium")

        with c_row2_left:
            st.markdown(f"##### {t('weather_sensitivity_ranking', lang) if lang == 'en' else 'Xếp hạng nhạy cảm tích tụ'}")
            rank_html = generate_rankings_html(df_rank, lang)
            st.markdown(f"""
            <div class="glass-card" style="padding: 1.1rem; min-height: 195px; background: {card_bg}; backdrop-filter: {glass_blur}; -webkit-backdrop-filter: {glass_blur}; border: 1px solid {border_color}; border-radius: 12px; box-shadow: {shadow}; display: flex; flex-direction: column; justify-content: center;">
                {rank_html}
            </div>
            """, unsafe_allow_html=True)

        with c_row2_right:
            st.markdown(f"##### {t('weather_heatmap_title', lang) if lang == 'en' else 'Ma trận tương quan khí tượng'}")
            if not df_trend.empty:
                corr_df = df_trend[["avg_val", "avg_temp", "avg_hum", "avg_wind"]].corr()
                matrix_html = generate_correlation_matrix_html(corr_df, target_poll)
            else:
                matrix_html = f"<div style='color: gray; font-size: 0.8rem;'>{'No correlation data' if lang == 'en' else 'Không có dữ liệu tương quan'}</div>"
                
            st.markdown(f"""
            <div class="glass-card" style="padding: 0.75rem 0.95rem; min-height: 195px; background: {card_bg}; backdrop-filter: {glass_blur}; -webkit-backdrop-filter: {glass_blur}; border: 1px solid {border_color}; border-radius: 12px; box-shadow: {shadow}; display: flex; align-items: center; justify-content: center;">
                {matrix_html}
            </div>
            """, unsafe_allow_html=True)

        # ── 4. Circular Down Arrow ───────────────────────────────────────────
        btn_bg = "rgba(255, 255, 255, 0.06)" if theme == "dark" else "rgba(0, 0, 0, 0.04)"
        btn_border = "rgba(255, 255, 255, 0.08)" if theme == "dark" else "rgba(0, 0, 0, 0.06)"
        arrow_color = "#94a3b8" if theme == "dark" else "#64748b"
        
        st.markdown(f"""
        <div style="display: flex; justify-content: center; margin-top: 1rem; margin-bottom: 0.5rem;">
            <div style="width: 38px; height: 38px; border-radius: 50%; background: {btn_bg}; border: 1px solid {btn_border}; display: flex; align-items: center; justify-content: center; box-shadow: 0 4px 12px rgba(0,0,0,0.03);">
                <span style="font-size: 1.15rem; color: {arrow_color}; font-weight: 700; display: flex; align-items: center;">↓</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── 5. Bottom Insights & AI Ask Row ──────────────────────────────────
        c_bot_left, c_bot_right = st.columns([1.9, 1.05], gap="large")

        with c_bot_left:
            # Dynamic insights paragraph
            if lang == "vi":
                insight_title = "💨 Gió & bụi"
                insight_desc = f"Ngày có gió &gt; 2 m/s: nồng độ {target_poll.upper()} giảm trung bình đáng kể nhờ tốc độ khuếch tán khí quyển tối ưu. Ngược lại, khu vực miền Bắc vào mùa khô thiếu gió lặng gió là nguyên nhân chính thúc đẩy sự tích tụ nồng độ bụi cực đoan trong nhiều ngày."
            else:
                insight_title = "💨 Wind & Dust"
                insight_desc = f"Days with wind &gt; 2 m/s: {target_poll.upper()} concentrations show a significant drop due to optimal atmospheric dispersion. Conversely, Northern Vietnam during the dry season frequently encounters stagnant wind conditions, acting as the primary driver for extreme fine dust accumulation."

            st.markdown(f"""
            <div class="glass-card" style="padding: 1rem; min-height: 50px; background: {card_bg}; backdrop-filter: {glass_blur}; -webkit-backdrop-filter: {glass_blur}; border: 1px solid {border_color}; border-radius: 12px; box-shadow: {shadow}; display: flex; flex-direction: column; justify-content: center;">
                <div style="font-size: 0.92rem; font-weight: 700; color: {text_color}; margin-bottom: 4px; display: flex; align-items: center; gap: 4px;">{insight_title}</div>
                <div style="font-size: 0.85rem; line-height: 1.45; color: {text_color}; opacity: 0.9;">{insight_desc}</div>
            </div>
            """, unsafe_allow_html=True)

        with c_bot_right:
            if lang == "vi":
                ai_title = "💬 Hỏi AI ↗"
                ai_desc = "Tự động phân tích các hệ số tương quan khí tượng phức tạp bằng LLM."
                ai_btn_lbl = "Hỏi về tương quan này ↗"
            else:
                ai_title = "💬 Ask AI ↗"
                ai_desc = "Analyze complex meteorological correlations using LLM."
                ai_btn_lbl = "Ask about correlations ↗"

            st.markdown(f"""
            <div class="glass-card" style="padding: 1rem; min-height: 98px; background: {card_bg}; backdrop-filter: {glass_blur}; -webkit-backdrop-filter: {glass_blur}; border: 1px solid {border_color}; border-radius: 12px; box-shadow: {shadow}; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <div style="font-size: 0.92rem; font-weight: 700; color: {text_color}; margin-bottom: 2px;">{ai_title}</div>
                    <div style="font-size: 0.78rem; color: {sub_color}; line-height: 1.3;">{ai_desc}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Position button over or right under card
            st.markdown("<div style='margin-top: -38px; padding: 0 10px 10px 10px;'>", unsafe_allow_html=True)
            if st.button(ai_btn_lbl, use_container_width=True, key="bottom_ask_ai_trigger_btn"):
                st.session_state.show_ai_chat = not st.session_state.show_ai_chat
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        # ── 6. Analytical AI Chat (collapsible) ──────────────────────────────
        show_ai = st.session_state.show_ai_chat
        if show_ai:
            render_section_divider()
            with st.container():
                st.markdown(f"#### 💬 {t('weather_chat_title', lang)}")
                _render_ai_chat(lang)
                if st.button("Ẩn trợ lý AI" if lang == "vi" else "Hide AI Assistant", use_container_width=True, key="hide_ai_bot_btn"):
                    st.session_state.show_ai_chat = False
                    st.rerun()

    else:
        st.plotly_chart(create_empty_state("Không có dữ liệu thời tiết cho lựa chọn này."), use_container_width=True)

if __name__ == "__main__":
    main()
