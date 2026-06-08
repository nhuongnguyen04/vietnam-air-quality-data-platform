"""
Historical Trend page.
Analyzes temporal trends, monthly averages, daily patterns, and provincial heatmaps.
Redesigned to perfectly match the custom high-fidelity visual layout.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import (
    get_aqi_color_range,
    get_aqi_color_scale,
    get_aqi_colorbar_config,
)
from lib.clickhouse_client import query_df
from lib.data_service import (
    build_where_clause,
    get_pollutant_cols,
    get_source_mix,
    get_source_table,
)
from lib.filters import render_top_filters
from lib.i18n import t
from lib.page_helpers import render_section_divider, page_wrapper
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.tab_renderer import render_coverage_banner
from lib.ui_components import render_kpi_card

@st.cache_data(ttl=300)
def get_overall_stats(col, dates, source_name, tunit="day", spatial_grain="Toàn quốc"):
    table = get_source_table(spatial_grain, "Ngày", source_name)
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(None, None, dates, time_unit=tunit, source_mix=source_mix)
    q = f"""
    SELECT
        count(distinct date)            AS total_days,
        round(avg({col}), 1)      AS overall_avg,
        round(min({col}), 1)      AS overall_min,
        round(max({col}), 0)      AS overall_max
    FROM air_quality.{table}
    WHERE {where_clause}
      AND {col} IS NOT NULL
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_daily_trend(col, scope_grain, scope_val, dates, source_name, tunit="day"):
    table = get_source_table(scope_grain, "Ngày", source_name)
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(scope_grain, scope_val, dates, time_unit=tunit, source_mix=source_mix)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({col}), 0)  AS max_val
    FROM air_quality.{table}
    WHERE {where_clause}
      AND {col} IS NOT NULL
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)

def build_monthly_where_clause(dates):
    if not dates:
        return "1=1"
    month_starts = [
        pd.Timestamp(d).replace(day=1).strftime("%Y-%m-%d")
        for d in dates
    ]
    if len(month_starts) == 2:
        start_month, end_month = month_starts
        return f"date BETWEEN '{start_month}' AND '{end_month}'"
    return f"date = '{month_starts[0]}'"

@st.cache_data(ttl=300)
def get_monthly_trend(col, dates, source_name, spatial_grain="Toàn quốc"):
    table = get_source_table(spatial_grain, "Tháng", source_name)
    source_mix = get_source_mix(source_name)
    where_clause = build_monthly_where_clause(dates) + f" AND source_mix = '{source_mix}'"
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({col}), 0)  AS max_val
    FROM air_quality.{table}
    WHERE {where_clause}
      AND {col} IS NOT NULL
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_heatmap_data(col, scope_grain, scope_val, dates, source_name, tunit="day"):
    table = get_source_table(scope_grain, "Ngày", source_name)
    source_mix = get_source_mix(source_name)
    where_clause = build_where_clause(scope_grain, scope_val, dates, time_unit=tunit, source_mix=source_mix)
    q = f"""
    SELECT
        province,
        toString(date)           AS date_str,
        round(avg({col}), 1) AS display_val
    FROM air_quality.{table}
    WHERE province IS NOT NULL AND province != ''
      AND {where_clause}
      AND {col} IS NOT NULL
    GROUP BY province, date
    ORDER BY province, date
    """
    return query_df(q)

@page_wrapper("historical_trend", "Xu hướng lịch sử", icon="📈", skip_hero=True)
def main(lang):
    theme = st.session_state.get("theme", "light")
    standard = st.session_state.get("standard", "VN_AQI")

    # 4. Render synchronized top filters
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    time_unit     = filters["time_unit"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]
    pollutant     = filters["pollutant"]
    standard      = filters["standard"]

    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
    display_col, max_col = get_pollutant_cols(pollutant, standard)

    # 5. Build dynamic title texts to perfectly match the screenshot layout
    if date_range and len(date_range) == 2:
        delta_days = (pd.to_datetime(date_range[1]) - pd.to_datetime(date_range[0])).days + 1
        days_text = f"{delta_days} ngày" if lang == "vi" else f"{delta_days} days"
    elif date_range and len(date_range) == 1:
        days_text = "1 ngày" if lang == "vi" else "1 day"
    else:
        days_text = "365 ngày" if lang == "vi" else "365 days"

    if pollutant == "aqi":
        aqi_text = "AQI VN" if standard == "VN_AQI" else "AQI WHO"
    else:
        aqi_text = pollutant.upper()

    scope_val_label = scope_val if (spatial_grain in ["Tỉnh", "Phường"] and scope_val) else ("Toàn quốc" if lang == "vi" else "National")

    # 6. Render Page Title and Pill Source Switcher on one clean horizontal row
    st.markdown("<div style='margin-top: 0.75rem;'></div>", unsafe_allow_html=True)
    c_title, c_source_selector = st.columns([0.58, 0.42], vertical_alignment="bottom")
    
    with c_title:
        st.markdown("<h2 style='margin:0; padding:0; font-family:\"Outfit\",sans-serif; font-size:1.65rem; font-weight:700; opacity: 0.95;'>Xu hướng lịch sử</h2>" if lang == "vi" else "<h2 style='margin:0; padding:0; font-family:\"Outfit\",sans-serif; font-size:1.65rem; font-weight:700; opacity: 0.95;'>Historical Trends</h2>", unsafe_allow_html=True)
        st.markdown(f"<p style='margin:0.25rem 0 0 0; font-size:0.85rem; opacity:0.6; font-weight: 500;'>{scope_val_label} · {days_text} · {aqi_text}</p>", unsafe_allow_html=True)

    source_labels = {
        "ground": "Quan trắc" if lang == "vi" else "Monitors",
        "satellite": "Vệ tinh" if lang == "vi" else "Satellite",
        "comparison": "So sánh" if lang == "vi" else "Comparison"
    }
    
    if "trends_source" not in st.session_state:
        st.session_state.trends_source = "ground"

    with c_source_selector:
        st.markdown("<div class='source-selector-anchor'></div>", unsafe_allow_html=True)
        sub_c1, sub_c2 = st.columns([0.22, 0.78], vertical_alignment="center")
        with sub_c1:
            st.markdown("<div style='text-align: right; font-weight: 600; font-size: 0.88rem; opacity: 0.85; width: 100%; display: flex; justify-content: flex-end; align-items: center; height: 38px;'>Nguồn</div>" if lang == "vi" else "<div style='text-align: right; font-weight: 600; font-size: 0.88rem; opacity: 0.85; width: 100%; display: flex; justify-content: flex-end; align-items: center; height: 38px;'>Source</div>", unsafe_allow_html=True)
        with sub_c2:
            selected_source = st.segmented_control(
                label="Nguồn",
                options=list(source_labels.keys()),
                format_func=lambda x: source_labels[x],
                default=st.session_state.trends_source,
                key="trends_source_select",
                label_visibility="collapsed"
            )
            if selected_source:
                st.session_state.trends_source = selected_source

    st.markdown("<div style='margin-top: 0.85rem;'></div>", unsafe_allow_html=True)

    # Local chart builders
    def render_daily_trend_chart(df: pd.DataFrame, height: int):
        avg_label = "TB ngày" if lang == "vi" else "Daily Avg"
        max_label = "Cao nhất ngày" if lang == "vi" else "Daily Max"
        plot_df = df.rename(columns={"avg_val": avg_label, "max_val": max_label})

        fig = px.line(
            plot_df,
            x="date",
            y=[avg_label, max_label],
            labels={
                "date": t("chart_label_date", lang),
                "value": val_label,
                "variable": t("chart_label_type", lang),
            },
            color_discrete_map={avg_label: "#3B82F6", max_label: "#EF4444"},
        )
        
        # Add WHO limit horizontal line
        WHO_LIMITS = {
            "pm25": 15,
            "pm10": 45,
            "no2": 25,
            "so2": 40,
            "o3": 100,
            "co": 4000,
            "aqi": 100,
        }
        limit_val = WHO_LIMITS.get(pollutant, 100)
        
        fig.add_hline(
            y=limit_val,
            line_dash="dash",
            line_color="#D97706",
            annotation_text=f"WHO {limit_val}",
            annotation_position="top left",
            annotation_font_color="#D97706",
            annotation_font_size=10,
            annotation_font_weight="bold"
        )
        
        fig.add_scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="#D97706", dash="dash"),
            name="ngưỡng WHO" if lang == "vi" else "WHO threshold"
        )
        
        fig.update_layout(
            get_plotly_layout(height=height),
            hovermode="x unified",
            margin=dict(l=50, r=20, t=30, b=70),  # Expand bottom margin for the legend
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.18,  # Position legend below the x-axis ticks cleanly
                xanchor="center",
                x=0.5,
                title=None
            )
        )
        fig.update_xaxes(title_text=None, tickformat="%d/%m/%Y", hoverformat="%d/%m/%Y")
        return fig

    def render_monthly_average_chart(df: pd.DataFrame):
        plot_df = df.copy()
        
        def format_month_label(date_val, lang_choice):
            d = pd.to_datetime(date_val)
            if lang_choice == "vi":
                return f"T{d.month}"
            else:
                return d.strftime("%b")
                
        plot_df["period"] = plot_df["date"].apply(lambda x: format_month_label(x, lang))
        period_label = "Month" if lang == "en" else "Tháng"
        
        fig = px.bar(
            plot_df,
            x="period",
            y="avg_val",
            labels={"period": period_label, "avg_val": val_label},
            color="avg_val",
            color_continuous_scale=get_aqi_color_scale(standard) if pollutant == "aqi" else "Viridis",
            range_color=get_aqi_color_range(standard) if pollutant == "aqi" else None,
        )
        fig.update_layout(
            get_plotly_layout(height=320, compact=True),
            showlegend=False,
            coloraxis_showscale=False,
            xaxis={"type": "category", "title": None},
            yaxis={"title": None}
        )
        fig.update_traces(cliponaxis=False)
        return fig

    def get_aqi_from_pollutant(val: float, p: str, std: str = "VN_AQI") -> float:
        if val is None or pd.isna(val):
            return None
        p = p.lower()
        if p == "aqi":
            return val
            
        if std == "VN_AQI":
            if p == "pm25":
                breakpoints = [
                    (0.0, 25.0, 0.0, 50.0),
                    (25.0, 50.0, 51.0, 100.0),
                    (50.0, 80.0, 101.0, 150.0),
                    (80.0, 150.0, 151.0, 200.0),
                    (150.0, 250.0, 201.0, 300.0),
                    (250.0, 350.0, 301.0, 400.0),
                    (350.0, 500.0, 401.0, 500.0),
                ]
            elif p == "pm10":
                breakpoints = [
                    (0.0, 50.0, 0.0, 50.0),
                    (50.0, 150.0, 51.0, 100.0),
                    (150.0, 250.0, 101.0, 150.0),
                    (250.0, 350.0, 151.0, 200.0),
                    (350.0, 420.0, 201.0, 300.0),
                    (420.0, 500.0, 301.0, 400.0),
                    (500.0, 600.0, 401.0, 500.0),
                ]
            elif p == "no2":
                breakpoints = [
                    (0.0, 40.0, 0.0, 50.0),
                    (40.0, 80.0, 51.0, 100.0),
                    (80.0, 180.0, 101.0, 150.0),
                    (180.0, 280.0, 151.0, 200.0),
                    (280.0, 565.0, 201.0, 300.0),
                    (565.0, 750.0, 301.0, 400.0),
                    (750.0, 940.0, 401.0, 500.0),
                ]
            elif p == "so2":
                breakpoints = [
                    (0.0, 125.0, 0.0, 50.0),
                    (125.0, 350.0, 51.0, 100.0),
                    (350.0, 550.0, 101.0, 150.0),
                    (550.0, 800.0, 151.0, 200.0),
                    (800.0, 1600.0, 201.0, 300.0),
                    (1600.0, 2100.0, 301.0, 400.0),
                    (2100.0, 2630.0, 401.0, 500.0),
                ]
            elif p == "co":
                breakpoints = [
                    (0.0, 10000.0, 0.0, 50.0),
                    (10000.0, 30000.0, 51.0, 100.0),
                    (30000.0, 45000.0, 101.0, 150.0),
                    (45000.0, 60000.0, 151.0, 200.0),
                    (60000.0, 90000.0, 201.0, 300.0),
                    (90000.0, 120000.0, 301.0, 400.0),
                    (120000.0, 150000.0, 401.0, 500.0),
                ]
            elif p == "o3":
                breakpoints = [
                    (0.0, 160.0, 0.0, 50.0),
                    (160.0, 200.0, 51.0, 100.0),
                    (200.0, 240.0, 101.0, 150.0),
                    (240.0, 280.0, 151.0, 200.0),
                    (280.0, 400.0, 201.0, 300.0),
                    (400.0, 500.0, 301.0, 400.0),
                    (500.0, 600.0, 401.0, 500.0),
                ]
            else:
                return val
        else:  # US/WHO standard fallback
            if p == "pm25":
                breakpoints = [
                    (0.0, 12.0, 0.0, 50.0),
                    (12.1, 35.4, 51.0, 100.0),
                    (35.5, 55.4, 101.0, 150.0),
                    (55.5, 150.4, 151.0, 200.0),
                    (150.5, 250.4, 201.0, 300.0),
                    (250.5, 500.0, 301.0, 500.0),
                ]
            else:
                return val

        for bp_low, bp_high, aqi_low, aqi_high in breakpoints:
            if bp_low <= val <= bp_high:
                return ((aqi_high - aqi_low) / (bp_high - bp_low)) * (val - bp_low) + aqi_low
        return val

    def get_premium_color(val: float) -> str:
        if val is None or pd.isna(val):
            return "#1e293b" if theme == "dark" else "#f1f5f9"
            
        aqi_val = get_aqi_from_pollutant(val, pollutant, standard)
        
        if aqi_val <= 50:
            return "#e2f0d9"  # Mint Good
        elif aqi_val <= 100:
            return "#faf0d9"  # Cream Moderate
        elif aqi_val <= 150:
            return "#fcd8c4"  # Soft Orange USG
        elif aqi_val <= 200:
            return "#d95f56"  # Soft Red Unhealthy
        elif aqi_val <= 300:
            return "#8b3b30"  # Dark Red Very Unhealthy
        else:
            return "#5a1f18"  # Deep Maroon Hazardous

    def render_province_day_heatmap(df: pd.DataFrame, provinces: list[str], height: int) -> str:
        plot_df = df.copy()
        plot_df["date"] = pd.to_datetime(plot_df["date_str"])
        dates = sorted(plot_df["date"].dropna().unique())
        matrix = plot_df.pivot_table(
            index="province", columns="date", values="display_val", aggfunc="mean"
        ).reindex(index=provinces, columns=dates)

        if theme == "dark":
            sticky_bg = "#020617"  # Slate-950 background matching style.py bg_color
            border_color = "rgba(255, 255, 255, 0.08)"
            text_color = "#cbd5e1"
            cell_border = "rgba(0, 0, 0, 0.3)"
        else:
            sticky_bg = "#f8fafc"  # Slate-50 background matching style.py bg_color
            border_color = "rgba(15, 23, 42, 0.08)"
            text_color = "#0f172a"
            cell_border = "rgba(0, 0, 0, 0.12)"

        html = []
        html.append(f"""
        <style>
            .heatmap-scroll-container {{
                width: 100%;
                overflow-x: auto;
                position: relative;
                padding-bottom: 12px;
                scrollbar-width: thin;
            }}
            .heatmap-scroll-container::-webkit-scrollbar {{
                height: 6px;
            }}
            .heatmap-scroll-container::-webkit-scrollbar-track {{
                background: transparent;
            }}
            .heatmap-scroll-container::-webkit-scrollbar-thumb {{
                background: rgba(148, 163, 184, 0.3);
                border-radius: 10px;
            }}
            .heatmap-scroll-container::-webkit-scrollbar-thumb:hover {{
                background: rgba(148, 163, 184, 0.5);
            }}
            .heatmap-row {{
                display: flex;
                align-items: center;
                gap: 5px;
                margin-bottom: 6px;
                min-width: max-content;
            }}
            .heatmap-label {{
                position: sticky;
                left: 0;
                background-color: {sticky_bg};
                padding-right: 14px;
                width: 95px;
                min-width: 95px;
                font-family: 'Outfit', sans-serif;
                font-weight: 500;
                font-size: 0.88rem;
                color: {text_color};
                z-index: 10;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                text-align: left;
                transition: background-color 0.3s ease;
            }}
            .heatmap-cells-wrapper {{
                display: flex;
                gap: 4px;
            }}
            .heatmap-cell {{
                width: 74px;
                height: 22px;
                border-radius: 5px;
                border: 1px solid {cell_border};
                flex-shrink: 0;
                transition: transform 0.15s ease, filter 0.15s ease, box-shadow 0.15s ease;
                cursor: pointer;
            }}
            .heatmap-cell:hover {{
                transform: scale(1.08);
                filter: brightness(1.05);
                box-shadow: 0 3px 8px rgba(0, 0, 0, 0.25);
                z-index: 15;
            }}
        </style>
        <div class="heatmap-scroll-container">
        """)

        for prov in provinces:
            html.append(f'  <div class="heatmap-row">')
            html.append(f'    <div class="heatmap-label" title="{prov}">{prov}</div>')
            html.append(f'    <div class="heatmap-cells-wrapper">')
            
            for date in dates:
                val = matrix.loc[prov, date] if prov in matrix.index and date in matrix.columns else None
                color = get_premium_color(val)
                
                if val is not None and not pd.isna(val):
                    formatted_val = f"{val:.1f}" if pollutant != "aqi" else f"{int(val)}"
                    unit = " µg/m³" if pollutant != "aqi" else ""
                    tooltip = f"{prov}&#10;Ngày: {date.strftime('%d/%m/%Y')}&#10;{val_label}: {formatted_val}{unit}"
                else:
                    tooltip = f"{prov}&#10;Ngày: {date.strftime('%d/%m/%Y')}&#10;Không có dữ liệu"
                    
                html.append(f'      <div class="heatmap-cell" style="background-color: {color};" title="{tooltip}"></div>')
                
            html.append(f'    </div>')
            html.append(f'  </div>')
            
        html.append(f'</div>')
        
        # Clean leading and trailing whitespace from each line to prevent markdown code block escapes
        raw_html = "\n".join(html)
        cleaned_html = "\n".join(line.strip() for line in raw_html.split("\n"))
        return cleaned_html

    # ── Source trend tabs ─────────────────────────────────────────────────────────
    def render_source_historical_tab(source_name: str):
        render_coverage_banner(source_name, spatial_grain, scope_val, lang)
        
        stats = get_overall_stats(display_col, date_range, source_name, time_unit, spatial_grain)
        trend_df = get_daily_trend(display_col, spatial_grain, scope_val, date_range, source_name, time_unit)
        monthly_df = get_monthly_trend(display_col, date_range, source_name, spatial_grain)
        
        # 1. Custom metric cards with dynamic subtexts
        if not stats.empty and not trend_df.empty and not monthly_df.empty:
            row = stats.iloc[0]
            total_days_val = int(row.total_days or 0)
            overall_avg_val = row.overall_avg or 0
            overall_min_val = row.overall_min or 0
            overall_max_val = row.overall_max or 0
            
            # Days subtext
            days_subtext = "dữ liệu đầy đủ" if lang == "vi" else "complete data"
            
            # Month with highest monthly average
            max_month_row = monthly_df.loc[monthly_df["avg_val"].idxmax()]
            max_month_date = pd.to_datetime(max_month_row["date"])
            month_str = f"tháng {max_month_date.month}" if lang == "vi" else max_month_date.strftime("%B")
            avg_subtext = f"cao nhất {month_str}" if lang == "vi" else f"highest in {month_str}"
            
            # Month with lowest monthly average + wet/dry season context
            min_month_row = monthly_df.loc[monthly_df["avg_val"].idxmin()]
            min_month_date = pd.to_datetime(min_month_row["date"])
            month_num = min_month_date.month
            if month_num in [5, 6, 7, 8, 9, 10]:
                season_suffix = " (mùa mưa)" if lang == "vi" else " (rainy season)"
            else:
                season_suffix = " (mùa khô)" if lang == "vi" else " (dry season)"
            min_subtext = f"tháng {month_num}{season_suffix}" if lang == "vi" else f"{min_month_date.strftime('%B')}{season_suffix}"
            
            # Date containing absolute maximum daily value
            max_day_row = trend_df.loc[trend_df["max_val"].idxmax()]
            max_day_date = pd.to_datetime(max_day_row["date"])
            max_subtext = max_day_date.strftime("%d/%m/%Y")
            
            kpi_cols = st.columns(4)
            with kpi_cols[0]:
                render_kpi_card(
                    title="Số ngày phân tích" if lang == "vi" else "Days Analyzed",
                    value=f"{total_days_val}",
                    subtext=days_subtext
                )
            with kpi_cols[1]:
                render_kpi_card(
                    title=f"{val_label} trung bình" if lang == "vi" else f"Average {val_label}",
                    value=f"{overall_avg_val:.1f}" if pollutant != "aqi" else f"{int(overall_avg_val)}",
                    subtext=avg_subtext,
                    val_color="#f59e0b"
                )
            with kpi_cols[2]:
                render_kpi_card(
                    title=f"{val_label} thấp nhất" if lang == "vi" else f"Lowest {val_label}",
                    value=f"{overall_min_val:.1f}" if pollutant != "aqi" else f"{int(overall_min_val)}",
                    subtext=min_subtext,
                    val_color="#10b981"
                )
            with kpi_cols[3]:
                render_kpi_card(
                    title=f"{val_label} cao nhất" if lang == "vi" else f"Highest {val_label}",
                    value=f"{overall_max_val:.1f}" if pollutant != "aqi" else f"{int(overall_max_val)}",
                    subtext=max_subtext,
                    val_color="#ef4444"
                )
        else:
            kpi_cols = st.columns(4)
            with kpi_cols[0]:
                render_kpi_card("Số ngày phân tích" if lang == "vi" else "Days Analyzed", "0", "Không có dữ liệu" if lang == "vi" else "No data")
            with kpi_cols[1]:
                render_kpi_card(f"{val_label} trung bình" if lang == "vi" else f"Average {val_label}", "0.0", "Không có dữ liệu" if lang == "vi" else "No data")
            with kpi_cols[2]:
                render_kpi_card(f"{val_label} thấp nhất" if lang == "vi" else f"Lowest {val_label}", "0.0", "Không có dữ liệu" if lang == "vi" else "No data")
            with kpi_cols[3]:
                render_kpi_card(f"{val_label} cao nhất" if lang == "vi" else f"Highest {val_label}", "0.0", "Không có dữ liệu" if lang == "vi" else "No data")

        st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

        # 2 & 3. Daily Trend + Monthly Average (Dual chart layout matching 65% / 35% ratio)
        c_left, c_right = st.columns([1.9, 1.1], gap="large")
        
        with c_left:
            st.markdown(f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.05rem; opacity: 0.9;'>Diễn biến {val_label} theo ngày (trung bình + cao nhất)</h4>" if lang == "vi" else f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.05rem; opacity: 0.9;'>Daily {val_label} Evolution (avg + max)</h4>", unsafe_allow_html=True)
            if not trend_df.empty:
                fig = render_daily_trend_chart(trend_df, height=320)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu xu hướng cho nguồn này.", height=320), use_container_width=True)
                
        with c_right:
            st.markdown(f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.05rem; opacity: 0.9;'>TB theo tháng</h4>" if lang == "vi" else f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.05rem; opacity: 0.9;'>Monthly Avg</h4>", unsafe_allow_html=True)
            if not monthly_df.empty:
                fig = render_monthly_average_chart(monthly_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu trung bình tháng.", height=320), use_container_width=True)

        render_section_divider()

        # 5. Province heatmap
        if date_range and len(date_range) > 0:
            start_date = pd.to_datetime(date_range[0])
            month_year_text = f"tháng {start_date.month}/{start_date.year}" if lang == "vi" else start_date.strftime("%B %Y")
        else:
            month_year_text = "tháng 1/2026" if lang == "vi" else "January 2026"
            
        heatmap_title = f"Heatmap {val_label} — tỉnh × ngày ({month_year_text})" if lang == "vi" else f"{val_label} Heatmap — province × day ({month_year_text})"
        
        st.markdown(f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.5rem; font-weight: 700; font-size:1.05rem; opacity: 0.9;'>{heatmap_title}</h4>", unsafe_allow_html=True)
        heatmap_data = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, source_name, time_unit)
        if not heatmap_data.empty:
            all_provs = heatmap_data.groupby("province")["display_val"].mean().sort_values(ascending=False).index.tolist()
            filtered = heatmap_data[heatmap_data["province"].isin(all_provs)]
            chart_height = max(240, len(all_provs) * 32)
            heatmap_html = render_province_day_heatmap(filtered, all_provs, chart_height)
            st.markdown(heatmap_html, unsafe_allow_html=True)
            
            st.markdown("<p style='font-size:0.8rem; opacity:0.6; margin-top:-10px;'>Mỗi ô = 1 ngày. Cuộn ngang để xem đủ 365 ngày.</p>" if lang == "vi" else "<p style='font-size:0.8rem; opacity:0.6; margin-top:-10px;'>Each cell = 1 day. Scroll horizontally to view all 365 days.</p>", unsafe_allow_html=True)

    # ── Comparison tab renderer ──────────────────────────────────────────────────
    def render_comparison_historical_tab():
        st.markdown(f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.15rem; opacity: 0.9;'>So sánh Xu hướng thời gian ({val_label})</h4>" if lang == "vi" else f"<h4 style='font-family:\"Outfit\",sans-serif; margin-bottom: 0.75rem; font-weight: 700; font-size:1.15rem; opacity: 0.9;'>Trend Comparison ({val_label})</h4>", unsafe_allow_html=True)

        c_c1, c_c2 = st.columns(2, gap="medium")
        with c_c1:
            st.markdown("##### 📈 Biểu đồ so sánh xu hướng theo ngày" if lang == "vi" else "##### 📈 Daily Trend Comparison")
            g_daily = get_daily_trend(display_col, spatial_grain, scope_val, date_range, "aqiin", time_unit)
            s_daily = get_daily_trend(display_col, spatial_grain, scope_val, date_range, "openweather", time_unit)

            if not g_daily.empty and not s_daily.empty:
                merged_daily = pd.merge(g_daily, s_daily, on="date", suffixes=("_ground", "_sat"))
                plot_df = merged_daily.rename(columns={
                    "avg_val_ground": "📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors",
                    "avg_val_sat": "🛰️ Vệ tinh" if lang == "vi" else "🛰️ SILAM Model"
                })

                fig_line = px.line(
                    plot_df,
                    x="date",
                    y=["📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors", "🛰️ Vệ tinh" if lang == "vi" else "🛰️ SILAM Model"],
                    labels={"date": t("chart_label_date", lang), "value": val_label},
                    color_discrete_map={"📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors": SOURCE_PALETTE["📡 Mặt đất"], "🛰️ Vệ tinh" if lang == "vi" else "🛰️ Vệ tinh": SOURCE_PALETTE["🛰️ Vệ tinh"]}
                )
                fig_line.update_layout(get_plotly_layout(height=320, compact=True), hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không đủ dữ liệu song hành để so sánh."), use_container_width=True)

        with c_c2:
            st.markdown("##### 📅 So sánh trung bình theo tháng" if lang == "vi" else "##### 📅 Monthly Avg Comparison")
            g_monthly = get_monthly_trend(display_col, date_range, "aqiin", spatial_grain)
            s_monthly = get_monthly_trend(display_col, date_range, "openweather", spatial_grain)

            if not g_monthly.empty and not s_monthly.empty:
                g_monthly["Source"] = "📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors"
                s_monthly["Source"] = "🛰️ Vệ tinh" if lang == "vi" else "🛰️ SILAM Model"
                combined_monthly = pd.concat([g_monthly, s_monthly])
                combined_monthly["period"] = pd.to_datetime(combined_monthly["date"]).dt.strftime("%m/%Y")

                period_label = "Month" if lang == "en" else "Tháng"
                fig_monthly = px.bar(
                    combined_monthly,
                    x="period",
                    y="avg_val",
                    color="Source",
                    barmode="group",
                    labels={"period": period_label, "avg_val": val_label},
                    color_discrete_map={"📡 Mặt đất" if lang == "vi" else "📡 Ground Monitors": SOURCE_PALETTE["📡 Mặt đất"], "🛰️ Vệ tinh" if lang == "vi" else "🛰️ Vệ tinh": SOURCE_PALETTE["🛰️ Vệ tinh"]}
                )
                fig_monthly.update_layout(get_plotly_layout(height=320, compact=True))
                st.plotly_chart(fig_monthly, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không đủ dữ liệu tháng."), use_container_width=True)

        render_section_divider()

        # Heatmap of differences
        st.markdown("##### 🌡️ Bản đồ nhiệt chênh lệch (Mặt đất - Vệ tinh)" if lang == "vi" else "##### 🌡️ Bias Heatmap (Ground - Satellite)")
        st.caption("Màu đỏ thể hiện trạm mặt đất đo cao hơn; màu xanh thể hiện mô hình vệ tinh ước lượng cao hơn." if lang == "vi" else "Red shows ground station is higher; blue shows satellite model is higher.")

        g_heatmap = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, "aqiin", time_unit)
        s_heatmap = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, "openweather", time_unit)

        if not g_heatmap.empty and not s_heatmap.empty:
            merged_heat = pd.merge(g_heatmap, s_heatmap, on=["province", "date_str"], suffixes=("_ground", "_sat"))
            merged_heat["display_val"] = merged_heat["display_val_ground"] - merged_heat["display_val_sat"]

            all_provs = merged_heat.groupby("province")["display_val"].mean().sort_values(ascending=False).index.tolist()
            filtered = merged_heat[merged_heat["province"].isin(all_provs)]
            chart_height = max(240, len(all_provs) * 32)

            colorbar_config = {"title": {"text": f"Đo lệch {val_label}" if lang == "vi" else f"Bias {val_label}"}}
            colorbar_config.update({"x": 1.01, "xanchor": "left", "xpad": 4, "len": 0.84, "thickness": 16})

            plot_df = filtered.copy()
            plot_df["date"] = pd.to_datetime(plot_df["date_str"])
            dates = sorted(plot_df["date"].dropna().unique())

            matrix = plot_df.pivot_table(
                index="province", columns="date", values="display_val", aggfunc="mean"
            ).reindex(index=all_provs, columns=dates)

            max_abs = max(abs(matrix.min().min()), abs(matrix.max().max())) if not matrix.empty else 50
            if pd.isna(max_abs) or max_abs == 0:
                max_abs = 50

            fig_bias_heat = px.imshow(
                matrix,
                x=dates,
                y=all_provs,
                color_continuous_scale="RdBu_r",
                range_color=[-max_abs, max_abs],
                aspect="auto",
                labels={"x": t("chart_label_date", lang), "y": t("province", lang), "color": "Bias"},
            )
            fig_bias_heat.update_layout(
                height=chart_height,
                margin={"l": 20, "r": 130, "t": 10, "b": 58},
                coloraxis_colorbar=colorbar_config,
            )
            date_format = "%d/%m/%Y" if lang == "vi" else "%b %d, %Y"
            fig_bias_heat.update_xaxes(tickformat=date_format.replace("/%Y", "<br>%Y"))
            st.plotly_chart(fig_bias_heat, use_container_width=True)
            
            st.markdown("<p style='font-size:0.8rem; opacity:0.6; margin-top:-10px;'>Mỗi ô = 1 ngày. Cuộn ngang để xem chênh lệch theo thời gian.</p>" if lang == "vi" else "<p style='font-size:0.8rem; opacity:0.6; margin-top:-10px;'>Each cell = 1 day. Scroll horizontally to view bias over time.</p>", unsafe_allow_html=True)
        else:
            st.plotly_chart(create_empty_state("Không đủ dữ liệu để tạo bản đồ so sánh lệch."), use_container_width=True)

    # ── Render active source selector tab contents ──────────────────────────
    if st.session_state.trends_source == "ground":
        render_source_historical_tab("aqiin")
    elif st.session_state.trends_source == "satellite":
        render_source_historical_tab("openweather")
    else:
        render_comparison_historical_tab()



if __name__ == "__main__":
    main()
