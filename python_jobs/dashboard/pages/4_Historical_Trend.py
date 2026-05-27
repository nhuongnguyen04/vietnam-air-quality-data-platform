"""
Historical Trend page.
Analyzes temporal trends, monthly averages, daily patterns, and provincial heatmaps.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import (
    get_aqi_color_range,
    get_aqi_color_scale,
    get_aqi_colorbar_config,
    render_empty_chart,
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
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.tab_renderer import render_3_tabs

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

@st.cache_data(ttl=300)
def get_temporal_patterns(col: str, province: str | None, source_name: str, date_range=None, spatial_grain="Toàn quốc", lang="vi"):
    table = get_source_table(spatial_grain, "Giờ", source_name)
    source_mix = get_source_mix(source_name)
    where_clause = f"WHERE province = '{province}'" if province else ""

    if date_range and len(date_range) == 2:
        date_clause = build_where_clause(None, None, date_range, time_unit="hour")
        if where_clause:
            where_clause += f" AND {date_clause}"
        else:
            where_clause = f"WHERE {date_clause}"

    source_clause = f"source_mix = '{source_mix}'"
    if where_clause:
        where_clause += f" AND {source_clause}"
    else:
        where_clause = f"WHERE {source_clause}"

    q = f"""
    SELECT
        toHour(datetime_hour) as hour_of_day,
        toDayOfWeek(datetime_hour) as day_of_week,
        avg({col}) as avg_aqi
    FROM air_quality.{table}
    {where_clause}
    GROUP BY hour_of_day, day_of_week
    ORDER BY day_of_week, hour_of_day
    """
    df = query_df(q)
    if not df.empty:
        day_names = [t(f"day_{d}", lang) for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]]
        day_map = {i+1: day_names[i] for i in range(7)}
        df["day_name"] = df["day_of_week"].map(day_map)
        df["day_name"] = pd.Categorical(df["day_name"], categories=day_names, ordered=True)
    return df

@page_wrapper("nav_trends", "📈 Historical Trends Analysis", icon="📈")
def main(lang: str):
    # ── Sidebar Filters ────────────────────────────────────────────────────────────
    filters = render_top_filters()
    spatial_grain = filters["spatial_grain"]
    time_unit     = filters["time_unit"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]
    pollutant     = filters["pollutant"]
    standard      = filters["standard"]

    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
    display_col, max_col = get_pollutant_cols(pollutant, standard)

    # Local chart builders
    def render_daily_trend_chart(df: pd.DataFrame, height: int):
        avg_label = t("chart_label_avg", lang)
        max_label = t("chart_label_max", lang)
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
            color_discrete_map={avg_label: "#0891B2", max_label: "#EF4444"},
        )
        fig.update_layout(get_plotly_layout(height=height), hovermode="x unified")
        fig.update_xaxes(tickformat="%d/%m/%Y", hoverformat="%d/%m/%Y")
        return fig

    def render_monthly_average_chart(df: pd.DataFrame):
        plot_df = df.copy()
        plot_df["period"] = pd.to_datetime(plot_df["date"]).dt.strftime("%m/%Y")
        period_label = "Month" if lang == "en" else "Tháng"
        fig = px.bar(
            plot_df,
            x="period",
            y="avg_val",
            text="avg_val",
            labels={"period": period_label, "avg_val": val_label},
            color="avg_val",
            color_continuous_scale=get_aqi_color_scale(standard) if pollutant == "aqi" else "Viridis",
            range_color=get_aqi_color_range(standard) if pollutant == "aqi" else None,
        )
        fig.update_layout(
            get_plotly_layout(height=280, compact=True),
            showlegend=False,
            coloraxis_showscale=False,
            xaxis={"type": "category", "title": period_label},
        )
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
        return fig

    def render_province_day_heatmap(df: pd.DataFrame, provinces: list[str], height: int):
        avg_label = t("chart_label_avg", lang)
        metric_label = f"{val_label} {avg_label.lower()}" if lang == "vi" else f"{avg_label} {val_label}"
        colorbar_config = get_aqi_colorbar_config(standard, metric_label) if pollutant == "aqi" else {"title": {"text": metric_label}}
        colorbar_config.update({"x": 1.01, "xanchor": "left", "xpad": 4, "len": 0.84, "thickness": 16})
        
        plot_df = df.copy()
        plot_df["date"] = pd.to_datetime(plot_df["date_str"])
        dates = sorted(plot_df["date"].dropna().unique())
        matrix = plot_df.pivot_table(
            index="province", columns="date", values="display_val", aggfunc="mean"
        ).reindex(index=provinces, columns=dates)

        fig = px.imshow(
            matrix,
            x=dates,
            y=provinces,
            color_continuous_scale=get_aqi_color_scale(standard) if pollutant == "aqi" else "Viridis",
            range_color=get_aqi_color_range(standard) if pollutant == "aqi" else None,
            aspect="auto",
            labels={"x": t("chart_label_date", lang), "y": t("province", lang), "color": metric_label},
        )
        fig.update_layout(
            height=height,
            margin={"l": 20, "r": 130, "t": 10, "b": 58},
            xaxis={"title": t("chart_label_date", lang), "automargin": True},
            yaxis={"title": t("province", lang), "automargin": True},
            coloraxis_colorbar=colorbar_config,
        )
        date_format = "%d/%m/%Y" if lang == "vi" else "%b %d, %Y"
        fig.update_xaxes(tickformat=date_format.replace("/%Y", "<br>%Y"))
        return fig

    # ── Source trend tabs ─────────────────────────────────────────────────────────
    def render_source_historical_tab(source_name: str):
        stats = get_overall_stats(display_col, date_range, source_name, time_unit, spatial_grain)
        
        # 1. Custom metric cards instead of native streamlit metrics
        if not stats.empty:
            row = stats.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                render_metric_card(t("chart_label_days", lang), f"{int(row.total_days or 0)}", icon="schedule")
            with col2:
                render_metric_card(f"{t('chart_label_avg', lang)} {val_label}", f"{row.overall_avg:.1f}", icon="insights")
            with col3:
                render_metric_card(f"{t('chart_label_min', lang)} {val_label}", f"{row.overall_min:.1f}", icon="star")
            with col4:
                render_metric_card(f"{t('chart_label_max', lang)} {val_label}", f"{row.overall_max:.0f}", icon="error")

        # 2 & 3. Daily Trend + Monthly Average (2-column layout)
        c_left, c_right = st.columns(2, gap="large")
        
        with c_left:
            st.markdown(f"#### 📈 {t('nav_overview', lang)} ({val_label})")
            trend_df = get_daily_trend(display_col, spatial_grain, scope_val, date_range, source_name, time_unit)
            if not trend_df.empty:
                fig = render_daily_trend_chart(trend_df, height=280)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu xu hướng cho nguồn này.", height=280), use_container_width=True)
                
        with c_right:
            period_label = "month" if lang == "en" else "tháng"
            st.markdown(f"#### 📅 {t('chart_label_avg', lang)} {period_label} ({val_label})")
            monthly_df = get_monthly_trend(display_col, date_range, source_name, spatial_grain)
            if not monthly_df.empty:
                fig = render_monthly_average_chart(monthly_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không có dữ liệu trung bình tháng.", height=280), use_container_width=True)

        render_section_divider()

        # 4. Temporal heatmaps
        st.markdown(f"#### ⏱️ {t('weather_dispersal_analysis', lang)} ({val_label})")
        df_temporal = get_temporal_patterns(display_col, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None, source_name, date_range, spatial_grain, lang)
        
        if not df_temporal.empty:
            temporal_colorbar = get_aqi_colorbar_config(standard, val_label) if pollutant == "aqi" else {"title": {"text": val_label}}
            temporal_colorbar.update({"x": 1.01, "xanchor": "left", "xpad": 4, "len": 0.84, "thickness": 16})
            
            fig_temp = px.density_heatmap(
                df_temporal,
                x="hour_of_day",
                y="day_name",
                z="avg_aqi",
                color_continuous_scale=get_aqi_color_scale(standard) if pollutant == "aqi" else "Viridis",
                range_color=get_aqi_color_range(standard) if pollutant == "aqi" else None,
                labels={"hour_of_day": t("chart_label_hour", lang), "day_name": t("chart_label_day_of_week", lang), "avg_aqi": val_label},
            )
            fig_temp.update_layout(
                height=280,
                margin={"l": 20, "r": 130, "t": 10, "b": 42},
                coloraxis_colorbar=temporal_colorbar,
            )
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.caption("Chưa có dữ liệu temporal patterns cho nguồn này." if lang == "vi" else "No temporal patterns for this source.")

        render_section_divider()

        # 5. Province heatmap (User decided to "Giữ nguyên" height)
        st.markdown(f"#### 🌡️ {t('chart_heatmap', lang)} {val_label} - Tỉnh × Ngày")
        heatmap_data = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, source_name, time_unit)
        if not heatmap_data.empty:
            all_provs = heatmap_data.groupby("province")["display_val"].mean().sort_values(ascending=False).index.tolist()
            filtered = heatmap_data[heatmap_data["province"].isin(all_provs)]
            chart_height = max(380, len(all_provs) * 22)
            fig = render_province_day_heatmap(filtered, all_provs, chart_height)
            st.plotly_chart(fig, use_container_width=True)

    # ── Comparison tab renderer ──────────────────────────────────────────────────
    def render_comparison_historical_tab():
        st.markdown(f"#### 📊 So sánh Xu hướng thời gian ({val_label})")

        c_c1, c_c2 = st.columns(2)
        with c_c1:
            st.markdown("##### 📈 Biểu đồ so sánh xu hướng theo ngày")
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
                fig_line.update_layout(get_plotly_layout(height=380, compact=True), hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không đủ dữ liệu song hành để so sánh."), use_container_width=True)

        with c_c2:
            st.markdown("##### 📅 So sánh trung bình theo tháng")
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
                fig_monthly.update_layout(get_plotly_layout(height=380, compact=True))
                st.plotly_chart(fig_monthly, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không đủ dữ liệu tháng."), use_container_width=True)

        render_section_divider()

        # Heatmap of differences
        st.markdown("##### 🌡️ Bản đồ nhiệt chênh lệch (Mặt đất - Vệ tinh)")
        st.caption("Màu đỏ thể hiện trạm mặt đất đo cao hơn; màu xanh thể hiện mô hình vệ tinh ước lượng cao hơn.")

        g_heatmap = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, "aqiin", time_unit)
        s_heatmap = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, "openweather", time_unit)

        if not g_heatmap.empty and not s_heatmap.empty:
            merged_heat = pd.merge(g_heatmap, s_heatmap, on=["province", "date_str"], suffixes=("_ground", "_sat"))
            merged_heat["display_val"] = merged_heat["display_val_ground"] - merged_heat["display_val_sat"]

            all_provs = merged_heat.groupby("province")["display_val"].mean().sort_values(ascending=False).index.tolist()
            filtered = merged_heat[merged_heat["province"].isin(all_provs)]
            chart_height = max(380, len(all_provs) * 22)

            colorbar_config = {"title": {"text": f"Đo lệch {val_label}"}}
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
        else:
            st.plotly_chart(create_empty_state("Không đủ dữ liệu để tạo bản đồ so sánh lệch."), use_container_width=True)

    # ── Execute 3-Tab Renderer ──────────────────────────────────────────────
    render_3_tabs(
        lang=lang,
        ground_label="📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
        sat_label="🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
        comp_label="📊 So sánh Xu hướng" if lang == "vi" else "📊 Trend Comparison",
        render_ground_fn=lambda: render_source_historical_tab("aqiin"),
        render_sat_fn=lambda: render_source_historical_tab("openweather"),
        render_comp_fn=render_comparison_historical_tab,
        sat_info_text_vi="🛰️ Mô hình vệ tinh SILAM: Cung cấp chuỗi thời gian liên tục nhưng giá trị thường bị mịn hóa đáng kể.",
        sat_info_text_en="🛰️ Satellite SILAM Model: Continuous historical trends, with smoothing yielding lower values."
    )

if __name__ == "__main__":
    main()
