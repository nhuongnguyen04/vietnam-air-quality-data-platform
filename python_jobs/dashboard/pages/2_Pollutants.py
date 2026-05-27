"""
Pollutants analysis page.
Analyzes pollutant concentrations, source attribution, and standard compliance.
"""
import pandas as pd
import plotly.express as px
import streamlit as st

from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause, get_pollutant_cols, get_source_table
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.chart_config import get_plotly_layout, create_empty_state, SOURCE_PALETTE
from lib.tab_renderer import render_3_tabs

@st.cache_data(ttl=300)
def get_pollutant_trend(table: str, grain: str, scope_val: str | None, dates, source: str, tunit: str = "day"):
    where_clause = build_where_clause(grain, scope_val, dates, time_unit=tunit)
    if where_clause:
        where_clause += f" AND source = '{source}'"
    else:
        where_clause = f"source = '{source}'"

    if table.endswith("_hourly") or tunit == "hour":
        q = f"""
        SELECT
            datetime_hour AS date,
            round(avg(pm25_aqi), 1) AS pm25_aqi,
            round(avg(pm10_aqi), 1) AS pm10_aqi,
            round(avg(co_aqi), 1)   AS co_aqi,
            round(avg(no2_aqi), 1)  AS no2_aqi,
            round(avg(so2_aqi), 1)  AS so2_aqi,
            round(avg(o3_aqi), 1)   AS o3_aqi
        FROM air_quality.fct_air_quality_summary_hourly
        WHERE {where_clause}
        GROUP BY datetime_hour
        ORDER BY datetime_hour
        """
        return query_df(q)

    q = f"""
    SELECT
        date,
        round(avg(pm25_daily_aqi), 1)   AS pm25_aqi,
        round(avg(pm10_daily_aqi), 1)   AS pm10_aqi,
        round(avg(co_daily_aqi), 1)     AS co_aqi,
        round(avg(no2_daily_aqi), 1)    AS no2_aqi,
        round(avg(so2_daily_aqi), 1)   AS so2_aqi,
        round(avg(o3_daily_aqi), 1)     AS o3_aqi
    FROM air_quality.fct_air_quality_summary_daily
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_specific_pollutant_trend(grain: str, scope_val: str | None, dates, pollutant_col: str, tunit: str = "day"):
    where_clause = build_where_clause(grain, scope_val, dates, time_unit=tunit)
    if pollutant_col == "avg_aqi_vn":
        h_col = "final_aqi_vn"
        d_col = "daily_avg_aqi_vn"
    elif pollutant_col == "avg_aqi_us":
        h_col = "final_aqi_us"
        d_col = "daily_avg_aqi_us"
    else:
        poll_base = pollutant_col.split("_")[0]
        h_col = f"{poll_base}_value"
        d_col = f"{poll_base}_daily_avg"

    if tunit == "hour":
        q = f"""
        SELECT
            datetime_hour AS date,
            source,
            round(avg({h_col}), 1) AS value
        FROM air_quality.fct_air_quality_summary_hourly
        WHERE {where_clause}
        GROUP BY datetime_hour, source
        ORDER BY datetime_hour, source
        """
    else:
        q = f"""
        SELECT
            date,
            source,
            round(avg({d_col}), 1) AS value
        FROM air_quality.fct_air_quality_summary_daily
        WHERE {where_clause}
        GROUP BY date, source
        ORDER BY date, source
        """
    return query_df(q)

@st.cache_data(ttl=300)
def get_source_fingerprint(grain: str, scope_val: str | None, dates, source_mix: str, tunit: str = "day"):
    where_clause = build_where_clause(grain, scope_val, dates, time_unit=tunit)
    if where_clause:
        where_clause += f" AND source_mix = '{source_mix}'"
    else:
        where_clause = f"source_mix = '{source_mix}'"

    q = f"""
    SELECT
        probable_source,
        count(*) as cnt
    FROM air_quality.dm_pollutant_source_fingerprint
    WHERE {where_clause}
    GROUP BY probable_source
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_compliance_status(grain: str, scope_val: str | None, dates, source_mix: str, tunit: str = "day"):
    where_clause = build_where_clause(grain, scope_val, dates, time_unit=tunit)
    if where_clause:
        where_clause += f" AND source_mix = '{source_mix}'"
    else:
        where_clause = f"source_mix = '{source_mix}'"

    q = f"""
    SELECT
        province,
        compliance_status,
        count(*) AS cnt
    FROM air_quality.dm_aqi_compliance_standards
    WHERE {where_clause}
    GROUP BY province, compliance_status
    ORDER BY province, cnt DESC
    """
    return query_df(q)

@page_wrapper("nav_pollutants", "📊 Pollutants Analysis Dashboard", icon="📊")
def main(lang: str):
    # ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
    filters = render_sidebar_filters()
    spatial_grain = filters["spatial_grain"]
    time_grain    = filters["time_grain"]
    time_unit     = filters["time_unit"]
    scope_val     = filters["scope_val"]
    date_range    = filters["date_range"]
    pollutant     = filters["pollutant"]
    standard      = filters["standard"]

    val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
    display_col, max_col = get_pollutant_cols(pollutant, standard)
    table_name = get_source_table(spatial_grain, time_grain)

    # ── Source pollutants renderer ─────────────────────────────────────────────
    def render_source_pollutants_tab(source_name: str, source_mix: str):
        st.markdown(f"#### 📈 {t('nav_pollutants', lang)} (AQI VN)")
        trend = get_pollutant_trend(table_name, spatial_grain, scope_val, date_range, source_name, time_unit)

        if not trend.empty:
            col_map = {
                "pm25_aqi": t("pollutant_pm25", lang), "pm10_aqi": t("pollutant_pm10", lang),
                "o3_aqi": t("pollutant_o3", lang), "no2_aqi": t("pollutant_no2", lang),
                "so2_aqi": t("pollutant_so2", lang), "co_aqi": t("pollutant_co", lang)
            }
            plot_df = trend.rename(columns=col_map)
            display_pollutants = list(col_map.values())
            highlight_poll = pollutant.upper() if pollutant != "aqi" else None

            fig = px.line(
                plot_df,
                x="date",
                y=display_pollutants,
                labels={
                    "date": t("chart_label_date", lang),
                    "value": t("chart_label_aqi", lang),
                    "variable": t("chart_label_variable", lang),
                },
            )
            fig.update_layout(get_plotly_layout(height=380), hovermode="x unified")
            fig.update_xaxes(tickformat="%d/%m/%Y", hoverformat="%d/%m/%Y")

            if highlight_poll:
                for trace in fig.data:
                    if highlight_poll in trace.name:
                        trace.line.width = 4.5
                    else:
                        trace.line.width = 1.2
                        trace.opacity = 0.4

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.plotly_chart(create_empty_state("No data trend available for this source"), use_container_width=True)

        render_section_divider()

        # Source attribution & Compliance
        c1, c2 = st.columns([1, 1.5])
        with c1:
            st.markdown(f"#### 🎨 {t('source_attribution', lang)}")
            df_source = get_source_fingerprint(spatial_grain, scope_val, date_range, source_mix, time_unit)
            if not df_source.empty:
                source_map = {
                    'Combustion/Traffic': t('source_traffic', lang),
                    'Dust/Construction': t('source_dust', lang),
                    'Mixed': t('source_mixed', lang)
                }
                df_source['source_label'] = df_source['probable_source'].map(source_map).fillna(df_source['probable_source'])
                color_map = {
                    t('source_traffic', lang): '#ff7f0e',
                    t('source_dust', lang): '#8c564b',
                    t('source_mixed', lang): '#7f7f7f'
                }

                fig_pie = px.pie(
                    df_source,
                    values='cnt',
                    names='source_label',
                    color='source_label',
                    hole=0.45,
                    color_discrete_map=color_map,
                )
                fig_pie.update_layout(
                    get_plotly_layout(height=360, compact=True),
                    annotations=[{"text": "Sources", "showarrow": False, "font_size": 14, "font_family": "Outfit"}]
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Chưa có dữ liệu phân tích nguồn."), use_container_width=True)

        with c2:
            st.markdown(f"#### 📋 {t('compliance_status_title', lang)}")
            compliance = get_compliance_status(spatial_grain, scope_val, date_range, source_mix, time_unit)
            if not compliance.empty:
                comp_map = {
                    'Good/Safe': t('compliance_good', lang),
                    'Warning (WHO Breach)': t('compliance_who', lang),
                    'Unhealthy (TCVN Breach)': t('compliance_tcvn', lang)
                }
                compliance['status_label'] = compliance['compliance_status'].map(comp_map).fillna(compliance['compliance_status'])
                color_map = {
                    t('compliance_good', lang): "#10B981",
                    t('compliance_who', lang): "#F59E0B",
                    t('compliance_tcvn', lang): "#EF4444"
                }
                cat_orders = [t('compliance_good', lang), t('compliance_who', lang), t('compliance_tcvn', lang)]
                province_order = compliance["province"].drop_duplicates().tolist()

                fig_comp = px.bar(
                    compliance, x="province", y="cnt", color="status_label",
                    color_discrete_map=color_map,
                    category_orders={"province": province_order, "status_label": cat_orders},
                    labels={"province": t("province", lang), "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)},
                    barmode="stack"
                )
                fig_comp.update_layout(
                    get_plotly_layout(height=360, compact=True),
                    barnorm="percent",
                )
                fig_comp.update_xaxes(tickangle=-45)
                fig_comp.update_yaxes(ticksuffix="%")
                st.plotly_chart(fig_comp, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Chưa có dữ liệu tuân thủ chuẩn."), use_container_width=True)

    # ── Comparison tab renderer ──────────────────────────────────────────────────
    def render_pollutants_comparison_tab():
        st.markdown(f"#### ⚖️ {t('chart_aqi_by_source', lang)}")
        
        c_comp1, c_comp2 = st.columns(2)
        with c_comp1:
            st.markdown(f"##### 📈 Xu hướng chất ô nhiễm: {val_label}")
            comp_trend = get_specific_pollutant_trend(spatial_grain, scope_val, date_range, display_col, time_unit)
            if not comp_trend.empty:
                plot_df = comp_trend.copy()
                plot_df["Nguồn"] = plot_df["source"].apply(lambda x: "📡 Quan trắc mặt đất" if x == "aqiin" else "🛰️ Mô hình vệ tinh")

                fig = px.line(
                    plot_df,
                    x="date",
                    y="value",
                    color="Nguồn",
                    labels={"date": t("chart_label_date", lang), "value": val_label},
                    color_discrete_map={"📡 Quan trắc mặt đất": SOURCE_PALETTE["📡 Mặt đất"], "🛰️ Mô hình vệ tinh": SOURCE_PALETTE["🛰️ Vệ tinh"]}
                )
                fig.update_layout(get_plotly_layout(height=380, compact=True), hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("No data for comparison"), use_container_width=True)

        with c_comp2:
            st.markdown("##### 📋 So sánh chuẩn tuân thủ (WHO vs TCVN)")
            g_comp = get_compliance_status(spatial_grain, scope_val, date_range, "observed", time_unit)
            s_comp = get_compliance_status(spatial_grain, scope_val, date_range, "modeled", time_unit)

            if not g_comp.empty and not s_comp.empty:
                g_comp["Nguồn"] = "📡 Mặt đất" if lang == "vi" else "📡 Ground"
                s_comp["Nguồn"] = "🛰️ Vệ tinh" if lang == "vi" else "🛰️ Sat"

                combined_comp = pd.concat([g_comp, s_comp])
                comp_map = {
                    'Good/Safe': t('compliance_good', lang),
                    'Warning (WHO Breach)': t('compliance_who', lang),
                    'Unhealthy (TCVN Breach)': t('compliance_tcvn', lang)
                }
                combined_comp['status_label'] = combined_comp['compliance_status'].map(comp_map).fillna(combined_comp['compliance_status'])
                color_map = {
                    t('compliance_good', lang): "#10B981",
                    t('compliance_who', lang): "#F59E0B",
                    t('compliance_tcvn', lang): "#EF4444"
                }

                fig_comp = px.bar(
                    combined_comp,
                    x="Nguồn",
                    y="cnt",
                    color="status_label",
                    color_discrete_map=color_map,
                    barmode="stack",
                    labels={"Nguồn": "Nguồn dữ liệu", "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)}
                )
                fig_comp.update_layout(get_plotly_layout(height=380, compact=True), barnorm="percent")
                fig_comp.update_yaxes(ticksuffix="%")
                st.plotly_chart(fig_comp, use_container_width=True)
            else:
                st.plotly_chart(create_empty_state("Không đủ dữ liệu từ cả hai nguồn."), use_container_width=True)

    # ── Execute 3-Tab Renderer ──────────────────────────────────────────────
    render_3_tabs(
        lang=lang,
        ground_label="📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
        sat_label="🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
        comp_label="📊 So sánh chỉ số" if lang == "vi" else "📊 Pollutant Comparison",
        render_ground_fn=lambda: render_source_pollutants_tab("aqiin", "observed"),
        render_sat_fn=lambda: render_source_pollutants_tab("openweather", "modeled"),
        render_comp_fn=render_pollutants_comparison_tab,
        sat_info_text_vi="🛰️ Vệ tinh (SILAM): Do độ phân giải lưới ~25km, các đỉnh nồng độ ô nhiễm cao (đặc biệt hạt mịn PM2.5) sẽ mịn hóa và thấp hơn trạm mặt đất.",
        sat_info_text_en="🛰️ Satellite SILAM: Fine grid smoothing typically reports lower extreme pollution peaks compared to localized ground stations."
    )

    st.markdown("---")
    st.info("💡 Phân tích nguồn dựa trên tỷ lệ PM2.5/PM10. Tỷ lệ > 0.6 gợi ý hoạt động đốt cháy/giao thông, < 0.4 gợi ý bụi/xây dựng." if lang == "vi" else "💡 Source fingerprint based on PM2.5/PM10 ratio. Ratio > 0.6 suggests combustion/traffic; < 0.4 suggests dust/construction.")

if __name__ == "__main__":
    main()
