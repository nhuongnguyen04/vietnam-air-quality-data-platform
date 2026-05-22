"""
Trang Chất ô nhiễm (Pollutants) phân tích chi tiết nồng độ các chất gây ô nhiễm chính
(PM2.5, PM10, NO2, O3, CO, SO2) theo thời gian và địa điểm, giúp người dùng hiểu sâu
về thành phần ô nhiễm tại khu vực.
"""
import pandas as pd
import plotly.express as px
import streamlit as st
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause, get_pollutant_cols, get_source_table
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.style import get_plotly_layout

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_grain    = filters["time_grain"]
time_unit     = filters["time_unit"]
scope_val     = filters["scope_val"]
date_range    = filters["date_range"]
pollutant     = filters["pollutant"]
standard      = filters["standard"]

# Helper to format metric labels
val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
# Use unified helper for column mapping
display_col, max_col = get_pollutant_cols(pollutant, standard)

# ── Helpers ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_pollutant_trend(table: str, grain: str, scope_val: str | None, dates, source: str, tunit: str = "day"):
    where_clause = build_where_clause(grain, scope_val, dates, time_unit=tunit)
    
    # Append source filter to where_clause
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


def render_source_pollutants_tab(source_name: str, source_mix: str):
    # 1. Trends
    st.subheader(f"{t('nav_pollutants', lang)} (AQI VN)")
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
        fig.update_layout(get_plotly_layout(height=400), hovermode="x unified")
        fig.update_xaxes(tickformat="%d/%m/%Y", hoverformat="%d/%m/%Y")
        
        if highlight_poll:
            for trace in fig.data:
                if highlight_poll in trace.name:
                    trace.line.width = 5
                else:
                    trace.line.width = 1.5
                    trace.opacity = 0.6
                    
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Không có dữ liệu xu hướng chất ô nhiễm cho nguồn này." if lang == "vi" else "No pollutant trend data for this source.")

    st.markdown("---")
    
    # 2. Source attribution & Compliance status
    c1, c2 = st.columns([1, 1.5])
    with c1:
        st.subheader(t("source_attribution", lang))
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
                hole=0.4,
                color_discrete_map=color_map,
                labels={'source_label': t('chart_label_type', lang), 'cnt': t('chart_label_count', lang)}
            )
            fig_pie.update_layout(
                height=390,
                margin={"l": 20, "r": 20, "t": 20, "b": 70},
                legend={
                    "orientation": "h",
                    "yanchor": "top",
                    "y": -0.08,
                    "xanchor": "center",
                    "x": 0.5,
                    "title_text": None,
                },
            )
            st.plotly_chart(fig_pie, width='stretch')
        else:
            st.caption(t("no_data", lang) if lang=="en" else "Chưa có dữ liệu phân tích nguồn.")
            
    with c2:
        st.subheader(t("compliance_status_title", lang))
        compliance = get_compliance_status(spatial_grain, scope_val, date_range, source_mix, time_unit)
        if not compliance.empty:
            comp_map = {
                'Good/Safe': t('compliance_good', lang),
                'Warning (WHO Breach)': t('compliance_who', lang),
                'Unhealthy (TCVN Breach)': t('compliance_tcvn', lang)
            }
            compliance['status_label'] = compliance['compliance_status'].map(comp_map).fillna(compliance['compliance_status'])
            
            color_map = {
                t('compliance_good', lang): "#09ab3b",
                t('compliance_who', lang): "#ffa500",
                t('compliance_tcvn', lang): "#ff4b4b"
            }
            cat_orders = [t('compliance_good', lang), t('compliance_who', lang), t('compliance_tcvn', lang)]
            province_order = compliance["province"].drop_duplicates().tolist()
            
            fig_comp = px.bar(compliance, x="province", y="cnt", color="status_label",
                             color_discrete_map=color_map,
                             category_orders={"province": province_order, "status_label": cat_orders},
                             labels={"province": t("province", lang), "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)},
                             barmode="stack")
            fig_comp.update_layout(
                get_plotly_layout(height=390),
                barnorm="percent",
                margin={"l": 50, "r": 60, "t": 50, "b": 95},
            )
            fig_comp.update_xaxes(
                tickangle=-90,
                automargin=True,
                range=[-0.5, len(province_order) - 0.1],
            )
            fig_comp.update_yaxes(range=[0, 100], ticksuffix="%")
            st.plotly_chart(fig_comp, width='stretch')
        else:
            st.caption(t("no_data", lang) if lang=="en" else "Chưa có dữ liệu tuân thủ chuẩn.")


def render_pollutants_comparison_tab():
    st.subheader("📊 So sánh Xu hướng Chất ô nhiễm (Mặt đất vs Vệ tinh)" if lang == "vi" else "📊 Pollutant Trend Comparison (Ground vs Sat)")
    st.markdown(
        "So sánh nồng độ trung bình hoặc giá trị AQI của chất ô nhiễm đã chọn ở sidebar giữa dữ liệu quan trắc thực tế và mô hình vệ tinh."
        if lang == "vi" else
        "Compare concentrations or AQI value of the selected pollutant between direct observations and satellite model."
    )
    
    # Selected pollutant overlay chart
    st.markdown(f"### 📈 Xu hướng chất ô nhiễm: {val_label}" if lang == "vi" else f"### 📈 Pollutant Trend: {val_label}")
    comp_trend = get_specific_pollutant_trend(spatial_grain, scope_val, date_range, display_col, time_unit)
    if not comp_trend.empty:
        plot_df = comp_trend.copy()
        plot_df["Nguồn"] = plot_df["source"].apply(lambda x: "📡 Quan trắc mặt đất" if x == "aqiin" else "🛰️ Mô hình vệ tinh")
        
        fig = px.line(
            plot_df,
            x="date",
            y="value",
            color="Nguồn",
            labels={
                "date": t("chart_label_date", lang),
                "value": val_label,
            },
            color_discrete_map={"📡 Quan trắc mặt đất": "#2563eb", "🛰️ Mô hình vệ tinh": "#f97316"}
        )
        fig.update_layout(get_plotly_layout(height=400), hovermode="x unified")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Không có dữ liệu so sánh cho chất ô nhiễm này." if lang == "vi" else "No comparison data for this pollutant.")

    st.markdown("---")

    # Side-by-side compliance summary
    st.subheader("📋 So sánh mức độ tuân thủ tiêu chuẩn không khí (WHO vs TCVN)" if lang == "vi" else "📋 Standard Compliance Comparison (WHO vs TCVN)")
    st.markdown("So sánh tỷ lệ ngày tuân thủ giữa trạm mặt đất và mô hình mô phỏng vệ tinh." if lang == "vi" else "Compare standard compliance rates between ground monitors and satellite simulations.")
    
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
            t('compliance_good', lang): "#09ab3b",
            t('compliance_who', lang): "#ffa500",
            t('compliance_tcvn', lang): "#ff4b4b"
        }
        
        fig_comp = px.bar(
            combined_comp,
            x="Nguồn",
            y="cnt",
            color="status_label",
            color_discrete_map=color_map,
            barmode="stack",
            labels={"Nguồn": "Nguồn dữ liệu" if lang == "vi" else "Data Source", "cnt": t("chart_label_count", lang), "status_label": t("chart_label_status", lang)}
        )
        fig_comp.update_layout(get_plotly_layout(height=350), barnorm="percent")
        fig_comp.update_yaxes(ticksuffix="%")
        st.plotly_chart(fig_comp, width="stretch")
    else:
        st.info("Không đủ dữ liệu từ cả hai nguồn để so sánh chuẩn tuân thủ." if lang == "vi" else "Not enough data from both sources to compare standards.")


# ── UI Header ─────────────────────────────────────────────────────────────────
st.title(t("nav_pollutants", lang))

# Determine Source Table
table_name = get_source_table(spatial_grain, time_grain)

try:
    # ── 3-Tab Layout ─────────────────────────────────────────────────────────
    tab_ground, tab_sat, tab_corr = st.tabs([
        "📡 Quan trắc mặt đất" if lang == "vi" else "📡 Ground Monitors",
        "🛰 Mô hình vệ tinh" if lang == "vi" else "🛰 Satellite Model",
        "📊 So sánh chỉ số" if lang == "vi" else "📊 Pollutant Comparison"
    ])
    
    with tab_ground:
        render_source_pollutants_tab("aqiin", "observed")
        
    with tab_sat:
        st.info(
            "🛰️ **Lưu ý mô hình vệ tinh (SILAM):** Các chất ô nhiễm thứ cấp (đặc biệt là hạt mịn PM2.5) có nồng độ mô phỏng thấp hơn so với thực tế đo tại trạm mặt đất do đặc thù phân giải lưới ~25km."
            if lang == "vi" else
            "🛰️ **Note on Satellite model (SILAM):** Secondary pollutants (especially fine particles PM2.5) simulated concentrations are generally lower than ground monitor observations due to ~25km grid resolution smoothing."
        )
        render_source_pollutants_tab("openweather", "modeled")
        
    with tab_corr:
        render_pollutants_comparison_tab()

    st.markdown("---")
    st.info("Phân tích nguồn dựa trên tỷ lệ PM2.5/PM10. Tỷ lệ > 0.6 gợi ý hoạt động đốt cháy/giao thông, < 0.4 gợi ý bụi/xây dựng." if lang == "vi" else "Source fingerprint analysis based on PM2.5/PM10 ratio. Ratio > 0.6 indicates combustion/traffic; < 0.4 suggests dust/construction.")

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
