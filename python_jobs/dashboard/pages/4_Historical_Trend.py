"""Historical Trend page — daily/monthly AQI trends, province comparison, heatmap."""
from __future__ import annotations

import sys
sys.path.insert(0, "..")

import streamlit as st
import pandas as pd
import plotly.express as px

from lib.clickhouse_client import query_df
from lib.aqi_utils import get_epa_continuous_scale, render_empty_chart
from lib.filters import render_sidebar_filters
from lib.data_service import build_where_clause, get_pollutant_col, get_source_table, get_pollutant_cols
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(f"📈 {t('nav_trends', lang)}")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters["pollutant"]
standard = filters["standard"]

# Helper to format metric labels
val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
# Use unified helper for column mapping
display_col, max_col = get_pollutant_cols(pollutant, standard)

# ── helpers ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_national_daily_trend(col, max_col, dates):
    where_clause = build_where_clause(None, None, dates)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({max_col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_province_daily_trend(col, max_col, province: str, dates):
    where_clause = build_where_clause("Tỉnh", province, dates)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({max_col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_monthly_trend(col, max_col, dates):
    where_clause = build_where_clause(None, None, dates, date_col="date")
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({max_col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_monthly
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_heatmap_data(col, scope_grain, scope_val, dates):
    where_clause = build_where_clause(scope_grain, scope_val, dates)
    q = f"""
    SELECT
        province,
        toString(date)           AS date_str,
        round(avg({col}), 1) AS display_val
    FROM air_quality.dm_air_quality_overview_daily
    WHERE province IS NOT NULL AND province != ''
      AND {where_clause}
    GROUP BY province, date
    ORDER BY province, date
    """
    return query_df(q)

@st.cache_data(ttl=3600)
def get_temporal_patterns(province: str | None = None):
    where_clause = ""
    if province:
        where_clause = f"WHERE province = '{province}'"
    
    q = f"""
    SELECT
        hour_of_day,
        day_of_week,
        avg(avg_aqi_us) as avg_aqi
    FROM air_quality.dm_aqi_temporal_patterns
    {where_clause}
    GROUP BY hour_of_day, day_of_week
    ORDER BY day_of_week, hour_of_day
    """
    df = query_df(q)
    if not df.empty:
        day_map = {
            1: t("day_mon", lang) if lang=="en" else "Thứ 2", 
            2: t("day_tue", lang) if lang=="en" else "Thứ 3", 
            3: t("day_wed", lang) if lang=="en" else "Thứ 4", 
            4: t("day_thu", lang) if lang=="en" else "Thứ 5", 
            5: t("day_fri", lang) if lang=="en" else "Thứ 6", 
            6: t("day_sat", lang) if lang=="en" else "Thứ 7", 
            7: t("day_sun", lang) if lang=="en" else "Chủ nhật"
        }
        # Actually using keys from i18n is better
        day_names = [t(f"day_{d}", lang) for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]]
        day_map = {i+1: day_names[i] for i in range(7)}
        
        df["day_name"] = df["day_of_week"].map(day_map)
        df["day_name"] = pd.Categorical(df["day_name"], categories=day_names, ordered=True)
    return df

@st.cache_data(ttl=300)
def get_overall_stats(col, max_col, dates):
    where_clause = build_where_clause(None, None, dates)
    q = f"""
    SELECT
        count(distinct date)            AS total_days,
        round(avg({col}), 1)      AS overall_avg,
        round(min({col}), 1)      AS overall_min,
        round(max({max_col}), 0)      AS overall_max
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
    """
    return query_df(q)


# ── page body ─────────────────────────────────────────────────────────────────────

try:
    # ── KPI stats row ─────────────────────────────────────────────────────────
    with st.spinner("Đang tải thống kê..."):
        stats = get_overall_stats(display_col, max_col, date_range)
    if not stats.empty:
        row = stats.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(t("chart_label_days", lang), f"{int(row.total_days)} {t('chart_label_days', lang).lower()}")
        col2.metric(f"{t('chart_label_avg', lang)} {val_label}", f"{row.overall_avg:.0f}")
        col3.metric(f"{t('chart_label_min', lang)} {val_label}", f"{row.overall_min:.0f}")
        col4.metric(f"{t('chart_label_max', lang)} {val_label}", f"{row.overall_max:.0f}")

    # ── national daily trend ───────────────────────────────────────────────────
    st.subheader(f"{t('nav_overview', lang)} ({val_label})")
    national = get_national_daily_trend(display_col, max_col, date_range)
    if not national.empty:
        fig = px.line(
            national,
            x="date",
            y=["avg_val", "max_val"],
            labels={"date": t("chart_label_date", lang), "value": val_label, "variable": t("chart_label_type", lang),
                   "avg_val": t("chart_label_avg", lang), "max_val": t("chart_label_max", lang)},
            color_discrete_map={"avg_val": "#00A8E8", "max_val": "#FF0000"},
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(render_empty_chart("Không có dữ liệu xu hướng quốc gia."), use_container_width=True)

    # ── Temporal Patterns Heatmap ──────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"{t('weather_dispersal_analysis', lang)} (AQI)")
    with st.spinner(t("loading", lang) if lang=="en" else "Đang tải..."):
        df_temporal = get_temporal_patterns(scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)
    
    if not df_temporal.empty:
        fig_temp = px.density_heatmap(
            df_temporal,
            x="hour_of_day",
            y="day_name",
            z="avg_aqi",
            color_continuous_scale=get_epa_continuous_scale(),
            range_color=[0, 300],
            labels={"hour_of_day": t("chart_label_hour", lang), "day_name": t("chart_label_status", lang), "avg_aqi": "AQI US"}
        )
        fig_temp.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig_temp, use_container_width=True)
    else:
        st.caption("Chưa có dữ liệu temporal patterns cho vùng này.")

    # ── province daily trend (khi chọn tỉnh) ─────────────────────────────────
    if spatial_grain in ["Tỉnh", "Phường"] and scope_val:
        st.markdown("---")
        st.subheader(f"{t('nav_trends', lang)}: {scope_val} ({val_label})")
        prov_trend = get_province_daily_trend(display_col, max_col, scope_val, date_range)
        if not prov_trend.empty:
            fig = px.line(
                prov_trend,
                x="date",
                y=["avg_val", "max_val"],
                labels={"date": t("chart_label_date", lang), "value": val_label, "variable": t("chart_label_type", lang),
                       "avg_val": t("chart_label_avg", lang), "max_val": t("chart_label_max", lang)},
                color_discrete_map={"avg_val": "#00A8E8", "max_val": "#FF0000"},
            )
            fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=40))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.plotly_chart(render_empty_chart(f"Không có dữ liệu."), use_container_width=True)

    # ── monthly trend ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"{t('chart_label_avg', lang)} {t('chart_label_date', lang).lower()} ({val_label})")
    monthly_df = get_monthly_trend(display_col, max_col, date_range)
    if not monthly_df.empty:
        fig = px.bar(
            monthly_df,
            x="date",
            y="avg_val",
            text="avg_val",
            labels={"date": t("chart_label_date", lang), "avg_val": val_label},
            color="avg_val",
            color_continuous_scale=get_epa_continuous_scale() if pollutant == "aqi" else "Viridis",
        )
        fig.update_layout(height=280, showlegend=False, margin=dict(l=0, r=0, t=10, b=40))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── heatmap ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"Bản đồ nhiệt {val_label} - Tỉnh × Ngày")
    heatmap_data = get_heatmap_data(display_col, spatial_grain, scope_val, date_range)
    if not heatmap_data.empty:
        all_provs = (
            heatmap_data.groupby("province")["display_val"]
            .mean()
            .sort_values(ascending=False)
            .index.tolist()
        )
        filtered = heatmap_data[heatmap_data["province"].isin(all_provs)]
        
        # Calculate dynamic height: 20px per province + margins, min 380px
        chart_height = max(380, len(all_provs) * 22)
        
        fig = px.density_heatmap(
            filtered,
            x="date_str",
            y="province",
            z="display_val",
            color_continuous_scale=get_epa_continuous_scale() if pollutant == "aqi" else "Viridis",
            labels={"date_str": t("chart_label_date", lang), "province": t("province", lang), "display_val": val_label},
            category_orders={"province": all_provs} # Keep sorted order
        )
        fig.update_layout(height=chart_height, margin=dict(l=0, r=0, t=10, b=30))
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
