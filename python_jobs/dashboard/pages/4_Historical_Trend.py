"""Historical Trend page — daily/monthly AQI trends, province comparison, heatmap."""
from __future__ import annotations

# ruff: noqa: E402
import sys

sys.path.insert(0, "..")

"""
Trang Xu hướng Lịch sử (Historical Trend) hiển thị sự biến đổi của chất lượng không khí
theo thời gian (giờ, ngày, tháng, năm). Giúp xác định các chu kỳ ô nhiễm và đánh giá
hiệu quả của các biện pháp cải thiện môi trường.
"""
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
from lib.data_service import build_where_clause, get_pollutant_cols
from lib.filters import render_sidebar_filters
from lib.i18n import t
from lib.style import get_plotly_layout

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(f"📈 {t('nav_trends', lang)}")

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
time_unit     = filters["time_unit"]
scope_val     = filters["scope_val"]
date_range    = filters["date_range"]
pollutant     = filters["pollutant"]
standard      = filters["standard"]

# Helper to format metric labels
val_label = "AQI" if pollutant == "aqi" else pollutant.upper()
# Use unified helper for column mapping
display_col, max_col = get_pollutant_cols(pollutant, standard)

# ── helpers ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_national_daily_trend(col, dates, tunit="day"):
    where_clause = build_where_clause(None, None, dates, time_unit=tunit)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_province_daily_trend(col, province: str, dates, tunit="day"):
    where_clause = build_where_clause("Tỉnh", province, dates, time_unit=tunit)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
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
def get_monthly_trend(col, dates):
    where_clause = build_monthly_where_clause(dates)
    q = f"""
    SELECT
        date,
        round(avg({col}), 1)  AS avg_val,
        round(max({col}), 0)  AS max_val
    FROM air_quality.dm_air_quality_overview_monthly
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
    """
    return query_df(q)


@st.cache_data(ttl=300)
def get_heatmap_data(col, scope_grain, scope_val, dates, tunit="day"):
    where_clause = build_where_clause(scope_grain, scope_val, dates, time_unit=tunit)
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
def get_temporal_patterns(col: str, province: str | None = None):
    where_clause = ""
    if province:
        where_clause = f"WHERE province = '{province}'"

    q = f"""
    SELECT
        hour_of_day,
        day_of_week,
        avg({col}) as avg_aqi
    FROM air_quality.dm_aqi_temporal_patterns
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

@st.cache_data(ttl=300)
def get_overall_stats(col, dates, tunit="day"):
    where_clause = build_where_clause(None, None, dates, time_unit=tunit)
    q = f"""
    SELECT
        count(distinct date)            AS total_days,
        round(avg({col}), 1)      AS overall_avg,
        round(min({col}), 1)      AS overall_min,
        round(max({col}), 0)      AS overall_max
    FROM air_quality.dm_air_quality_overview_daily
    WHERE {where_clause}
    """
    return query_df(q)


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
        color_discrete_map={avg_label: "#00A8E8", max_label: "#FF0000"},
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
        hover_data={"period": True, "avg_val": ":.1f"},
    )
    fig.update_layout(
        get_plotly_layout(height=280),
        showlegend=False,
        bargap=0.55,
        coloraxis_showscale=False,
        margin={"l": 20, "r": 20, "t": 10, "b": 45},
        xaxis={"type": "category", "title": period_label},
        yaxis={"rangemode": "tozero", "title": val_label},
    )
    fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
    return fig


def get_average_metric_label(metric: str) -> str:
    """Return a localized label for averaged chart values."""
    avg_label = t("chart_label_avg", lang)
    if lang == "vi":
        return f"{metric} {avg_label.lower()}"
    return f"{avg_label} {metric}"


def get_heatmap_date_format() -> str:
    """Return the Plotly date format for heatmap ticks and hover labels."""
    if lang == "vi":
        return "%d/%m/%Y"
    return "%b %d, %Y"


def render_province_day_heatmap(df: pd.DataFrame, provinces: list[str], height: int):
    metric_label = get_average_metric_label(val_label)
    colorbar_config = (
        get_aqi_colorbar_config(standard, metric_label)
        if pollutant == "aqi"
        else {"title": {"text": metric_label}}
    )
    colorbar_config.update(
        {
            "x": 1.02,
            "xanchor": "left",
            "xpad": 8,
            "len": 0.84,
            "thickness": 16,
        }
    )
    plot_df = df.copy()
    plot_df["date"] = pd.to_datetime(plot_df["date_str"])

    dates = sorted(plot_df["date"].dropna().unique())
    matrix = (
        plot_df.pivot_table(
            index="province",
            columns="date",
            values="display_val",
            aggfunc="mean",
        )
        .reindex(index=provinces, columns=dates)
    )

    fig = px.imshow(
        matrix,
        x=dates,
        y=provinces,
        color_continuous_scale=get_aqi_color_scale(standard) if pollutant == "aqi" else "Viridis",
        range_color=get_aqi_color_range(standard) if pollutant == "aqi" else None,
        aspect="auto",
        labels={
            "x": t("chart_label_date", lang),
            "y": t("province", lang),
            "color": metric_label,
        },
    )
    fig.update_layout(
        height=height,
        margin={"l": 20, "r": 110, "t": 10, "b": 58},
        xaxis={"title": t("chart_label_date", lang), "automargin": True},
        yaxis={"title": t("province", lang), "automargin": True},
        coloraxis_colorbar=colorbar_config,
    )
    date_format = get_heatmap_date_format()
    fig.update_xaxes(tickformat=date_format.replace("/%Y", "<br>%Y"))
    fig.update_traces(
        hovertemplate=(
            f"{t('province', lang)}: %{{y}}<br>"
            f"{t('chart_label_date', lang)}: %{{x|{date_format}}}<br>"
            f"{metric_label}: %{{z:.1f}}<extra></extra>"
        )
    )
    return fig


# ── page body ─────────────────────────────────────────────────────────────────────

try:
    # ── KPI stats row ─────────────────────────────────────────────────────────
    with st.spinner("Đang tải thống kê..."):
        stats = get_overall_stats(display_col, date_range, time_unit)
    if not stats.empty:
        row = stats.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(t("chart_label_days", lang), f"{int(row.total_days)} {t('chart_label_days', lang).lower()}")
        col2.metric(f"{t('chart_label_avg', lang)} {val_label}", f"{row.overall_avg:.0f}" if pd.notna(row.overall_avg) else "N/A")
        col3.metric(f"{t('chart_label_min', lang)} {val_label}", f"{row.overall_min:.0f}" if pd.notna(row.overall_min) else "N/A")
        col4.metric(f"{t('chart_label_max', lang)} {val_label}", f"{row.overall_max:.0f}" if pd.notna(row.overall_max) else "N/A")

    # ── national daily trend ───────────────────────────────────────────────────
    st.subheader(f"{t('nav_overview', lang)} ({val_label})")
    national = get_national_daily_trend(display_col, date_range, time_unit)
    if not national.empty:
        fig = render_daily_trend_chart(national, height=300)
        st.plotly_chart(fig, width='stretch')
    else:
        st.plotly_chart(render_empty_chart("Không có dữ liệu xu hướng quốc gia."), width='stretch')

    # ── Temporal Patterns Heatmap ──────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"{t('weather_dispersal_analysis', lang)} (AQI)")
    with st.spinner(t("loading", lang) if lang=="en" else "Đang tải..."):
        df_temporal = get_temporal_patterns(display_col, scope_val if spatial_grain in ["Tỉnh", "Phường"] else None)

    if not df_temporal.empty:
        temporal_colorbar = get_aqi_colorbar_config(standard, "AQI")
        temporal_colorbar.update(
            {
                "x": 0.935,
                "xanchor": "left",
                "xpad": 6,
                "len": 0.82,
                "thickness": 16,
            }
        )
        fig_temp = px.density_heatmap(
            df_temporal,
            x="hour_of_day",
            y="day_name",
            z="avg_aqi",
            color_continuous_scale=get_aqi_color_scale(standard),
            range_color=get_aqi_color_range(standard),
            labels={
                "hour_of_day": t("chart_label_hour", lang),
                "day_name": t("chart_label_day_of_week", lang),
                "avg_aqi": "AQI",
            },
        )
        fig_temp.update_layout(
            height=350,
            margin={"l": 20, "r": 60, "t": 10, "b": 42},
            xaxis={"domain": [0, 0.9]},
            coloraxis_colorbar=temporal_colorbar,
        )
        st.plotly_chart(fig_temp, width='stretch')
    else:
        st.caption("Chưa có dữ liệu temporal patterns cho vùng này.")

    # ── province daily trend (khi chọn tỉnh) ─────────────────────────────────
    if spatial_grain in ["Tỉnh", "Phường"] and scope_val:
        st.markdown("---")
        st.subheader(f"{t('nav_trends', lang)}: {scope_val} ({val_label})")
        prov_trend = get_province_daily_trend(display_col, scope_val, date_range, time_unit)
        if not prov_trend.empty:
            fig = render_daily_trend_chart(prov_trend, height=280)
            st.plotly_chart(fig, width='stretch')
        else:
            st.plotly_chart(render_empty_chart("Không có dữ liệu."), width='stretch')

    # ── monthly trend ─────────────────────────────────────────────────────────
    st.markdown("---")
    period_label = "month" if lang == "en" else "tháng"
    st.subheader(f"{t('chart_label_avg', lang)} {period_label} ({val_label})")
    monthly_df = get_monthly_trend(display_col, date_range)
    if not monthly_df.empty:
        fig = render_monthly_average_chart(monthly_df)
        st.plotly_chart(fig, width='stretch')

    # ── heatmap ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"{t('chart_heatmap', lang)} {val_label} - {t('province', lang)} × {t('chart_label_date', lang)}")
    heatmap_data = get_heatmap_data(display_col, spatial_grain, scope_val, date_range, time_unit)
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

        fig = render_province_day_heatmap(filtered, all_provs, chart_height)
        st.plotly_chart(fig, width='stretch')

except Exception as e:
    st.error(f"Truy vấn thất bại: {e}")
    st.info("Kiểm tra ClickHouse và dbt transform đã chạy thành công.")
