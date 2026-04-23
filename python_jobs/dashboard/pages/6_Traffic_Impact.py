"""
Trang Ảnh hưởng Giao thông (Traffic Impact) phân tích mối tương quan giữa mật độ 
phương tiện giao thông và chất lượng không khí. Giúp xác định vai trò của khí thải 
giao thông trong việc gây ô nhiễm tại các đô thị lớn.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib.clickhouse_client import query_df
from lib.data_service import build_where_clause
from lib.style import render_metric_card, get_plotly_layout
from lib.aqi_utils import render_empty_chart
from lib.i18n import t
from lib.filters import render_sidebar_filters

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("traffic_title", lang))

# ── Sidebar Filters (Synchronized) ────────────────────────────────────────────
filters = render_sidebar_filters()
spatial_grain = filters["spatial_grain"]
scope_val = filters["scope_val"]
date_range = filters["date_range"]
pollutant = filters.get("pollutant", "pm25")

# ── Dynamic Mapping ───────────────────────────────────────────────────────────
target_poll = "pm25" if pollutant not in ["pm10"] else pollutant

if pollutant != "pm25":
    st.warning(f"⚠️ Traffic cards use observed **PM2.5** and traffic overlap, not a causal estimate for **{pollutant.upper()}**.")

# ── Data Fetching ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_traffic_correlation_hourly(dates, grain, scope, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"
    
    q = f"""
    SELECT
        toTimeZone(datetime_hour, 'Asia/Ho_Chi_Minh') as datetime_hour,
        avg(avg_congestion) as avg_congestion,
        avg({target_col}) as avg_p,
        avg(traffic_coverage_ratio) as traffic_coverage_ratio,
        sum(traffic_ward_count) as traffic_ward_count
    FROM air_quality.dm_traffic_hourly_trend
    WHERE {where_clause}
    GROUP BY datetime_hour
    ORDER BY datetime_hour
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_summary_stats(grain: str, scope: str | None = None, dates=None):
    where_clause = build_where_clause(grain, scope, dates)
    
    q = f"""
    SELECT
        avg(pm25_daily_avg) as avg_pm25,
        avg(congestion_daily_avg) as avg_congestion,
        avg(pm25_congestion_uplift) as avg_pm25_uplift,
        avg(traffic_pollution_impact_score) as avg_comovement_score,
        avg(avg_traffic_coverage_ratio) as avg_traffic_coverage_ratio,
        sum(total_hours) as observed_hours,
        sum(low_congestion_hours) as low_congestion_hours,
        sum(high_congestion_hours) as high_congestion_hours,
        countIf(pm25_congestion_uplift IS NOT NULL) as uplift_sample_days
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause}
    """
    return query_df(q)

@st.cache_data(ttl=300)
def get_traffic_ranking_data(grain: str, scope: str | None = None, dates=None, col="pm25"):
    where_clause = build_where_clause(grain, scope, dates)
    target_col = col if col in ["pm25", "pm10", "co"] else "pm25"

    if grain in ["Tỉnh", "Phường"] and scope:
        ward_where_clause = build_where_clause(grain, scope, dates, date_col="t.date").replace("province =", "a.province =")
        q = f"""
        WITH ward_traffic AS (
            SELECT
                ward_code,
                toStartOfHour(timestamp_utc) as datetime_hour,
                toDate(timestamp_utc) as date,
                avg(value) as avg_congestion
            FROM air_quality.stg_tomtom__flow
            GROUP BY ward_code, datetime_hour, date
        )
        SELECT
            any(a.ward_name) as label_col,
            any(case
                when a.province IN ('Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
                when a.province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
                else 'Rural'
            end) as location_type,
            avg(h.{target_col} * t.avg_congestion) as impact_score
        FROM air_quality.dim_administrative_units a
        INNER JOIN ward_traffic t
            ON a.ward_code = t.ward_code
        INNER JOIN air_quality.dm_traffic_hourly_trend h
            ON a.province = h.province
            AND t.datetime_hour = h.datetime_hour
        WHERE {ward_where_clause}
            AND a.province != ''
            AND a.ward_code != ''
            AND h.{target_col} IS NOT NULL
            AND t.avg_congestion IS NOT NULL
            AND t.avg_congestion > 0
        GROUP BY a.ward_code
        ORDER BY impact_score DESC
        LIMIT 15
        """
        return query_df(q)
    
    q = f"""
    SELECT
        province as label_col,
        any(location_type) as location_type,
        avg({target_col}_daily_avg * congestion_daily_avg) as impact_score
    FROM air_quality.dm_traffic_pollution_correlation_daily
    WHERE {where_clause} AND province != ''
    GROUP BY label_col
    ORDER BY impact_score DESC
    LIMIT 15
    """
    return query_df(q)


def render_traffic_hotspot_ranking(df_rank: pd.DataFrame, grain: str, scope: str | None, target_poll: str):
    st.markdown("---")
    st.subheader(t("traffic_hotspot_ranking", lang))

    if df_rank.empty:
        st.plotly_chart(render_empty_chart("Không có dữ liệu xếp hạng phường cho lựa chọn này."), use_container_width=True)
        return

    is_ward_ranking = grain in ["Tỉnh", "Phường"] and bool(scope)
    area_label = "Phường" if is_ward_ranking and lang == "vi" else ("Ward" if is_ward_ranking else t("chart_label_area", lang))
    ranking_title = (
        f"Xếp hạng phường trong {scope} ({target_poll.upper()})"
        if is_ward_ranking and lang == "vi"
        else f"Ward ranking in {scope} ({target_poll.upper()})"
        if is_ward_ranking
        else f"{t('chart_top_polluted', lang)} ({target_poll.upper()})"
    )

    df_rank_plot = df_rank.sort_values('impact_score', ascending=True)

    loc_map = {
        "Urban": t("location_urban", lang),
        "Industrial": t("location_industrial", lang),
        "Rural": t("location_rural", lang)
    }
    df_rank_plot["loc_label"] = df_rank_plot["location_type"].map(loc_map).fillna(df_rank_plot["location_type"])

    color_map = {
        t("location_urban", lang): "#00CC96",
        t("location_industrial", lang): "#EF553B",
        t("location_rural", lang): "#636EFA"
    }

    fig_rank = px.bar(
        df_rank_plot,
        x="impact_score",
        y="label_col",
        color="loc_label",
        orientation='h',
        labels={"impact_score": t("traffic_impact", lang), "label_col": area_label, "loc_label": t("chart_label_type", lang)},
        color_discrete_map=color_map,
        title=ranking_title
    )
    fig_rank.update_layout(get_plotly_layout(height=500))
    st.plotly_chart(fig_rank, use_container_width=True)


df_hourly = get_traffic_correlation_hourly(date_range, spatial_grain, scope_val, col=target_poll)
df_summary = get_traffic_summary_stats(spatial_grain, scope_val, date_range)
df_rank = get_traffic_ranking_data(spatial_grain, scope_val, date_range, col=target_poll)

if not df_summary.empty and not pd.isna(df_summary.iloc[0].avg_pm25):
    stats = df_summary.iloc[0]
    avg_traffic = stats.avg_congestion
    pm25_uplift = stats.avg_pm25_uplift
    comovement_score = stats.avg_comovement_score
    avg_coverage = stats["avg_traffic_coverage_ratio"] if "avg_traffic_coverage_ratio" in stats.index else None
    observed_hours = stats["observed_hours"] if "observed_hours" in stats.index else 0
    low_congestion_hours = stats["low_congestion_hours"] if "low_congestion_hours" in stats.index else 0
    high_congestion_hours = stats["high_congestion_hours"] if "high_congestion_hours" in stats.index else 0
    uplift_sample_days = stats["uplift_sample_days"] if "uplift_sample_days" in stats.index else 0
    has_uplift = (
        not pd.isna(pm25_uplift)
        and observed_hours >= 24
        and low_congestion_hours >= 3
        and high_congestion_hours >= 3
        and uplift_sample_days > 0
    )

    if spatial_grain in ["Tỉnh", "Phường"]:
        st.info("KPI và xu hướng theo giờ vẫn tổng hợp ở mức tỉnh; biểu đồ xếp hạng bên dưới drill-down theo phường trong tỉnh đã chọn.")

    st.warning("Đây là tương quan quan sát từ phần dữ liệu AQI-traffic trùng thời gian, không phải ước lượng nhân quả.")

    if pd.isna(avg_coverage) or avg_coverage < 0.1 or not has_uplift:
        st.warning("Dữ liệu traffic chưa đủ để tính chênh lệch PM2.5 giữa giờ tắc nghẽn cao và thấp. Card uplift hiển thị N/A thay vì nội suy số liệu.")

    # ── Row 1: KPI Cards ──────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    
    traffic_display = f"{avg_traffic:.1%}" if avg_traffic > 0 else "N/A"

    with c1:
        render_metric_card(t("traffic_congestion", lang), traffic_display, icon="traffic")
    with c2:
        uplift_display = f"{pm25_uplift:.1f} µg/m³" if has_uplift else "N/A"
        render_metric_card(t("traffic_contribution", lang), uplift_display, icon="pie_chart")
    with c3:
        render_metric_card(t("traffic_impact", lang), f"{comovement_score:.2f}", icon="analytics")

    st.markdown("---")
    
    # ── Row 2: Hourly Correlation Charts ──────────────────────────────────────
    st.subheader(t("traffic_hourly_correlation", lang))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_congestion, name="Chỉ số tắc nghẽn giao thông",
                  line=dict(color='#1f77b4', width=3)),
        secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=df_hourly.datetime_hour, y=df_hourly.avg_p, name=target_poll.upper(),
                  fill='tozeroy', line=dict(color='#ff7f0e', width=2)),
        secondary_y=False,
    )
    fig.update_layout(get_plotly_layout(height=450), margin=dict(l=60, r=60, t=20, b=80), hovermode="x unified")
    fig.update_yaxes(title_text=target_poll.upper(), secondary_y=False)
    fig.update_yaxes(title_text="Chỉ số tắc nghẽn giao thông", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    render_traffic_hotspot_ranking(df_rank, spatial_grain, scope_val, target_poll)

else:
    st.plotly_chart(render_empty_chart("Không có dữ liệu KPI/xu hướng cho lựa chọn này."), use_container_width=True)
    render_traffic_hotspot_ranking(df_rank, spatial_grain, scope_val, target_poll)
