"""System Status page for data freshness and source coverage."""
from __future__ import annotations

import streamlit as st
import pandas as pd

from lib.clickhouse_client import query_df
from lib.style import render_metric_card
from lib.aqi_utils import render_empty_chart
from lib.i18n import t

# ── Translation Helper ────────────────────────────────────────────────────────
lang = st.session_state.get("lang", "vi")

st.title(t("status_title", lang))


def format_hours(value) -> str:
    if pd.isna(value):
        return "-"
    value = float(value)
    if value < 1:
        return f"{round(value * 60):.0f}m"
    if value < 24:
        return f"{value:.1f}h"
    return f"{value / 24:.1f}d"


def format_pct(value) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):.1f}%"


def platform_status(latest_lag: float, reliable_pct: float) -> str:
    if latest_lag <= 1 and reliable_pct >= 90:
        return t("status_operational", lang)
    if latest_lag <= 3 and reliable_pct >= 70:
        return t("status_degraded", lang)
    return t("status_delayed", lang)


@st.cache_data(ttl=60)
def get_platform_status_data():
    summary_q = """
    SELECT *
    FROM air_quality.dm_platform_health_summary
    """
    source_q = """
    SELECT *
    FROM air_quality.dm_platform_source_health
    ORDER BY source
    """
    return query_df(summary_q), query_df(source_q)


summary, source_summary = get_platform_status_data()

if not summary.empty and int(summary.loc[0, "source_ward_count"]) > 0:
    summary_row = summary.iloc[0]
    latest_lag = float(summary_row["latest_lag_hours"])
    latest_ingest_lag = float(summary_row["latest_ingest_lag_hours"])
    reliable_pct = float(summary_row["reliable_pct"])
    source_ward_count = int(summary_row["source_ward_count"])
    attention_count = int(summary_row["attention_count"])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card(t("latest_data_lag", lang), format_hours(latest_lag), icon="schedule")
    with col2:
        render_metric_card(t("latest_ingest_lag", lang), format_hours(latest_ingest_lag), icon="upload")
    with col3:
        render_metric_card(t("reliable_coverage", lang), f"{format_pct(reliable_pct)}", icon="insights")
    with col4:
        render_metric_card(t("system_status", lang), platform_status(latest_lag, reliable_pct), icon="health")

    stale_count = int(summary_row.get("stale_count", 0))
    offline_count = int(summary_row.get("offline_count", 0))
    if attention_count > 0:
        st.warning(
            t("data_trust_warning", lang).format(
                attention_count=attention_count,
                source_ward_count=source_ward_count,
                stale_count=stale_count,
                offline_count=offline_count,
            )
        )
    else:
        st.success(t("data_trust_ok", lang))

    st.caption(t("ops_dashboard_note", lang))
    st.markdown("---")

    st.subheader(t("source_reliability_monitoring", lang))
    if not source_summary.empty:
        source_summary = source_summary.copy()
        source_summary[t("chart_label_source", lang)] = source_summary["source"].str.upper()
        source_summary[t("latest_data_lag", lang)] = source_summary["latest_lag_hours"].map(format_hours)
        source_summary[t("latest_ingest_lag", lang)] = source_summary["latest_ingest_lag_hours"].map(format_hours)
        source_summary[t("reliable_coverage", lang)] = source_summary["reliable_pct"].map(format_pct)
        source_summary[t("source_ward_count", lang)] = source_summary["source_ward_count"].astype(int)
        source_summary[t("attention_needed", lang)] = (
            source_summary["stale_count"].astype(int).astype(str)
            + " / "
            + source_summary["offline_count"].astype(int).astype(str)
        )
        st.dataframe(
            source_summary[
                [
                    t("chart_label_source", lang),
                    t("latest_data_lag", lang),
                    t("latest_ingest_lag", lang),
                    t("reliable_coverage", lang),
                    t("source_ward_count", lang),
                    t("attention_needed", lang),
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
else:
    st.plotly_chart(
        render_empty_chart(
            t("no_data", lang) if lang == "en" else "Không có dữ liệu sức khỏe hệ thống."
        ),
        use_container_width=True,
    )
