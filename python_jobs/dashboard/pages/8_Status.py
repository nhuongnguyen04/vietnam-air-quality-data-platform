"""
System Status page.
Displays data ingestion pipeline logs, source reliable coverage ratios, and system health status.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib.aqi_utils import render_empty_chart
from lib.clickhouse_client import query_df
from lib.data_service import localize_confidence_level, localize_source_mix
from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.style import render_metric_card
from lib.chart_config import get_plotly_layout, create_empty_state

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

def platform_status(latest_lag: float, reliable_pct: float, lang: str = "vi") -> str:
    if latest_lag <= 1 and reliable_pct >= 90:
        return t("status_operational", lang) if "status_operational" in TRANSLATIONS[lang] else "Operational"
    if latest_lag <= 3 and reliable_pct >= 70:
        return t("status_degraded", lang) if "status_degraded" in TRANSLATIONS[lang] else "Degraded"
    return t("status_delayed", lang) if "status_delayed" in TRANSLATIONS[lang] else "Delayed"

# Backup translate list in case of local testing
TRANSLATIONS = {
    "vi": {"status_operational": "Ổn định", "status_degraded": "Giảm chất lượng", "status_delayed": "Bị trễ"},
    "en": {"status_operational": "Operational", "status_degraded": "Degraded", "status_delayed": "Delayed"}
}

@st.cache_data(ttl=60)
def get_platform_status_data():
    summary_q = "SELECT * FROM air_quality.dm_platform_health_summary"
    source_q = "SELECT * FROM air_quality.dm_platform_source_health ORDER BY source"
    
    confidence_q = """
    WITH latest_date AS (
        SELECT max(date) AS date
        FROM air_quality.dm_air_quality_overview_daily
        WHERE source_mix = 'observed'
    )
    SELECT
        source_mix,
        confidence_level,
        count() AS ward_count,
        round(avg(confidence_score), 2) AS confidence_score,
        sum(aqiin_observation_count) AS aqiin_observations,
        sum(openweather_observation_count) AS openweather_observations
    FROM air_quality.dm_air_quality_overview_daily
    WHERE date = (SELECT date FROM latest_date)
      AND source_mix = 'observed'
    GROUP BY source_mix, confidence_level
    ORDER BY confidence_score DESC
    """
    return query_df(summary_q), query_df(source_q), query_df(confidence_q)

@page_wrapper("status", "⚙️ System Platform Health Status", icon="⚙️")
def main(lang: str):
    # Fetch Data
    summary, source_summary, confidence_summary = get_platform_status_data()

    if not summary.empty and int(summary.loc[0, "source_ward_count"]) > 0:
        summary_row = summary.iloc[0]
        latest_lag = float(summary_row["latest_lag_hours"])
        latest_ingest_lag = float(summary_row["latest_ingest_lag_hours"])
        reliable_pct = float(summary_row["reliable_pct"])
        source_ward_count = int(summary_row["source_ward_count"])
        attention_count = int(summary_row["attention_count"])
        stale_count = int(summary_row["stale_count"])
        offline_count = int(summary_row["offline_count"])

        # ── KPI Cards ──────────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        
        # Platform status determination
        status_val = "Operational" if latest_lag <= 1.5 and reliable_pct >= 90 else ("Degraded" if latest_lag <= 4 else "Delayed")
        status_label = TRANSLATIONS.get(lang, TRANSLATIONS["vi"]).get(f"status_{status_val.lower()}", status_val)
        
        with c1:
            render_metric_card(t("latest_data_lag", lang), format_hours(latest_lag), icon="schedule")
        with c2:
            render_metric_card(t("latest_ingest_lag", lang), format_hours(latest_ingest_lag), icon="upload")
        with c3:
            render_metric_card(t("reliable_coverage", lang), f"{format_pct(reliable_pct)}", icon="insights")
        with c4:
            render_metric_card(t("system_status", lang), status_label, icon="health")

        render_section_divider()

        # Alert banner using render_info_banner helper
        from lib.page_helpers import render_info_banner
        
        if attention_count > 0:
            msg = t("data_trust_warning", lang).format(
                attention_count=attention_count,
                source_ward_count=source_ward_count,
                stale_count=stale_count,
                offline_count=offline_count
            )
            render_info_banner(msg, type="warning")
        else:
            render_info_banner(t("data_trust_ok", lang), type="success")

        st.caption(f"💡 {t('ops_dashboard_note', lang)}")
        
        render_section_divider()

        # ── Confidence Table ──────────────────────────────────────────────────
        st.markdown(f"#### 🛡️ {'Độ tin cậy AQI' if lang == 'vi' else 'AQI Confidence'}")
        if not confidence_summary.empty:
            confidence_summary = confidence_summary.copy()
            confidence_summary["Nguồn"] = confidence_summary["source_mix"].apply(lambda x: localize_source_mix(x, lang))
            confidence_summary["Mức tin cậy"] = confidence_summary["confidence_level"].apply(lambda x: localize_confidence_level(x, lang))
            st.dataframe(
                confidence_summary[
                    [
                        "Nguồn", "Mức tin cậy", "ward_count", "confidence_score",
                        "aqiin_observations", "openweather_observations",
                    ]
                ].rename(
                    columns={
                        "ward_count": "Số ward" if lang == "vi" else "Wards",
                        "confidence_score": "Điểm tin cậy" if lang == "vi" else "Confidence",
                        "aqiin_observations": "Quan trắc AQI.in" if lang == "vi" else "AQI.in observations",
                        "openweather_observations": "Ước tính OpenWeather" if lang == "vi" else "OpenWeather estimates",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            
        render_section_divider()

        # ── Source Reliability Chart & Monitoring ─────────────────────────────
        st.markdown(f"#### 📡 {t('source_reliability_monitoring', lang)}")
        if not source_summary.empty:
            source_summary_plot = source_summary.copy()
            source_summary_plot["source_name"] = source_summary_plot["source"].str.upper()
            fig_rel = px.bar(
                source_summary_plot,
                x="reliable_pct",
                y="source_name",
                color="source_name",
                orientation="h",
                labels={"reliable_pct": "Reliability (%)", "source_name": t("chart_label_source", lang)},
                color_discrete_map={"AQIIN": "#0891B2", "OPENWEATHER": "#F59E0B"}
            )
            fig_rel.update_layout(
                get_plotly_layout(height=160, compact=True),
                showlegend=False,
                margin={"l": 20, "r": 20, "t": 10, "b": 30}
            )
            fig_rel.update_xaxes(ticksuffix="%", range=[0, 100])
            st.plotly_chart(fig_rel, use_container_width=True)

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
            create_empty_state(t("no_data", lang) if lang == "en" else "Không có dữ liệu sức khỏe hệ thống."),
            use_container_width=True,
        )

if __name__ == "__main__":
    main()
