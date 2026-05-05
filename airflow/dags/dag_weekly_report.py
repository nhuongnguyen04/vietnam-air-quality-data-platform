"""
Weekly Air Quality Report DAG.

Schedule: 0 2 * * 1 (Monday 02:00 UTC = 09:00 UTC+7)
Owner: air-quality-team

Queries current warehouse marts and sends a formatted Telegram message
with: province AQI averages, top 5 worst wards, 7-day trend, pollutant analysis.
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task
import os
import sys

# Add python_jobs to path for imports in container
sys.path.insert(0, "/opt/python/jobs")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "air_quality")

default_args = {
    "owner": "air-quality-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_client():
    """Get a clickhouse_connect HttpClient."""
    import clickhouse_connect
    clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")
    if clickhouse_password in (None, ""):
        raise RuntimeError("CLICKHOUSE_PASSWORD environment variable is required")
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=clickhouse_password,
        database=CLICKHOUSE_DB,
    )


def _emoji(aqi: float) -> str:
    """Return emoji for AQI value."""
    if aqi is None:
        return "⚪"
    if aqi <= 50:
        return "🟢"
    if aqi <= 100:
        return "🟡"
    if aqi <= 150:
        return "🟠"
    return "🔴"


def _week_start() -> str:
    """Return ISO date string for the Monday of the current week."""
    import datetime
    today = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


def _week_end() -> str:
    """Return ISO date string for yesterday."""
    import datetime
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


@dag(
    default_args=default_args,
    description="Weekly air quality report — city AQI averages, top 5 worst, trend, pollutant analysis",
    schedule="0 2 * * 1",
    start_date=datetime(2026, 4, 6),
    catchup=False,
    max_active_runs=1,
    tags=["report", "telegram", "weekly"],
)
def dag_weekly_report():
    """Build and send weekly Telegram report."""

    # ── Section 1: City AQI Averages ─────────────────────────────────
    @task
    def query_city_aqi():
        """Query 7-day average AQI per province from the current daily overview mart."""
        from dashboard.lib.aqi_utils import get_aqi_category

        client = _get_client()
        rows = client.query("""
            SELECT province,
                   round(avg(avg_aqi_vn), 1) AS weekly_avg
            FROM dm_air_quality_overview_daily
            WHERE date >= today() - 7
            GROUP BY province
            ORDER BY weekly_avg DESC
            LIMIT 20
        """).result_rows

        return [
            {
                "province": r[0],
                "weekly_avg": r[1],
                "category": get_aqi_category(r[1]),
                "emoji": _emoji(r[1]),
            }
            for r in rows
        ]

    # ── Section 2: Top 5 Worst Stations ────────────────────────────
    @task
    def query_top5_worst():
        """Return top 5 worst wards by peak AQI this week."""
        client = _get_client()
        rows = client.query("""
            SELECT province, ward_name,
                   round(max(max_aqi_vn), 0) AS peak_aqi,
                   argMax(main_pollutant, max_aqi_vn) AS pollutant
            FROM dm_air_quality_overview_daily
            WHERE date >= today() - 7
              AND ward_name != ''
            GROUP BY province, ward_name
            ORDER BY peak_aqi DESC
            LIMIT 5
        """).result_rows
        return [
            {"province": r[0], "ward_name": r[1], "peak_aqi": r[2], "pollutant": r[3]}
            for r in rows
        ]

    # ── Section 3: 7-Day Trend vs Last Week ─────────────────────────
    @task
    def query_trend():
        """Return week-over-week AQI comparison for each province."""
        client = _get_client()
        rows = client.query("""
            SELECT province,
                   round(avg(if(date >= today()-7, avg_aqi_vn, NULL)), 1) AS this_week,
                   round(avg(if(date BETWEEN today()-14 AND today()-8, avg_aqi_vn, NULL)), 1) AS last_week
            FROM dm_air_quality_overview_daily
            WHERE date >= today() - 14
            GROUP BY province
            HAVING this_week IS NOT NULL AND last_week IS NOT NULL
            ORDER BY (this_week - last_week) DESC
        """).result_rows
        return [
            {
                "province": r[0],
                "this_week": r[1],
                "last_week": r[2],
                "delta": r[1] - r[2],
                "trend": "📈" if r[1] > r[2] else "📉",
            }
            for r in rows
        ]

    # ── Section 4: Dominant Pollutant Analysis ──────────────────────
    @task
    def query_pollutant_analysis():
        """Return dominant pollutant per province (most frequent pollutant this week)."""
        client = _get_client()
        rows = client.query("""
            SELECT province, main_pollutant,
                   count(*) AS hours
            FROM dm_air_quality_overview_daily
            WHERE date >= today() - 7
              AND main_pollutant IS NOT NULL
            GROUP BY province, main_pollutant
            ORDER BY province, hours DESC
        """).result_rows
        # Keep top pollutant per province (first row per province after ordering by hours DESC)
        seen, result = set(), []
        for r in rows:
            province = r[0]
            if province not in seen:
                seen.add(province)
                result.append({"province": province, "pollutant": r[1], "hours": r[2]})
        return result

    # ── Section 5: Build + Send Report ──────────────────────────────
    @task
    def build_and_send(city_data, top5, trend, pollutants):
        """Build formatted Telegram HTML message and send."""
        from jobs.alerting.telegram_client import send_message

        lines = [
            "📊 <b>Air Quality Weekly Report</b>",
            f"Week of {_week_start()} – {_week_end()}",
            "",
            "🌡️ <b>AQI by Province (7-day average)</b>",
            "```",
            f"{'Province':<22} {'Avg AQI':>8} {'Status':<24}",
            "─" * 58,
        ]
        for c in city_data:
            lines.append(
                f"{c['province']:<22} {c['weekly_avg']:>8.1f} "
                f"{c['emoji']} {c['category']:<22}"
            )
        lines.extend(["```", ""])

        if top5:
            lines.append("🔴 <b>Top 5 Worst Wards</b>")
            for i, s in enumerate(top5, 1):
                lines.append(
                    f"{i}. {s['ward_name']} ({s['province']}) "
                    f"AQI {s['peak_aqi']:.0f} [{s['pollutant']}]"
                )
            lines.append("")

        if trend:
            lines.append("📈 <b>7-Day Province Trend (this week vs last week)</b>")
            for t in trend[:5]:
                delta_str = f"+{t['delta']:.1f}" if t['delta'] >= 0 else f"{t['delta']:.1f}"
                lines.append(
                    f"{t['trend']} {t['province']}: {t['last_week']:.1f} → "
                    f"{t['this_week']:.1f} ({delta_str})"
                )
            lines.append("")

        if pollutants:
            lines.append("🧪 <b>Dominant Pollutant per Province</b>")
            for p in pollutants[:10]:
                lines.append(f"• {p['province']}: <b>{p['pollutant'].upper()}</b> ({p['hours']}h)")
            lines.append("")

        lines.extend([
            "⚙️ Vietnam Air Quality Platform · docker compose up -d",
            "🔔 Manage alerts: Grafana → Alerting",
        ])

        text = "\n".join(lines)
        # Truncate to 3900 chars before Telegram 4096-char limit
        if len(text) > 3900:
            text = text[:3897] + "..."
        return send_message(text)

    # Wire up the DAG
    city_aqi_data = query_city_aqi()
    top5_data = query_top5_worst()
    trend_data = query_trend()
    pollutant_data = query_pollutant_analysis()

    build_and_send(city_aqi_data, top5_data, trend_data, pollutant_data)


dag_weekly_report = dag_weekly_report()
