from __future__ import annotations

import pytest

from python_jobs.text_to_sql.eval_runner import (
    EvalValidationError,
    find_matching_eval_case,
    load_eval_cases,
    evaluate_sql_against_case,
)


GOOD_SQL_BY_CASE_ID = {
    "vi-current-status": """
        SELECT province, MAX(avg_aqi_vn) AS max_aqi
        FROM dm_air_quality_overview_hourly
        WHERE datetime_hour >= now() - INTERVAL 24 HOUR
        GROUP BY province
        ORDER BY max_aqi DESC
        LIMIT 10
    """,
    "vi-compliance": """
        SELECT province, MAX(pm25_who_days) AS breach_days
        FROM dm_aqi_compliance_standards
        WHERE air_quality_standard = 'WHO'
          AND date >= today() - 7
        GROUP BY province
        ORDER BY breach_days DESC
        LIMIT 20
    """,
    "vi-traffic": """
        SELECT province, AVG(congestion_daily_avg) AS avg_congestion, AVG(pm25_daily_avg) AS avg_pm25
        FROM dm_traffic_pollution_correlation_daily
        WHERE date >= today() - 30
        GROUP BY province
        ORDER BY avg_congestion DESC
        LIMIT 20
    """,
    "en-current-status": """
        SELECT province, MAX(avg_aqi_vn) AS max_aqi
        FROM dm_air_quality_overview_hourly
        WHERE datetime_hour >= now() - INTERVAL 24 HOUR
        GROUP BY province
        ORDER BY max_aqi DESC
        LIMIT 10
    """,
    "en-health": """
        SELECT r.province, MAX(r.health_risk_score) AS risk_score, AVG(h.pm25_population_exposure) AS exposure
        FROM dm_regional_health_risk_ranking AS r
        JOIN dm_aqi_health_impact_summary AS h
          ON r.province = h.province
        WHERE h.date >= toStartOfMonth(addMonths(today(), -1))
        GROUP BY r.province
        ORDER BY risk_score DESC
        LIMIT 10
    """,
    "en-weather": """
        SELECT w.province, AVG(w.avg_humidity) AS humidity_avg, AVG(w.avg_wind_speed) AS wind_speed_avg, AVG(c.avg_pm25) AS pm25_avg
        FROM dm_weather_hourly_trend AS w
        JOIN dm_weather_pollution_correlation_daily AS c
          ON w.province = c.province
        WHERE c.date >= today() - 30
        GROUP BY w.province
        ORDER BY pm25_avg DESC
        LIMIT 20
    """,
}


@pytest.mark.unit
def test_find_matching_eval_case_by_question_and_language():
    case = find_matching_eval_case(
        question="Which provinces had the worst AQI in the last 24 hours?",
        lang="en",
    )

    assert case is not None
    assert case.id == "en-current-status"


@pytest.mark.unit
def test_eval_runner_accepts_repo_eval_corpus(semantic_dir):
    for case in load_eval_cases():
        result = evaluate_sql_against_case(
            GOOD_SQL_BY_CASE_ID[case.id],
            case,
            semantic_dir=str(semantic_dir),
        )
        assert result.case_id == case.id
        assert result.referenced_tables


@pytest.mark.unit
def test_eval_runner_rejects_missing_expected_table(semantic_dir):
    case = find_matching_eval_case(
        question="How do humidity and wind speed relate to PM2.5 over the last 30 days?",
        lang="en",
    )
    assert case is not None

    with pytest.raises(EvalValidationError, match="did not use any expected tables"):
        evaluate_sql_against_case(
            """
            SELECT province, current_aqi_vn
            FROM dm_aqi_current_status
            WHERE province IS NOT NULL
            GROUP BY province
            LIMIT 10
            """,
            case,
            semantic_dir=str(semantic_dir),
        )
