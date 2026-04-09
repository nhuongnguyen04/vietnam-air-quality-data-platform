{{ config(materialized='view') }}

-- D-AQI-02: Phase 6 — Forecast accuracy model
-- D-01 (Phase 01.1): Forecast ingestion removed — AQICN and OpenWeather forecast APIs no longer used.
-- Dashboard Forecast → Historical Trend (uses actual measurements from int_unified__measurements).
-- This model returns empty — kept as schema placeholder to avoid breaking downstream references.
select
    null                                                      AS station_id,
    null                                                      AS pollutant,
    null                                                      AS forecast_date,
    null                                                      AS forecast_measurement_time,
    null                                                      AS forecast_avg,
    null                                                      AS forecast_max,
    null                                                      AS forecast_min,
    null                                                      AS forecast_lead_days,
    null                                                      AS actual_measurement_time,
    null                                                      AS actual_value,
    null                                                      AS absolute_error,
    null                                                      AS percentage_error,
    null                                                      AS mae,
    null                                                      AS rmse,
    null                                                      AS forecast_accuracy_score
where false