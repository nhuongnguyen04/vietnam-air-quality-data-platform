{{ config(materialized='table') }}

select
    station_id,
    pollutant,
    forecast_date,
    forecast_measurement_time,
    forecast_avg,
    forecast_max,
    forecast_min,
    forecast_lead_days,
    actual_value,
    actual_measurement_time,
    absolute_error,
    percentage_error,
    mae,
    rmse,
    forecast_accuracy_score,
    case
        when forecast_accuracy_score >= 80 then 'High'
        when forecast_accuracy_score >= 60 then 'Medium'
        when forecast_accuracy_score >= 40 then 'Low'
        else 'Very Low'
    end as accuracy_category
from {{ ref('int_forecast_accuracy') }}

