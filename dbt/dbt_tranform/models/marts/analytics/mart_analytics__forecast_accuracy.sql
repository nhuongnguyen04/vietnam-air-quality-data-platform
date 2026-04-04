{{ config(
    materialized='incremental',
    unique_key='station_id || pollutant || toString(forecast_date)',
    on_schema_change='sync_all_columns',
    clickhouse_settings={
        'max_bytes_before_external_group_by': '1073741824'
    }
) }}

{% if is_incremental() %}
select
    station_id,
    pollutant,
    forecast_date,
    forecast_measurement_time,
    forecast_avg,
    forecast_max,
    forecast_min,
    forecast_lead_days,
    actual_measurement_time,
    actual_value,
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
where forecast_date >= now() - interval 90 day
{% else %}
select
    station_id,
    pollutant,
    forecast_date,
    forecast_measurement_time,
    forecast_avg,
    forecast_max,
    forecast_min,
    forecast_lead_days,
    actual_measurement_time,
    actual_value,
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
{% endif %}
