{{ config(materialized='view') }}

with forecasts as (
    select
        station_id,
        pollutant,
        forecast_date,
        measurement_datetime             AS forecast_measurement_time,
        forecast_avg,
        forecast_max,
        forecast_min,
        forecast_lead_days
    from {{ ref('stg_aqicn__forecast') }}
    where forecast_date is not null
),

-- Pre-materialized daily actuals (see int_daily_actuals__aqicn)
-- This avoids ClickHouse correlated subquery limitation in JOINs
actuals as (
    select
        station_id,
        pollutant,
        measurement_date,
        avg_value,
        max_value
    from {{ ref('int_daily_actuals__aqicn') }}
),

matched as (
    select
        f.station_id,
        f.pollutant,
        f.forecast_date,
        f.forecast_measurement_time,
        f.forecast_avg,
        f.forecast_max,
        f.forecast_min,
        f.forecast_lead_days,
        a.measurement_date AS actual_measurement_time,
        a.avg_value  AS actual_value,
        abs(f.forecast_avg - a.avg_value)                                          AS absolute_error,
        if(a.avg_value > 0, abs(f.forecast_avg - a.avg_value) / a.avg_value * 100, null) AS percentage_error
    from forecasts f
    inner join actuals a
        on  f.station_id     = a.station_id
        and f.pollutant      = a.pollutant
        and f.forecast_date  = a.measurement_date
),

with_scores as (
    select
        *,
        avg(absolute_error) over (partition by station_id, pollutant)                        AS mae,
        sqrt(avg(absolute_error * absolute_error) over (partition by station_id, pollutant)) AS rmse,
        case
            when percentage_error <= 10 then 100
            when percentage_error <= 20 then 80
            when percentage_error <= 30 then 60
            when percentage_error <= 50 then 40
            else 20
        end                                                                                AS forecast_accuracy_score
    from matched
)

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
    forecast_accuracy_score
from with_scores
