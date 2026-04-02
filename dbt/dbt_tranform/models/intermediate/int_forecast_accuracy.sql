{{ config(materialized='view') }}

with forecasts as (
    select
        f.station_id,
        f.measurement_datetime,
        f.pollutant,
        f.forecast_date,
        f.forecast_avg,
        f.forecast_max,
        f.forecast_min,
        f.forecast_lead_days
    from {{ ref('stg_aqicn__forecast') }} f
),

actuals as (
    select
        station_id,
        pollutant,
        timestamp_utc,
        toDate(timestamp_utc) as measurement_date,
        value as actual_value
    from {{ ref('stg_aqicn__measurements') }}
    where value is not null
),

matched as (
    select
        f.station_id,
        f.pollutant,
        f.forecast_date,
        f.measurement_datetime as forecast_measurement_time,
        f.forecast_avg,
        f.forecast_max,
        f.forecast_min,
        f.forecast_lead_days,
        a.actual_value,
        a.measurement_datetime as actual_measurement_time,
        abs(f.forecast_avg - a.actual_value) as absolute_error,
        case
            when a.actual_value > 0
            then abs(f.forecast_avg - a.actual_value) / a.actual_value * 100
            else null
        end as percentage_error
    from forecasts f
    inner join actuals a
        on f.station_id = a.station_id
        and f.pollutant = a.pollutant
        and f.forecast_date = a.measurement_date
),

accuracy_metrics as (
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
        avg(absolute_error) over (
            partition by station_id, pollutant
        ) as mae,
        sqrt(avg(absolute_error * absolute_error) over (
            partition by station_id, pollutant
        )) as rmse,
        case
            when percentage_error <= 10 then 100
            when percentage_error <= 20 then 80
            when percentage_error <= 30 then 60
            when percentage_error <= 50 then 40
            else 20
        end as forecast_accuracy_score
    from matched
)

select * from accuracy_metrics
