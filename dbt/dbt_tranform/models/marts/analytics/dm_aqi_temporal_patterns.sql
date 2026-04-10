{{ config(
    materialized='incremental',
    engine='SummingMergeTree',
    unique_key='(province, hour_of_day, day_of_week)',
    order_by='(province, hour_of_day, day_of_week)'
) }}

with hourly_data as (
    select
        province,
        toHour(datetime_hour) as hour_of_day,
        toDayOfWeek(datetime_hour) as day_of_week,
        final_aqi_us,
        final_aqi_vn,
        pm25_value,
        pm10_value,
        ingest_time
    from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

aggregates as (
    select
        province,
        hour_of_day,
        day_of_week,
        avg(final_aqi_us) as avg_aqi_us,
        avg(final_aqi_vn) as avg_aqi_vn,
        avg(pm25_value) as avg_pm25,
        avg(pm10_value) as avg_pm10,
        count(*) as reading_count,
        max(ingest_time) as ingest_time
    from hourly_data
    group by province, hour_of_day, day_of_week
)

select * from aggregates
