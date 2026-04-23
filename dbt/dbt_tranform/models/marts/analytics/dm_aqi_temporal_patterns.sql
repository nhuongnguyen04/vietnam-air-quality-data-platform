{{ config(
    materialized='incremental',
    engine='SummingMergeTree',
    unique_key='(province, hour_of_day, day_of_week)',
    order_by='(province, hour_of_day, day_of_week)'
) }}

with hourly_data as (
    select
        province,
        region_3,
        region_8,
        toHour(datetime_hour) as hour_of_day,
        toDayOfWeek(datetime_hour) as day_of_week,
        avg_aqi_us,
        avg_aqi_vn,
        pm25_avg,
        pm10_avg,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_hourly') }}
    {% if is_incremental() %}
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

aggregates as (
    select
        province,
        any(region_3) as region_3,
        any(region_8) as region_8,
        hour_of_day,
        day_of_week,
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        avg(pm25_avg) as avg_pm25,
        avg(pm10_avg) as avg_pm10,
        count(*) as reading_count,
        max(last_ingested_at) as ingest_time
    from hourly_data
    group by province, hour_of_day, day_of_week
)

select * from aggregates
