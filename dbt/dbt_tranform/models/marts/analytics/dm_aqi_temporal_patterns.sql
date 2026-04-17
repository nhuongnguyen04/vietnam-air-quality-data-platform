{{ config(
    materialized='incremental',
    engine='SummingMergeTree',
    unique_key='(province, hour_of_day, day_of_week)',
    order_by='(province, hour_of_day, day_of_week)'
) }}

-- In ClickHouse, SummingMergeTree will aggregate rows based on the unique_key.
-- For non-numeric columns, it will pick the first value encountered.
-- For numeric columns (avg_aqi_us, avg_aqi_vn, etc.), we should technically be using 
-- an AggregatingMergeTree if we wanted precise averages of averages, 
-- but since this is for "Temporal Patterns", we'll stick to a simple averaging logic.

with hourly_data as (
    select
        province,
        toHour(datetime_hour) as hour_of_day,
        toDayOfWeek(datetime_hour) as day_of_week,
        prov_avg_aqi_us,
        prov_avg_aqi_vn,
        pm25_prov_avg,
        pm10_prov_avg,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_hourly') }}
    {% if is_incremental() %}
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

aggregates as (
    select
        province,
        hour_of_day,
        day_of_week,
        avg(prov_avg_aqi_us) as avg_aqi_us,
        avg(prov_avg_aqi_vn) as avg_aqi_vn,
        avg(pm25_prov_avg) as avg_pm25,
        avg(pm10_prov_avg) as avg_pm10,
        count(*) as reading_count,
        max(last_ingested_at) as ingest_time
    from hourly_data
    group by province, hour_of_day, day_of_week
)

select * from aggregates
