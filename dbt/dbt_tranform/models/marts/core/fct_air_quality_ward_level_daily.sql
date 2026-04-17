{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, date)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with ward_hourly as (
    select * from {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    where date >= (select max(date) - interval 2 day from {{ this }})
    {% endif %}
),

daily_agg as (
    select
        date,
        province,
        ward_code,
        region_3,
        region_8,
        
        avg(hourly_avg_aqi_us) as daily_avg_aqi_us,
        max(hourly_avg_aqi_us) as daily_max_aqi_us,
        min(hourly_avg_aqi_us) as daily_min_aqi_us,
        
        avg(hourly_avg_aqi_vn) as daily_avg_aqi_vn,
        max(hourly_avg_aqi_vn) as daily_max_aqi_vn,
        min(hourly_avg_aqi_vn) as daily_min_aqi_vn,
        
        avg(pm25_hourly_avg) as pm25_daily_avg,
        avg(pm10_hourly_avg) as pm10_daily_avg,
        avg(co_hourly_avg)   as co_daily_avg,
        avg(no2_hourly_avg)  as no2_daily_avg,
        avg(so2_hourly_avg)  as so2_daily_avg,
        avg(o3_hourly_avg)   as o3_daily_avg,
        
        max(last_ingested_at) as last_ingested_at
        
    from ward_hourly
    group by 1, 2, 3, 4, 5
)

select * from daily_agg
