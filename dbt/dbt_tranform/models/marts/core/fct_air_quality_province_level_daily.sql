{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, date)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with hourly as (
    select * from {{ ref('fct_air_quality_province_level_hourly') }}
    {% if is_incremental() %}
    where date >= (select max(date) - interval 1 day from {{ this }})
    {% endif %}
),

daily as (
    select
        date,
        province,
        region_3,
        region_8,
        
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        
        -- Daily metrics for the province
        max(pm25_value) as pm25_max,
        avg(pm25_value) as pm25_avg,
        avg(pm10_value) as pm10_avg,
        
        avg(co_value)   as co_avg,
        avg(no2_value)  as no2_avg,
        avg(so2_value)  as so2_avg,
        avg(o3_value)   as o3_avg,

        max(last_ingested_at) as last_ingested_at
        
    from hourly
    group by
        date,
        province,
        region_3,
        region_8
)

select * from daily
