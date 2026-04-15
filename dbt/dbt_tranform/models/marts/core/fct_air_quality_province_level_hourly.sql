{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with district_hourly as (
    select * from {{ ref('fct_air_quality_district_level_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

province_hourly as (
    select
        datetime_hour,
        date,
        province,
        region_3,
        region_8,
        
        -- Average of districts in the province
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        
        -- Concentrations
        avg(pm25_value) as pm25_value,
        avg(pm10_value) as pm10_value,
        avg(co_value)   as co_value,
        avg(no2_value)  as no2_value,
        avg(so2_value)  as so2_value,
        avg(o3_value)   as o3_value,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi,
        avg(pm10_aqi) as pm10_aqi,
        avg(co_aqi)   as co_aqi,
        avg(no2_aqi)  as no2_aqi,
        avg(so2_aqi)  as so2_aqi,
        avg(o3_aqi)   as o3_aqi,
        
        max(last_ingested_at) as last_ingested_at
        
    from district_hourly
    group by
        datetime_hour,
        date,
        province,
        region_3,
        region_8
)

select * from province_hourly
